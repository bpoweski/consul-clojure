(ns consul.core
  (:require [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [cheshire.core :as json]
            [clj-http.lite.client :as client]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (javax.xml.bind DatatypeConverter)))

;; A couple of initial helper functions.

(defn base64->str [^String s]
  (String. (DatatypeConverter/parseBase64Binary s) "UTF8"))

(defn str->base64 [^String s]
  (DatatypeConverter/printBase64Binary (.getBytes s "UTF8")))

(defn endpoint->path
  "Converts a vector into the consul endpoint path."
  [endpoint]
  {:pre [(vector? endpoint)]}
  (str "/v1/" (str/join "/" (map #(if (keyword? %) (name %) (str %)) endpoint))))

(defn parse-response [response]
  (let [content-type (get-in response [:headers "content-type"])]
    (if (= content-type "application/json")
      (update-in response [:body] json/parse-string keyword)
      response)))

(def local-conn {:scheme :http :server-name "127.0.0.1" :server-port 8500})

(defn consul-request
  "Constructs a Consul HTTP request."
  [conn method endpoint & [{:as opts :keys [body]}]]
  {:pre [(or (= conn :local) (map? conn))]
   :post [(:request-method %) (:server-name %) (:server-port %)]}
  (cond-> (assoc local-conn :request-method method)
    (map? conn)        (merge conn)
    (vector? endpoint) (assoc :uri (endpoint->path endpoint))
    (string? endpoint) (assoc :uri endpoint)
    (map? opts)        (merge opts)
    (map? body)        (update-in [:body] json/generate-string)
    (vector? body)     (update-in [:body] json/generate-string)))

(defn success? [{:keys [status] :as resp}]
  (or (client/unexceptional-status? status)
      (= 404 status)))

(defn ex-info? [x]
  (instance? clojure.lang.ExceptionInfo x))

(defn consul
  "Creates a request and calls consul using the HTTP API."
  [conn method endpoint & [{:as request}]]
  (let [http-request (consul-request conn method endpoint request)]
    (try
      (let [response (-> http-request
                         (assoc :throw-exceptions false)
                         client/request parse-response)]
        (if (success? response)
          response
          (if-let [errors (get-in response [:body :Errors])]
            (throw (ex-info "Transaction failure" {:reason :transaction-failure
                                                   :conn conn
                                                   :endpoint endpoint
                                                   :request request
                                                   :http-request http-request
                                                   :http-response response
                                                   :errors errors}))
            (throw (ex-info (:body response) {:reason :application-failure
                                              :conn conn
                                              :endpoint endpoint
                                              :request request
                                              :http-request http-request
                                              :http-response response})))))
      (catch java.net.ConnectException ex
        (throw (ex-info "connection failure" {:reason :connect-failure :conn conn :endpoint endpoint :request request :http-request http-request} ex)))
      (catch java.net.UnknownHostException ex
        (throw (ex-info "unknown host" {:reason :unknown-host :conn conn :endpoint endpoint :request request :http-request http-request} ex)))
      (catch Exception ex
        (if (ex-info? ex)
          (throw ex)
          (throw (ex-info (.getMessage ex) {:reason :exception :conn conn :endpoint endpoint :request request :http-request http-request} ex)))))))

(defn headers->index
  "Selects the X-Consul-* headers into a map with keywordized names."
  [m]
  (reduce-kv
   (fn [m k v]
     (cond (= "x-consul-knownleader" k)             (assoc m :known-leader (= "true" v))
           (and (= "x-consul-index" k) (string? v)) (assoc m :modify-index (Long/parseLong v))
           (re-matches #"x-consul-.*" k)            (assoc m (keyword k) v)
           :else                                    m))
   {} m))

(defn consul-200
  [conn method endpoint params]
  (-> (consul conn method endpoint params)
      :status
      (= 200)))

(defn consul-index
  [conn method endpoint params]
  (let [{:keys [body headers]} (consul conn method endpoint params)]
    (assoc (headers->index headers) :body (cske/transform-keys csk/->kebab-case-keyword body))))

(def consul-pascal-case-substitutions
  "Differences from PascalCase used by Consul."
  {:Http      :HTTP
   :Id        :ID
   :Ttl       :TTL
   :ServiceId :ServiceID
   :CheckId   :CheckID
   :Dns       :DNS})

(defn rename-keys [m substitutions]
  (reduce (fn [r [k v]]
            (assoc r (get substitutions k k) (if (map? v)
                                               (rename-keys v substitutions)
                                               v)))
          {} m))

(defn map->consulcase [m]
  (-> (cske/transform-keys csk/->PascalCase m)
      (rename-keys consul-pascal-case-substitutions)))
      ;(set/rename-keys consul-pascal-case-substitutions)))


(comment
  (queries :local)
  (def m {:name "query-bourne-2"
          :service {:service "bourne-api-development"
                    :failover {:nearest-n 4
                               :datacenters ["dc1" "dc2"]}
                    :near :_ip
                    :only-passing true
                    :tags ["development" "!experimental"]}
          :dns {:ttl "10s"}})
  (map->consulcase m))

(def kv (juxt :key :value))

;; Key/Value endpoint - https://www.consul.io/docs/agent/http/kv.html

(defn kv-map-convert
  [response convert?]
  {:pre [(map? response)]}
  (cske/transform-keys csk/->kebab-case-keyword
                       (assoc response
                              :value (if (and convert? (string? (:Value response)))
                                       (base64->str (:Value response))
                                       (:Value response)))))

(defn mapify-response
  "Converts a list of kv's into a map, stripping off a given prefix"
  [prefix kvs]
  (if (seq? kvs)
    (reduce
     (fn [a v]
       (let [ks
             (map
              keyword
              (->
               ^String (:key v)
               (.replaceFirst prefix "")
               (.split "/")
               seq))]
         (assoc-in a ks (:value v))))
     {} kvs)
    kvs))

(defn kv-keys
  "Retrieves a set of keys using prefix.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :separator - List keys up to separator."
  ([conn prefix]
   (kv-keys conn prefix {}))
  ([conn prefix {:keys [dc wait index separator] :as params}]
   (let [{:keys [body headers status] :as response}
         (consul conn :get [:kv prefix] {:query-params (assoc params :keys "")})]
     (cond
       (= 404 status) (headers->index headers)
       (seq body)     (assoc (headers->index headers) :keys (into #{} body))))))

(defn kv-get
  "Retrieves key k from consul given the following optional parameters.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :raw?      - If true and used with a non-recursive GET, the response is just the raw value of the key, without any encoding.
  :string?    - Converts the value returned for k into a string.  Defaults to true."
  ([conn k]
   (kv-get conn k {}))
  ([conn k {:keys [dc wait index raw? string?] :or {raw? false string? true} :as params}]
   (let [{:keys [body headers] :as response}
         (consul conn :get [:kv k]
                 {:query-params (cond-> (dissoc params :raw? :string?)
                                  raw? (assoc :raw ""))})]
     (cond
       (or (nil? body) raw?)
       (assoc (headers->index headers) :key k :body body)
       :else (kv-map-convert (first body) string?)))))



(defn kv-recurse
  "Retrieves key k from consul given the following optional parameters.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :string?    - Converts the value returned for k into a string.  Defaults to true."
  ([conn prefix]
   (kv-recurse conn prefix {:map? true}))
  ([conn prefix {:as params :keys [string? map?] :or {string? true map? true}}]
   (let [{:keys [body headers] :as response}
         (consul conn :get [:kv prefix] {:query-params (assoc params :recurse "")})
         body (if (and body (seq? body)) (map #(kv-map-convert %1 string?) body))]
     (assoc
      (headers->index headers)
      :body body
      :mapped (mapify-response prefix body)))))

(defn kv-put
  "Sets key k with value v.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.

  :flags     - This can be used to specify an unsigned value between 0 and 2^(64-1). Stored against the key for client app use.
  :cas       - Uses a CAS index when updating. If the index is 0, puts only if the key does not exist. If index is non-zero, index must match the ModifyIndex of that key.
  :acquire   - Session ID. Updates using lock acquisition. A key does not need to exist to be acquired
  :release   - Session ID. Yields a lock acquired with :acquire."
  ([conn k v]
   (kv-put conn k v {}))
  ([conn k v {:keys [dc flags cas acquire release] :as params}]
   (:body (consul conn :put [:kv k] {:query-params params :body v}))))

(defn kv-del
  "Deletes a key k from consul.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :recurse?  - If true, then consul it will delete all keys with the given prefix. Defaults to false.
  :cas       - Uses a CAS index when deleting. Index must be non-zero and must match the ModifyIndex of the key to delete successfully"
  ([conn k]
   (kv-del conn k {}))
  ([conn k {:as params :keys [recurse?] :or {recurse? false}}]
   (:body (consul conn :delete [:kv k] {:query-params (cond-> (dissoc params :recurse?)
                                                        recurse? (assoc :recurse ""))}))))

;; Agent endpoint - https://www.consul.io/docs/agent/http/agent.html

(defn shallow-nameify-keys
  "Returns a map with the root keys converted to strings using name."
  [m]
  (reduce-kv #(assoc %1 (name %2) (cske/transform-keys csk/->kebab-case-keyword %3)) {} m))

(defn agent-checks
  "Returns all the checks that are registered with the local agent as a map with the check names as strings. "
  ([conn]
   (agent-checks conn {}))
  ([conn {:as params}]
   (shallow-nameify-keys (:body (consul conn :get [:agent :checks] {:query-params params})))))

(defn agent-services
  "Returns all services registered with the local agent."
  ([conn]
   (agent-services conn {}))
  ([conn {:as params}]
   (shallow-nameify-keys (:body (consul conn :get [:agent :services] {:query-params params})))))

(defn agent-members
  "Returns the members as seen by the local serf agent."
  ([conn]
   (agent-members conn {}))
  ([conn {:as params}]
   (cske/transform-keys csk/->kebab-case-keyword (:body (consul conn :get [:agent :members] {:query-params params})))))

(defn agent-self
  "Returns the local node configuration."
  ([conn]
   (agent-self conn {}))
  ([conn {:as params}]
   (cske/transform-keys csk/->kebab-case-keyword (:body (consul conn :get [:agent :self] {:query-params params})))))



(defn agent-maintenance
  "Manages node maintenance mode.

  Requires a boolean value for enable?.  An optional :reason can be provided, returns true if successful."
  ([conn enable?]
   (agent-maintenance conn enable? {}))
  ([conn enable? {:as params}]
   {:pre [(contains? #{true false} enable?)]}
   (consul-200 conn :put [:agent :maintenance] {:query-params (assoc params :enable enable?)})))

(defn agent-join
  "Triggers the local agent to join a node.  Returns true if successful."
  ([conn address]
   (agent-join conn address {}))
  ([conn address {:as params}]
   (consul-200 conn :get [:agent :join address] {:query-params params})))

(defn agent-force-leave
  "Forces removal of a node, returns true if the request succeeded."
  ([conn node]
   (agent-force-leave conn node {}))
  ([conn node {:as params}]
   (consul-200 conn :get [:agent :force-leave node] {:query-params params})))

;; Agent Checks - https://www.consul.io/docs/agent/http/agent.html#agent_check_register


(defn check?
  "Simple validation outlined in https://www.consul.io/docs/agent/checks.html."
  [{:keys [Name HTTP TTL Script Interval] :as check}]
  (and (string? Name)
       (or (string? Script) (string? HTTP) (string? TTL))
       (if (or Script HTTP)
         (string? Interval)
         true)))


(defn map->check
  "Creates a check from a map.  Returns a map with excessive capitalization conforming to consul's expectations.

  Requires :script, :http or :ttl to be set.

  Map keys:

  :name       - required
  :id         - If not provided, is set to :name.  However, duplicate :id values are not allowed.
  :notes      - Free text.
  :script     - Path of the script Consul should invoke as part of the check.
  :http       - URL of the HTTP check.
  :interval   - Required if :script or :http is set.
  :service-id - Associates the check with an existing service."
  [check]
  {:post [(check? %)]}
  (map->consulcase check))

(defn query-check?
  "Simple valdation for prepared query maps"
  [{:keys [Name Service] :as query}]
  (and (string? Name)
       (map? Service)))

(defn map->prepared-query
  "Creates a prepared query from a map. Returns a map with excessive capitalization conforming to consul's expectations.

  Requires :name and :service to be set"
  [query]
  {:post [(query-check? %)]}
  (map->consulcase query))

(defn agent-register-check
  "Registers a new check with the local agent."
  ([conn {:keys [name id notes script http interval service-id] :as m}]
   (agent-register-check conn m {}))
  ([conn m {:as params}]
   (consul-200 conn :put [:agent :check :register] {:body (map->check m) :query-params params})))

(defn agent-deregister-check
  "Deregisters a check with the local agent."
  ([conn check-id]
   (agent-deregister-check conn check-id {}))
  ([conn check-id {:as params}]
   (consul-200 conn :put [:agent :check :deregister check-id] {:query-params params})))

(defn agent-pass-check
  "Marks a local check as passing."
  ([conn check-id]
   (agent-pass-check conn check-id {}))
  ([conn check-id {:keys [note] :as params}]
   (consul-200 conn :get [:agent :check :pass check-id] {:query-params params})))

(defn agent-warn-check
  "Marks a local test as warning."
  ([conn check-id]
   (agent-warn-check conn check-id {}))
  ([conn check-id {:keys [note] :as params}]
   (consul-200 conn :get [:agent :check :warn check-id] {:query-params params})))

(defn agent-fail-check
  "Marks a local test as critical."
  ([conn check-id]
   (agent-fail-check conn check-id {}))
  ([conn check-id {:keys [note] :as params}]
   (consul-200 conn :get [:agent :check :warn check-id] {:query-params params})))

;; Services - https://www.consul.io/docs/agent/http/agent.html#agent_service_register

(defn agent-register-service
  "Registers a new local service."
  ([conn {:keys [id name tags address port check]  :as m}]
   (agent-register-service conn m {}))
  ([conn m {:as params}]
   (consul-200 conn :put [:agent :service :register] {:query-params params :body m})))

(defn agent-deregister-service
  "Registers a new local service."
  ([conn service-id]
   (agent-deregister-service conn service-id {}))
  ([conn service-id {:as params}]
   (consul-200 conn :put [:agent :service :deregister service-id] {:query-params params})))


(defn agent-maintenance-service
  "Put a service into maintenance"
  ([conn service-id enable reason]
   (agent-maintenance-service conn service-id {:enable enable :reason reason}))
  ([conn service-id {:keys [enable reason] :as params}]
   (consul-200 conn :get [:agent :service :maintenance service-id] {:query-params params})))


;; Catalog endpoints - https://www.consul.io/docs/agent/http/catalog.html

;; These are low level endpoints, so it is preferrable to use the other functions instead

(defn catalog-register
  "Register a catalog entry. Low level - preferably"
  ([conn {:keys [datacenter node address service check] :as entry}]
   (catalog-register conn entry {}))
  ([conn entry {:as params}]
   (consul-200 conn :put [:catalog :register] {:query-params params :body (map->consulcase entry)})))

(defn catalog-deregister
  ([conn entry]
   (catalog-deregister conn entry {}))
  ([conn {:keys [datacenter node checkid serviceid] :as entry} {:as params}]
   (consul-200 conn :put [:catalog :deregister] {:query-params params :body (map->consulcase entry)})))

(defn catalog-datacenters
  ([conn]
   (catalog-datacenters conn {}))
  ([conn {:as params}]
   (consul-index conn :get [:catalog :datacenters] {:query-params params})))

(defn catalog-nodes
  ([conn]
   (catalog-nodes conn {}))
  ([conn {:keys [dc] :as params}]
   (:body (consul-index conn :get [:catalog :nodes] {:query-params params}))))

(defn catalog-services
  ([conn]
   (catalog-services conn {}))
  ([conn {:keys [dc] :as params}]
   (:body (consul-index conn :get [:catalog :services] {:query-params params}))))

(defn catalog-service
  ([conn service]
   (catalog-service conn service {}))
  ([conn service {:keys [tag dc] :as params}]
   (:body (consul-index conn :get [:catalog :service service] {:query-params params}))))

(defn catalog-node
  ([conn node]
   (catalog-node conn node {}))
  ([conn node {:keys [dc] :as params}]
   (:body (consul-index conn :get [:catalog :node node] {:query-params params}))))


;; Sessions - https://www.consul.io/docs/agent/http/status.html

(defn session-create
  "Create a new consul session"
  ([conn]
   (session-create conn {} {}))
  ([conn {:keys [lock-delay node name checks behavior ttl] :as session}]
   (session-create conn session {}))
  ([conn session {:keys [dc] :as params}]
   (-> (consul-index conn :put [:session :create] {:query-params params :body (map->consulcase session)})
       :body :id)))

(defn session-renew
  "Renews a session. NOTE the TTL on the response and use that as a basic on when to renew next time
  since consul may return a higher TTL, requesting that you renew less often (high server load)"
  ([conn session]
   (session-renew conn session {}))
  ([conn session {:keys [dc] :as params}]
   (-> (consul-index conn :put [:session :renew session] {:query-params params})
       :body first)))

(defn session-destroy
  ([conn session]
   (session-destroy conn session {}))
  ([conn session {:keys [dc] :as params}]
   (consul-200 conn :put [:session :destroy session] {:query-params params})))

(defn session-info
  ([conn session]
   (session-info conn session {}))
  ([conn session {:keys [dc] :as params}]
   (consul-index conn :put [:session :info session] {:query-params params})))

(defn session-node
  ([conn node]
   (session-node conn node {}))
  ([conn node {:keys [dc] :as params}]
   (consul-index conn :put [:session :node node] {:query-params params})))

(defn session-list
  ([conn]
   (session-list conn {}))
  ([conn {:keys [dc] :as params}]
   (consul-index conn :put [:session :list] {:query-params params})))

;; ACL's - https://www.consul.io/docs/agent/http/acl.html

;; Also, read up on ACL's here https://www.consul.io/docs/internals/acl.html


(defn acl-create
  "Create a new ACL token

  :type is 'client' or 'management'"
  [conn {:keys [name type rules] :as tkn} {:keys [token] :as params}]
  (-> (consul-index conn :put [:acl :create] {:query-params params :body (map->consulcase tkn)})
      :body :id))

(defn acl-update
  [conn {:keys [id name type rules] :as tkn} {:keys [token] :as params}]
  (consul-index conn :put [:acl :update] {:query-params params :body (map->consulcase tkn)}))

(defn acl-destroy
  [conn token-id {:keys [token] :as params}]
  (consul-200 conn :put [:acl :delete token-id] {:query-params params}))

(defn acl-info
  [conn token-id {:keys [token] :as params}]
  (:body (consul-index conn :get [:acl :info token-id] {:query-params params})))

(defn acl-clone
  [conn token-id {:keys [token] :as params}]
  (-> (consul-index conn :put [:acl :clone token-id] {:query-params params})
      :body :id))

(defn acl-list
  [conn {:keys [token] :as params}]
  (consul-index conn :put [:acl :list] {:query-params params}))

;; Events - https://www.consul.io/docs/agent/http/event.html
;; https://www.consul.io/docs/commands/event.html

(defn event-fire
  ([conn name]
   (event-fire conn name {}))
  ([conn name {:keys [node service tag] :as params}]
   (consul-index conn :put [:event :fire name] {:query-params params})))

(defn event-list
  ([conn]
   (event-list conn {}))
  ([conn {:keys [name] :as params}]
   (consul-index conn :get [:event :list] {:query-params params})))

;; Health Checks - https://www.consul.io/docs/agent/http/health.html

(defn node-health
  "Returns the health info of a node"
  ([conn node]
   (node-health conn node {}))
  ([conn node {:keys [dc] :as params}]
   (consul-index conn :get [:health :node node] {:query-params params})))

(defn service-health-checks
  "Returns the checks of a service"
  ([conn service]
   (service-health-checks conn service {}))
  ([conn service {:as params}]
   (consul-index conn :get [:health :checks service] {:query-params params})))

(defn service-health
  "Returns the nodes and health info of a service."
  ([conn service]
   (service-health conn service {}))
  ([conn service {:keys [dc tag passing?] :or {passing? false}:as params}]
   (consul-index conn :get [:health :service service]
                 {:query-params (cond-> (dissoc params :passing?)
                                  passing? (assoc :passing ""))})))

(defn health-state
  "Returns the checks in a given state (any, unknown, passing, warning or critical)"
  ([conn state]
   (health-state conn state {}))
  ([conn state {:as params}]
   (consul-index conn :get [:health :state state] {:query-params params})))


;; Status - https://www.consul.io/docs/agent/http/status.html

(defn leader
  "Returns the current Raft leader."
  [conn]
  (:body (consul conn :get [:status :leader])))

(defn peers
  "Returns the Raft peers for the datacenter in which the agent is running."
  [conn]
  (:body (consul conn :get [:status :peers])))

;; Helper functions

(defn passing?
  "Returns true if check is passing"
  [check]
  (contains? #{"passing"} (:Status check)))

(defn healthy-service?
  "Returns true if every check is passing for each object returned from /v1/health/service/<service>."
  [health-service]
  (every? passing? (:Checks health-service)))

;; Prepared queries
(defn prepared-queries
  "Return the installed prepared queries"
  [conn]
  (->> (consul conn :get [:query])
       :body
       (reduce (fn [r q] (assoc r (:Name q) q)) {})))

(defn create-prepared-query
  "Create a new prepared query based on the provided entry map, keys are automatically transformed
   to the format Consul wants"
  [conn {:keys [name token session service-name fail-over near only-passing? tags ttl] :as entry}]
  (:body (consul conn :post [:query] {:body (map->prepared-query entry) :query-params {}})))

(defn delete-prepared-query
  "Delete a prepared query by its ID (note: names do not work here).
  The ID can either be given as a String, or as a java.util.UUID"
  [conn query-id]
  (consul-200 conn :delete [:query (.toString query-id)] {}))

(defn execute-prepared-query
  "Execute a prepared query by either its ID (a UUID) or
   its name"
  [conn query-id]
  (:body (consul conn :get [:query (.toString query-id) :execute])))

(defn explain-prepared-query
  "This generates a fully-rendered query for a given, post interpolation"
  [conn query-id]
  (:body (consul conn :get [:query (.toString query-id) :explain])))

(comment
  (def m {:name "query-bourne-2"
          :service {:service "bourne-api-development"
                    :failover {:nearest-n 4
                               :datacenters ["dc1" "dc2"]}
                    :near "_ip"
                    :only-passing true
                    :tags ["development" "!experimental"]}
          :dns {:ttl "10s"}})
  (->> (prepared-queries :local)
       vals
       (map :ID)
       (map (partial delete-prepared-query :local)))
  (prepared-queries :local)
  (create-prepared-query :local m)
  (delete-prepared-query :local "8a9dfb89-4f1e-1ecf-92db-f8c8de2ea137")
  (execute-prepared-query :local "query-bourne-2")
  (explain-prepared-query :local "query-bourne-2"))
