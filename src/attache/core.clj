(ns attache.core
  (:require [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [cheshire.core :as json]
            [clj-http.lite.client :as client]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (javax.xml.bind DatatypeConverter)))


(defn base64->str [^String s]
  (String. (DatatypeConverter/parseBase64Binary s) "UTF8"))

(defn str->base64 [^String s]
  (DatatypeConverter/printBase64Binary (.getBytes s)))

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
    (map? body)        (update-in [:body] json/generate-string)))

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
          (throw (ex-info "application failure" {:reason :application-failure :conn conn :endpoint endpoint :request request :http-request http-request :http-response response}))))
      (catch java.net.ConnectException ex
        (throw (ex-info "connection failure" {:reason :connect-failure :conn conn :endpoint endpoint :request request :http-request http-request} ex)))
      (catch java.net.UnknownHostException ex
        (throw (ex-info "unknown host" {:reason :unknown-host :conn conn :endpoint endpoint :request request :http-request http-request} ex))))))

(defn headers->index
  "Selects the X-Consul-* headers into a map with keywordized names."
  [m]
  (reduce-kv (fn [m k v]
               (cond (= "x-consul-knownleader" k)             (assoc m :known-leader (= "true" v))
                     (and (= "x-consul-index" k) (string? v)) (assoc m :modify-index (Long/parseLong v))
                     (re-matches #"x-consul-.*" k)            (assoc m (keyword k) v)
                     :else                                    m))
             {}
             m))

(defn kv-map->vec
  [response convert?]
  {:pre [(map? response)]}
  (with-meta [(:Key response) (if (and convert? (string? (:Value response)))
                                (base64->str (:Value response))
                                (:Value response))]
    (cske/transform-keys csk/->kebab-case-keyword (dissoc response :Key :Value))))

(defn kv-keys
  "Retrieves a set of keys using prefix.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :separator - List keys up to separator."
  [conn prefix & {:as params}]
  (let [{:keys [body headers status] :as response} (consul conn :get [:kv prefix] {:query-params (assoc params :keys "")})]
    (cond (= 404 status) (with-meta #{} (headers->index headers))
          (seq body) (with-meta (into #{} body) (headers->index headers)))))

(defn kv-get
  "Retrieves key k from consul given the following optional parameters.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :raw?      - If true and used with a non-recursive GET, the response is just the raw value of the key, without any encoding.
  :string?    - Converts the value returned for k into a string.  Defaults to true."
  [conn k & {:as params :keys [raw? string?] :or {raw? false string? true}}]
  (let [{:keys [body headers] :as response} (consul conn :get [:kv k] {:query-params (cond-> (dissoc params :raw? :string?)
                                                                                                    raw? (assoc :raw ""))})]
    (cond (or (nil? body) raw?) (with-meta [k body] (headers->index headers))
          :else (kv-map->vec (first body) string?))))

(defn kv-recurse
  "Retrieves key k from consul given the following optional parameters.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :wait      - Used in conjunction with :index to get using a blocking query. e.g. 10s, 1m, etc.
  :index     - The current Consul index, suitable for making subsequent calls to wait for changes since this query was last run.

  :recurse?  - If true, then consul it will return all keys with the given prefix. Defaults to false.
  :binary?   - Converts the value returned for k into a byte array, defaults to converting Value into a UTF-8 String. "
  [conn prefix & {:as params}]
  (let [{:keys [body headers] :as response} (consul conn :get [:kv prefix] {:query-params (assoc params :recurse "")})]
    (if (nil? body)
      (with-meta #{} (headers->index headers))
      (with-meta (reduce #(conj %1 (kv-map->vec %2 false)) #{} body) (headers->index headers)))))

(defn kv-put
  "Sets key k with value v.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.

  :flags     - This can be used to specify an unsigned value between 0 and 264-1.
  :cas       - Uses a CAS index when updating. If the index is 0, puts only if the key does not exist. If index is non-zero, index must match the ModifyIndex of that key.
  :acquire   - Updates using lock acquisition. A key does not need to exist to be acquired.
  :release   - Yields a lock acquired with :acquire."
  [conn k v & {:as params}]
  (:body (consul conn :put [:kv k] {:query-params params :body v})))

(defn kv-del
  "Deletes a key k from consul.

  Parameters:

  :dc        - Optional data center in which to retrieve k, defaults agent's data center.
  :recurse?  - If true, then consul it will return all keys with the given prefix. Defaults to false.
  :cas       - Uses a CAS index when updating. If the index is 0, puts only if the key does not exist. If index is non-zero, index must match the ModifyIndex of that key."
  [conn k & {:as params :keys [recurse?] :or {recurse? false}}]
  (:body (consul conn :delete [:kv k] {:query-params (cond-> (dissoc params :recurse?)
                                                       recurse? (assoc :recurse ""))})))

(defn shallow-nameify-keys
  "Returns a map with the root keys converted to strings using name."
  [m]
  (reduce-kv #(assoc %1 (name %2) %3) {} m))

(defn agent-checks
  "Returns all the checks that are registered with the local agent as a map with the check names as strings. "
  [conn & {:as params}]
  (shallow-nameify-keys (:body (consul conn :get [:agent :checks] {:query-params params}))))

(defn agent-services
  "Returns all services registered with the local agent."
  [conn & {:as params}]
  (shallow-nameify-keys (:body (consul conn :get [:agent :services] {:query-params params}))))

(defn agent-members
  "Returns the members as seen by the local serf agent."
  [conn & {:as params}]
  (:body (consul conn :get [:agent :members] {:query-params params})))

(defn agent-self
  "Returns the local node configuration."
  [conn & {:as params}]
  (:body (consul conn :get [:agent :self] {:query-params params})))

(defn agent-maintenance
  "Manages node maintenance mode.

  Requires a boolean value for enable?.  An optional :reason can be provided, returns true if successful."
  [conn enable? & {:as params}]
  {:pre [(contains? #{true false} enable?)]}
  (let [{:keys [status] :as response} (consul conn :put [:agent :maintenance] {:query-params (assoc params :enable enable?)})]
    (= 200 status)))

(defn agent-join
  "Triggers the local agent to join a node.  Returns true if successful."
  [conn address & {:as params}]
  (let [{:keys [status] :as response} (consul conn :get [:agent :join address] {:query-params params})]
    (= 200 status)))

(defn agent-force-leave
  "Forces removal of a node, returns true if the request succeeded."
  [conn node & {:as params}]
  (let [{:keys [status] :as response} (consul conn :get [:agent :force-leave node] {:query-params params})]
    (= 200 status)))

(def consul-screaming-pascal-case-substitutions
  "Differences from PascalCase used by Consul."
  {:Http      :HTTP
   :Id        :ID
   :Ttl       :TTL
   :ServiceId :ServiceID
   :CheckId   :CheckID})

(defn check?
  "Simple validation outlined in https://www.consul.io/docs/agent/checks.html."
  [{:keys [Name HTTP TTL Script Interval] :as check}]
  (and (string? Name)
       (or (string? Script) (string? HTTP) (string? :TTL))
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
  (-> (cske/transform-keys csk/->PascalCase check)
      (set/rename-keys consul-screaming-pascal-case-substitutions)))

(defn agent-register-check
  "Registers a new check with the local agent."
  [conn m & {:as params}]
  (let [{:keys [status] :as response} (consul conn :put [:agent :check :register] {:body (map->check m) :query-params params})]
    (= 200 status)))

(defn agent-deregister-check
  "Deregisters a check with the local agent."
  [conn check-id & {:as params}]
  (let [{:keys [status] :as response} (consul conn :delete [:agent :check :deregister check-id] {:query-params params})]
    (= 200 status)))

;; /v1/agent/check/pass/<checkID> : Marks a local test as passing
(defn agent-pass-check
  "Marks a local check as passing."
  [conn check-id & {:as params}]
  (let [{:keys [status] :as response} (consul conn :delete [:agent :check :pass check-id] {:query-params params})]
    (= 200 status)))

;; /v1/agent/check/warn/<checkID> : Marks a local test as warning

;; /v1/agent/check/fail/<checkID> : Marks a local test as critical

;; /v1/agent/service/register : Registers a new local service

;; /v1/agent/service/deregister/<serviceID> : Deregisters a local service

;; /v1/agent/service/maintenance/<serviceID> : Manages service maintenance mode


;; Health Checks

;; /v1/health/node/<node>: Returns the health info of a node

;; /v1/health/checks/<service>: Returns the checks of a service
(defn service-health-checks
  "Returns the checks of a service"
  [conn service & {:as params}]
  (let [{:keys [body headers] :as response} (consul conn :get [:health :service (str service)])]
    (vary-meta body merge (headers->index headers))))

;; /v1/health/service/<service>: Returns the nodes and health info of a service

(defn service-health
  "Returns the nodes and health info of a service."
  [conn service & {:as params :keys [passing?] :or {passing? false}}]
  (let [{:keys [body headers] :as response} (consul conn :get [:health :service service] {:query-params (cond-> (dissoc params :passing?)
                                                                                                                       passing? (assoc :passing ""))})]
    (vary-meta body merge (headers->index headers))))

;; /v1/health/state/<state>: Returns the checks in a given state

(defn leader
  "Returns the current Raft leader."
  [conn]
  (let [{:keys [body]} (consul conn :get [:status :leader])]
    body))

(defn peers
  "Returns the Raft peers for the datacenter in which the agent is running."
  [conn]
  (let [{:keys [body] :as response} (consul conn :get [:status :peers])]
    body))
