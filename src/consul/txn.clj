(ns consul.txn
  (:require [consul.core :as core]))

(defn- build-operation
  "Create a map for a txn operation. Currently only :KV is supported."
  [operation verb key params]
  {operation (merge {:Verb verb :Key key} params)})

(defn- build-params
  "Create the request parameters."
  [params]
  (reduce-kv (fn [acc key val]
               (cond
                 (and (= key :stale) val)            (assoc acc :stale "")
                 (and (= key :consistent) val)       (assoc acc :consistent "")
                 (and (= key :stale) (not val))      (dissoc acc :stale)
                 (and (= key :consistent) (not val)) (dissoc acc :consistent)
                 (= key :string?)                    acc
                 :else                               (assoc acc key val)))
             {} params))

(defn put
  "Starts an atomic transaction. Available in Consul 0.7 and later.

  Parameters:

  :dc      - Optional data center in which to run the transaction, defaults agent's data center.
  :token   - The ACL token
  :string? - Converts the value returned into a string, if the result has a value."
  ([conn f]
   (put conn {} f))

  ([conn {:keys [dc token stale consistent string?] :or {stale? false consistent? false string? true} :as params} f]
   (let [operations (-> [] transient f persistent!)
         response   (core/consul conn :put [:txn] {:body operations
                                                   :query-params (build-params params)})]
     (map (fn [result] (core/kv-map-convert (:KV result) string?))
          (get-in response [:body :Results])))))

(defn kv-set
  "Sets the key to the given value"
  [tx key value & {:keys [flags] :as optional}]
  (let [base   {:Value (core/str->base64 value)}
        params (if flags (assoc base :Flags flags) base)
        op     (build-operation :KV "set" key params)]
    (conj! tx op)))

(defn kv-set-cas
  "Sets the key ot the given value with check-and-set semantics.
  The key will only be set if its current modify index matches the
  supplied index"
  [tx key value index & {:keys [flags] :as optional}]
  (let [base   {:Value (core/str->base64 value)
                :Index index}
        params (if flags (assoc base :Flags flags) base)
        op     (build-operation :KV "cas" key params)]
    (conj! tx op)))

(defn kv-lock
  "Unlocks the key with the given Session. The key will only release
  the lock if the session is valid and currently has it locked."
  [tx key value session & {:keys [flags] :as optional}]
  (let [base   {:Value (core/str->base64 value)
                :Session session}
        params (if flags (assoc base :Flags flags) base)
        op     (build-operation :KV "lock" key params)]
    (conj! tx op)))

(defn kv-unlock
  "Gets the key during the transaction. This fails the transaction if
  the key doesn't exist. The key may not be present in the results if
  ACLs do not permit it to be read."
  [tx key value session & {:keys [flags] :as optional}]
  (let [base   {:Value (core/str->base64 value)
                :Session session}
        params (if flags (assoc base :Flags flags) base)
        op     (build-operation :KV "unlock" key params)]
    (conj! tx op)))

(defn kv-get
  "Gets the key during the transaction. This fails the transaction if
  the key doesn't exist. The key may not be present in the results if
  ACLs do not permit it to be read."
  [tx key]
  (conj! tx (build-operation :KV "get" key nil)))

(defn kv-get-tree
  "Gets all keys with a prefix of key during the transaction. This does
  not fail the transaction if the key doesn't exist. Not all keys may be
  present in the results if ACLs do not permit them to be read."
  [tx prefix]
  (conj! tx (build-operation :KV "get-tree" prefix nil)))

(defn kv-check-index
  "Fails the transaction if key does not have a modify index equal to
  supplied index."
  [tx key index]
  (conj! tx (build-operation :KV "check-index" key {:Index index})))

(defn kv-check-session
  "Fails the transaction if key is not currently locked by session."
  [tx key session]
  (conj! tx (build-operation :KV "check-session" key {:Session session})))

(defn kv-delete
  "Deletes the key."
  [tx key]
  (conj! tx (build-operation :KV "delete" key nil)))

(defn kv-delete-tree
  "Deletes all keys with a prefix of key."
  [tx prefix]
  (conj! tx (build-operation :KV "delete-tree" prefix nil)))

(defn kv-delete-cas
  "Deletes the key with check-and-set semantics. The key will only
  be deleted if its current modify index matches the supplied index."
  [tx key index]
  (conj! tx (build-operation :KV "delete-cas" key {:Index index})))
