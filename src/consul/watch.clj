(ns consul.watch
  (:require [consul.core :as consul]
            [clojure.core.async :as async]))

(def watch-fns
  {:key       consul/kv-get
   :keyprefix consul/kv-recurse
   :service   consul/service-health})

(defn poll! [conn [kw x] params]
  (let [args (apply concat (seq params))
        f    (get watch-fns kw)]
    (apply f conn x args)))

(defn poll
  "Calls consul for the given spec passing params.  Returns the exception if one occurs."
  [conn spec params]
  (try (poll! conn spec params)
       (catch Exception err
         err)))

(defn long-poll
  "Polls consul using spec and publishes results onto ch.  Applies no throttling of requests towards consul except via ch."
  [conn spec ch & {:as options}]
  (assert (get watch-fns (first spec) (str "unimplemented watch type " spec)))
  (async/go-loop [resp (async/<! (async/thread (poll conn spec options)))]
    (let [v (async/>! ch resp)]
      (when (true? v)
        (recur (async/<! (async/thread (if (consul/ex-info? resp)
                                         (poll conn spec options) ;; don't rely on the old index if we experience a consul failure
                                         (poll conn spec (merge options {:index (:modify-index (meta resp)) :wait "45s"}))))))))))

(defn update-state
  "When called with new-config, creates a new state map.  When called with old-state and new-config, old-state is updated with new-config."
  ([new-config] {:config new-config :failures 0})
  ([old-state new-config]
   (if (consul/ex-info? new-config)
     (-> old-state
         (update-in [:failures] inc)
         (assoc :error new-config))
     (assoc old-state :config new-config :failures 0))))

(defn exp-wait [n max-ms]
  {:pre [(number? n) (>= n 0) (number? max-ms) (> max-ms 0)]}
  (min (* (Math/pow 2 n) 100) max-ms))

(defn setup-watch
  "Creates a watching channel and notifies changes to a change-fn

  :query-params   - the query params passed into the underlying service call
  :max-retry-wait - max interval before retying consul when a failure occurs.  Defaults to 5s.

  Returns a function that shuts down the channel when called."
  [conn [watch-key path :as spec] change-fn {:keys [max-retry-wait query-params] :as options}]
  (let [state (atom nil)
        ch (async/chan)]
    (apply long-poll conn spec ch (mapcat identity query-params))
    (async/go-loop []
      (when-let [new-config (async/<! ch)]
        (let [old-state @state]
          (when (not= (:config old-state) new-config)
            (swap! state update-state new-config)
            (if-not (consul/ex-info? new-config)
              (change-fn (:config @state))
              (async/<! (async/timeout (exp-wait (or (:failures @state) 0) (get options :max-retry-wait 5000))))))
          (recur))))
    #(async/close! ch)))

(defn ttl-check-update
  [conn check-id ^long ms ch]
  "Periodically updates check-id according to freq. Exits if ch closes."
  {:pre [(string? check-id) ms]}
  (async/go-loop []
    (async/<! (async/thread (consul/agent-pass-check conn check-id)))
    (when (async/alt! ch
            ([v] v)
            (async/timeout ms)
            ([_] :continue))
      (recur))))

(defn check-id [service-definition]
  (str "service:" (:id service-definition)))

(defn register-service
  "Register a service and keeps sending a health check to Consul every update-ms"
  [conn service-definition update-ms]
  (let [control-ch (async/chan)]
    (consul/agent-register-service conn service-definition)
    (ttl-check-update conn (check-id service-definition) update-ms control-ch)
    (fn []
      (async/close! control-ch)
      (consul/agent-deregister-service conn (:id service-definition)))))

(defn is-leader?
  [conn key]
  (let [session ()]))