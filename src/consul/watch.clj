(ns consul.watch
  (:require [consul.core :as consul]
            [clojure.core.async :as async]))

(def watch-fns
  {:key       consul/kv-get
   :keyprefix consul/kv-recurse
   :service   consul/service-health})

(defn poll! [conn [kw x] params]
  (let [f    (get watch-fns kw)]
    (f conn x params)))

(defn poll
  "Calls consul for the given spec passing params.  Returns the exception if one occurs."
  [conn spec params]
  (try (poll! conn spec params)
       (catch Exception err
         err)))

(defn long-poll
  "Polls consul using spec and publishes results onto ch.  Applies no throttling of requests towards consul except via ch."
  [conn spec ch {:as options}]
  (assert (get watch-fns (first spec) (str "unimplemented watch type " spec)))
  (async/go-loop [resp (async/<! (async/thread (poll conn spec options)))]
    (when (async/>! ch resp)
      (recur (async/<! (async/thread (if (consul/ex-info? resp)
                                       (poll conn spec options) ;; don't rely on the old index if we experience a consul failure
                                       (poll conn spec (merge options {:index (:modify-index resp) :wait "45s"})))))))))

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
  "Creates a watching channel and notifies changes to a change-chan channel

  :query-params   - the query params passed into the underlying service call
  :max-retry-wait - max interval before retying consul when a failure occurs.  Defaults to 5s.

  The watch will terminate when the change-chan output channel is closed or when resulting
  function is called"
  [conn [watch-key path :as spec] change-chan {:keys [max-retry-wait query-params log] :as options}]
  (let [ch (async/chan)
        log (or log (fn [& _]))]
    (long-poll conn spec ch query-params)
    (async/go
      (loop [old-state nil]
        (log "Start watching " spec)
        (when-let [new-config (async/<! ch)]
          (if-not (consul/ex-info? new-config) (log "Message: " new-config))
          (cond
            (consul/ex-info? new-config)
            (do
              (async/<! (async/timeout (exp-wait (or (:failures old-state) 0) (get options :max-retry-wait 5000))))
              (recur (update-state old-state new-config)))
            (not= (:mapped (:config old-state)) (:mapped new-config))
            (do
              (log "State changed for " spec " : " new-config)
              (when (async/>! change-chan new-config)
                (recur (update-state old-state new-config))))
            :else
            (recur (update-state old-state new-config)))))
      (log "Finished watching " spec))
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

(defn setup-leader-watch [name]
  (let [w (async/chan (async/sliding-buffer 1))]
    (async/go-loop [m (async/<! w)]
      (when m
        (println name ": " m)
        (recur (async/<! w))))
    w))


;; TODO: The session needs to manage its own ttl renewal and shutdown lifecycle.

(defn leader-watch
  "Setup a leader election watch on a given key, sends a vector with [path true/false] when the
   election changes.

   If you close the leader-ch, it releases leader for the key"
  [conn {:keys [lock-delay node name checks behavior ttl] :as session} k leader-ch {:keys [max-retry-wait query-params log] :as options}]
  (let [ch (async/chan)
        log (or log (fn [& _]))]
    (long-poll conn [:key k] ch query-params)
    (log "Start leader election on" k)
    (async/go
      ;; We don't need the initial value.
      (async/<! ch)
      (loop [state {}]
        (log "")
        (log "")
        (log "---------- Loop with " state)
        (let [sessioninfo
              (if (:session state)
                (try (:body (consul/session-info conn (:session state)))
                     (catch Exception e (log "Exception info session"))))
              _ (log "session info: " sessioninfo)
              sessionid
              (if (nil? sessioninfo)
                (try
                  (log "Create session " k)
                  (consul/session-create conn session)
                   (catch Exception e (log "Exception creating session: " (.getMessage e)) (.printStackTrace e)))
                (:session state))
              _ (log "Sessionid: " sessionid)
              state
              (assoc state :session sessionid)]
          (try
            (when (and sessionid (not (:leader state)))
              (log "Acquiring on " sessionid)
              (let [a (consul/kv-put conn k "1" {:acquire sessionid})]
                (log "Acquire: " a)))
            (catch Exception e
              (log "Exception acquiring session: " (.getMessage e))
              (.printStackTrace e)))

          (cond
            ;; First, keep the session going, creating a new one if needed
            (nil? sessionid)
            (when (async/>! leader-ch [k false])
              (log "Invalid session, leader lost for" k " - " state)
              (async/<! (async/timeout 2000))
              (recur state))
            :else
            (do
              (log "Waiting for change on " k ", session: " sessionid)
              (when-let [result (async/<! ch)]
                (log "Received: " result)
                (cond
                  (consul/ex-info? result)
                  (when (async/>! leader-ch [k false])
                    (log "Error, release leader for" k " - " (.getMessage result))
                    (async/<! (async/timeout (exp-wait (or (:failures state) 0) (get options :max-retry-wait 5000))))
                    (recur (update-state state result)))
                  (and (:leader state) (not= sessionid (:session result)))
                  (when (async/>! leader-ch [k false])
                    (log "Leader lost for" k " - " result)
                    (async/<! (async/timeout 15000))
                    (recur (assoc state :leader false)))
                  (and (not (:leader state)) (= sessionid (:session result)))
                  (when (async/>! leader-ch [k true])
                    (log "Leader gained for" k " - " result)
                    (async/<! (async/timeout 15000))
                    (recur (assoc state :leader true)))
                  :else
                  (do
                    (async/<! (async/timeout 15000))
                    (recur state))))))))

      ;; Release out here
      (log "Finished and releasing:" k)
      (async/>! leader-ch [k false])
      (consul/kv-put conn k "1" {:release session}))))

