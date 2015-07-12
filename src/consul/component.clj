(ns consul.component
  (:require [consul.core :as consul]
            [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [com.stuartsierra.dependency :as deps]
            [taoensso.timbre :as timbre :refer (log trace debug info warn error fatal spy)]))


(defn log-changes [key atom old-state new-state]
  (when (not= old-state new-state)
    (info "detected changes from consul")
    (info "was:")
    (spy :info old-state)
    (info "now:")
    (spy :info new-state)))

(def watch-fns
  {:key     consul/kv-get
   :service consul/service-health})

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

(defn watch
  "Watches consul for changes associated with spec and publishes them onto ch."
  [conn spec ch & {:as options}]
  (assert (get watch-fns (first spec) (str "unimplemented watch type " spec)))
  (async/go-loop [resp (async/<! (async/thread (poll conn spec options)))]
    (let [v (async/>! ch resp)]
      (if (true? v)
        (recur (async/<! (async/thread (if (consul/ex-info? resp)
                                         (poll conn spec options) ;; don't rely on the old index if we experience a consul failure
                                         (poll conn spec (merge options {:index (:modify-index (meta resp)) :wait "45s"}))))))
        (do (info "exiting consul watch")
            (spy :debug spec))))))

(defn update-state
  "When called with new-config, creates a new state map.  When called with old-state and new-config, old-state is updated with new-config."
  ([new-config] {:config new-config :failures 0})
  ([old-state new-config]
   (if (consul/ex-info? new-config)
     (-> old-state
         (update-in [:failures] inc)
         (assoc :error new-config))
     (-> old-state
         (assoc :config new-config :failures 0)
         (dissoc :error)))))

(defn exp-wait [n max-ms]
  {:pre [(number? n) (>= n 0) (number? max-ms) (> max-ms 0)]}
  (min (* (Math/pow 2 n) 100) max-ms))

(defrecord WatchComponent [conn spec ch state options]
  component/Lifecycle
  (start [{:keys [ch] :as component}]
    (info "starting consul watch")
    (spy :info spec)
    (let [_          (add-watch state :logger log-changes)
          watcher-ch (watch conn spec ch)
          updater-ch (async/go-loop []
                       (when-let [new-config (async/<! ch)]
                         (swap! state update-state new-config)
                         (when (consul/ex-info? new-config)
                           (warn "failure polling consul")
                           (spy :warn new-config)
                           (async/<! (async/timeout (exp-wait (or (:failures @state) 0) (get options :max-retry-wait 5000)))))
                         (recur)))
          resp       (async/<!! ch)]
      (reset! state (update-state resp))
      (assoc component :watcher-ch watcher-ch :updater-ch updater-ch)))
  (stop [{:keys [ch] :as component}]
    (info "stopping consul watch")
    (spy :info spec)
    (async/close! ch)
    component))

(defn watch-component
  "Creates a component that watches for changes to spec and caches results in state.

  :query-params   - the query params passed into the underlying service call
  :max-retry-wait - max interval before retying consul when a failure occurs.  Defaults to 5s."
  [conn spec & {:as options}]
  (map->WatchComponent {:conn conn :spec spec :ch (async/chan) :state (atom nil) :options options}))

(defn ttl-check-update
  [conn check-id ^long ms ch]
  "Periodically updates check-id according to freq. Exits if ch closes."
  {:pre [(string? check-id) ms]}
  (async/go-loop []
    (async/<! (async/thread (consul/agent-pass-check conn check-id)))
    (when (async/alt! ch ([v] v) (async/timeout ms) ([_] :continue))
      (recur))))

(defn check-id [service-definition]
  (str "service:" (:id service-definition)))

(defrecord ServiceRegistrar [conn service-definition update-ms control-ch]
  component/Lifecycle
  (start [registration]
    (consul/agent-register-service conn service-definition)
    (assoc registration :check-updater (ttl-check-update conn (check-id service-definition) update-ms control-ch)))
  (stop [registration]
    (async/close! control-ch)
    (consul/agent-deregister-service conn (:id service-definition))
    registration))

(defn service-registrar
  "Creates a component that registers the application service with consul."
  [conn service-definition update-ms & {:as opts}]
  (map->ServiceRegistrar {:conn conn :service-definition service-definition :control-ch (async/chan) :update-ms update-ms}))
