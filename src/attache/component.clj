(ns attache.component
  (:require [attache.core :as attache]
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

(defn watch-consul
  "Watches consul for changes associated with spec and publishes them onto ch."
  [conn spec ch]
  (async/go-loop [resp (async/<! (async/thread (attache/kv-get conn (:key spec))))]
    (let [v (async/>! ch resp)]
      (if (true? v)
        (recur (async/<! (async/thread
                           (if (attache/ex-info? resp)
                             (attache/kv-get conn (:key spec)) ;; ditch the index for faster clearing of errors
                             (attache/kv-get conn (:key spec) :index (:modify-index (meta resp)) :wait "45s")))))
        (do (info "exiting consul watch")
            (spy :debug spec))))))

(defn update-state
  "When called with new-config, creates a new state map.  When called with old-state and new-config, old-state is updated with new-config."
  ([new-config] {:config new-config :failures 0})
  ([old-state new-config]
   (if (attache/ex-info? new-config)
     (-> old-state
         (update-in [:failures] inc)
         (assoc :error new-config))
     (-> old-state
         (assoc :config new-config :failures 0)
         (dissoc :error)))))

(defrecord WatchComponent [conn spec ch state options]
  component/Lifecycle
  (start [{:keys [ch] :as watch}]
    (info "starting consul watch component")
    (spy :info spec)
    (let [_          (add-watch state :logger log-changes)
          watcher-ch (watch-consul conn spec ch)
          updater-ch (async/go-loop []
                       (when-let [new-config (async/<! ch)]
                         (swap! state update-state new-config)
                         (when (attache/ex-info? new-config)
                           (warn "failure when polling consul")
                           (spy :warn new-config)
                           (async/<! (async/timeout (Math/pow 2 (or (:failures @state) 0)))))
                         (recur)))
          resp       (async/<!! ch)]
      (reset! state (update-state resp))
      (assoc watch :watcher-ch watcher-ch :updater-ch updater-ch)))
  (stop [{:keys [ch] :as watch}]
    (info "stopping consul watch component")
    (spy :info spec)
    (async/close! ch)
    watch))

(defn watch-component
  "Creates a component that watches for changes to spec and caches results in state."
  [conn spec options]
  (map->WatchComponent {:conn conn :spec spec :ch (async/chan) :state (atom nil) :options (merge {:wait "45s"} options)}))
