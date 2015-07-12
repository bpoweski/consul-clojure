(ns attache.example
  "Example of registering a Jetty service."
  (:require [attache.component :as service-component]
            [com.stuartsierra.component :as component]
            [ring.adapter.jetty9 :as jetty]
            [ring.util.response :as response]
            [taoensso.timbre :refer [spy info warn]]))

(def running-system (atom nil))

(defn handler [request]
  (response/response "hello"))

(defrecord WebComponent [service-name port handler]
  component/Lifecycle
  (start [web]
    (info "starting jetty")
    (assoc web :server (jetty/run-jetty #'handler {:join? false :port port})))
  (stop [web]
    (if-let [server (:server web)]
      (do (info "stopping jetty")
          (.stop server)
          (.join server)
          (info "jetty requests have completed")
          (dissoc web :server))
      web)))

(defn make-system
  "Creates an example system."
  [{:keys [service-name server-port service-id] :or {server-port 8080 service-name "jetty" service-id "jetty-8080"} :as opts}]
  (component/system-map
   :web (map->WebComponent {:service-name (name service-name) :port server-port})
   :registrar (service-component/service-registrar :local {:id service-id :name service-name :check {:ttl "30s"}} 15000)))

(defn shutdown-system! []
  (when-let [system @running-system]
    (component/stop-system system)
    (reset! running-system nil))
  (shutdown-agents))

(defn -main [& {:as opts}]
  (->> (reduce-kv #(assoc %1 (read-string %2) (read-string %3)) {} opts)
       make-system
       component/start-system
       (reset! running-system))
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable shutdown-system!)))
