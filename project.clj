(defproject consul-clojure "0.1.0-SNAPSHOT"
  :description "A Consul client for Clojure applications."
  :url "http://github.com/bpoweski/consul-clojure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[camel-snake-kebab "0.3.1" :exclusions [org.clojure/clojure com.keminglabs/cljx]]
                 [cheshire "5.5.0"]
                 [clj-http-lite "0.2.1" :exclusions [org.clojure/clojure]]
                 [com.stuartsierra/component "0.2.2"]]
  :profiles {:dev {:dependencies [[info.sunng/ring-jetty9-adapter "0.8.2" :exclusions [ring/ring-core org.clojure/clojure]]
                                  [ring/ring-core "1.4.0"]
                                  [org.clojure/clojure "1.5.1"]
                                  [org.clojure/tools.trace "0.7.8"]
                                  [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
                   :jvm-opts ["-XX:MaxPermSize=128M"]}})
