(ns consul.watch-test
  (:require [consul.watch :as w]
            [consul.core :as consul]
            [clojure.core.async :as async]
            [clojure.test :refer :all]))


(defn cleanup [f]
  (consul/kv-del :local "consul-clojure" {:recurse? true})
  (f)
  (consul/kv-del :local "consul-clojure" {:recurse? true}))

(use-fixtures :each cleanup)

(defn try<!! [ch]
  (async/alt!! ch ([v] v) (async/timeout 600) :timeout))

(deftest long-poll-kv-test
  (testing "immediate returns the initial kv pair"
    (let [ch       (async/chan 1)
          watch-ch (w/long-poll :local [:key "consul-clojure.key"] ch {})]
      (is (= ["consul-clojure.key" nil] (consul/kv (try<!! ch))))
      (consul/kv-put :local "consul-clojure.key" "value")
      (is (= ["consul-clojure.key" "value"] (consul/kv (try<!! ch))))
      (consul/kv-put :local "consul-clojure.key" "value2")
      (is (= ["consul-clojure.key" "value2"] (consul/kv (try<!! ch))))
      (consul/kv-del :local "consul-clojure.key")
      (is (=  ["consul-clojure.key" nil] (consul/kv (try<!! ch))))))
  (testing "with a connection error"
    (let [ch      (async/chan 1)
          poll-ch (w/long-poll {:server-port 8501} [:key "consul-clojure.key"] ch {})
          result  (try<!! ch)]
      (is (consul/ex-info? (try<!! ch))))))

(deftest exp-wait-test
  (is (= 100.0 (w/exp-wait 0 10000)))
  (is (= 1000 (w/exp-wait 5 1000)))
  (is (= 3200.0 (w/exp-wait 5 5000))))

;; (deftest watch-component-test
;;   (testing "watch a specific KV pair"
;;     (let [watch (component/start (watch-component :local [:key "consul-clojure.keytest"]))]
;;       (is (= {:config ["consul-clojure.keytest" nil], :failures 0} @(:state watch)))
;;       (consul/kv-put :local "consul-clojure.keytest" "1")
;;       (async/<!! (async/timeout 100))
;;       (is (= {:config ["consul-clojure.keytest" "1"], :failures 0} @(:state watch)))
;;       (component/stop watch))))
