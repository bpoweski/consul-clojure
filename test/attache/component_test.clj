(ns attache.component-test
  (:require [attache.component :refer :all]
            [attache.core :as attache]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [taoensso.timbre :as timbre :refer (log trace debug info warn error fatal spy)]))


(defn cleanup [f]
  (attache/kv-del :local "attache" :recurse? true)
  (f)
  (attache/kv-del :local "attache" :recurse? true))

(use-fixtures :each cleanup)

(defn try<!! [ch]
  (async/alt!! ch ([v] v) (async/timeout 600) :timeout))

(deftest watch-kv-test
  (testing "immediate returns the initial kv pair"
    (let [ch       (async/chan 1)
          watch-ch (watch-consul :local {:type :key :key "attache.key"} ch)]
      (is (= ["attache.key" nil] (try<!! ch)))
      (attache/kv-put :local "attache.key" "value")
      (is (= ["attache.key" "value"] (try<!! ch)))
      (attache/kv-put :local "attache.key" "value2")
      (is (= ["attache.key" "value2"] (try<!! ch)))
      (attache/kv-del :local "attache.key")
      (is (= ["attache.key" nil] (try<!! ch)))))
  (testing "with a connection error"
    (let [ch       (async/chan 1)
          watch-ch (watch-consul {:server-port 8501} {:type :key :key "attache.key"} ch)
          result   (try<!! ch)]
      (is (attache/ex-info? (try<!! ch))))))

(deftest exp-wait-test
  (is (= 100.0 (exp-wait 0 10000)))
  (is (= 1000 (exp-wait 5 1000)))
  (is (= 3200.0 (exp-wait 5 5000))))

(deftest watch-component-test
  (testing "watch a specific KV pair"
    (let [watch (component/start (watch-component :local {:key "attache.keytest" :type :key}))]
      (is (= {:config ["attache.keytest" nil], :failures 0} @(:state watch)))
      (attache/kv-put :local "attache.keytest" "1")
      (async/<!! (async/timeout 100))
      (is (= {:config ["attache.keytest" "1"], :failures 0} @(:state watch)))
      (component/stop watch)))
  (testing "watch a prefix in the KV store"))
