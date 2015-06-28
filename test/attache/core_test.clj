(ns attache.core-test
  (:require [clojure.test :refer :all]
            [attache.core :refer :all]
            [com.stuartsierra.component :as component])
  (:import (java.util UUID)))


(defn cleanup [f]
  (kv-del :local "attache" :recurse? true)
  (f)
  (kv-del :local "attache" :recurse? true))

(deftest endpoint->path-test
  (testing "Key/Value store"
    (are [path endpoint] (= path (endpoint->path endpoint))
         "/v1/kv/key"              [:kv "key"]
         "/v1/agent/services"      [:agent :services]
         "/v1/agent/join/10.1.1.2" [:agent :join "10.1.1.2"])))

(deftest consul-request-test
  (testing ":local conn"
    (is (= 8500 (:server-port (consul-request :local :get [:agent :checks]))))
    (is (= "127.0.0.1" (:server-name (consul-request :local :get [:agent :checks]))))
    (is (= :http (:scheme (consul-request :local :get [:agent :checks]))))
    (is (= "/v1/kv/key" (:uri (consul-request :local :get [:kv "key"]))))
    (is (= "/v1/kv/foo" (:uri (consul-request :local :get "/v1/kv/foo")))))
  (testing "maps are merged into local-conn"
    (is (= :https (:scheme (consul-request {:scheme :https} :get [:agent :checks]))))
    (is (= 8501 (:server-port (consul-request {:scheme :https :server-name "10.0.37.4" :server-port 8501} :get [:agent :checks]))))
    (is (= :https (:scheme (consul-request {:scheme :https :server-name "10.0.37.4" :server-port 8501} :get [:agent :checks]))))
    (is (= "10.0.37.4" (:server-name (consul-request {:scheme :https :server-name "10.0.37.4" :server-port 8501} :get [:agent :checks]))))))

(deftest ^{:integration true} kv-store-test
  (testing "basic kv operations"
    (let [k (str *ns* "." (UUID/randomUUID))
          v (str (UUID/randomUUID))]
      (is (nil? (kv-get :local k)))
      (is (true? (kv-put :local k v)))
      (is (= v (kv-get :local k :raw? true)))
      (is (= (str->base64 v) (:Value (first (kv-get :local k)))))
      (is (true? (kv-del :local k)))
      (is (nil? (kv-get :local k))))))

(deftest map->check-test
  (let [m {:id       "search"
           :name     "Google"
           :notes    "lies"
           :http     "http://www.google.com"
           :interval "10s"}]
    (is (thrown? AssertionError (map->check {})))
    (is (thrown? AssertionError (map->check (dissoc m :interval))))
    (is (= (:ID (map->check m)) "search"))
    (is (= (:Name (map->check m)) "Google"))
    (is (= (:HTTP (map->check m)) "http://www.google.com"))
    (is (= (:Interval (map->check m)) "10s"))
    (is (= (:ServiceID (map->check (assoc m :service-id "redis-01"))) "redis-01"))))


(deftest ^{:integration true} agent-test
  (testing "registering and removing checks"
    (let [check-id (str *ns* ".check." (UUID/randomUUID))]
      (is (nil? (get-in (agent-checks :local) [check-id])))
      (is (true? (agent-register-check :local {:name "test-check" :interval "10s" :http "http://127.0.0.1:8500/ui/" :id check-id})))
      (is (map? (get-in (agent-checks :local) [check-id])))
      (is (true? (agent-deregister-check :local check-id)))
      (is (nil? (get-in (agent-checks :local) [check-id]))))))

(use-fixtures :each cleanup)
