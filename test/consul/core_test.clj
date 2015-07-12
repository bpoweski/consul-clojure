(ns consul.core-test
  (:require [clojure.test :refer :all]
            [consul.core :refer :all]
            [com.stuartsierra.component :as component])
  (:import (java.util UUID)))


(deftest endpoint->path-test
  (testing "Key/Value store"
    (are [path endpoint] (= path (endpoint->path endpoint))
      "/v1/kv/key"              [:kv "key"]
      "/v1/agent/services"      [:agent :services]
      "/v1/agent/join/10.1.1.2" [:agent :join "10.1.1.2"])))

(deftest base64-test
  (is (= "bar" (base64->str "YmFy"))))

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

(deftest kv-map->vec-test
  (are [v k] (is (= v (get (meta (kv-map->vec {:CreateIndex 2, :ModifyIndex 389, :LockIndex 0, :Key "foo", :Flags 1, :Value "YmFy"} false)) k)))
    389 :modify-index
    2 :create-index
    0 :lock-index
    1 :flags)
  (is (= ["foo" "bar"] (kv-map->vec {:CreateIndex 2, :ModifyIndex 389, :LockIndex 0, :Key "foo", :Flags 0, :Value "bar"} false)))
  (is (= ["foo" nil] (kv-map->vec {:CreateIndex 2, :ModifyIndex 389, :LockIndex 0, :Key "foo", :Flags 0, :Value nil} true)))
  (is (= ["foo" "bar"] (kv-map->vec {:CreateIndex 2, :ModifyIndex 389, :LockIndex 0, :Key "foo", :Flags 1, :Value "YmFy"} true))))

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

(deftest ^{:integration true} consul-test
  (testing "with a connection failure"
    (are [v ks] (= v (get-in (ex-data (try (consul {:server-port 8501} :get [:kv "foo"]) (catch Exception err err))) ks))
      8501 [:http-request :server-port]
      :connect-failure [:reason])))

(deftest ^{:integration true} kv-store-test
  (let [k   (str "consul-clojure." (UUID/randomUUID))
        k-2 (str "consul-clojure." (UUID/randomUUID))
        v   (str (UUID/randomUUID))]
    (testing "basic kv-get operations"
      (is [k nil] (kv-get :local k))
      (is (true? (kv-put :local k v)))
      (is (= [k v] (kv-get :local k :raw? true)))
      (is (= [k v] (kv-get :local k :string? true)))
      (is (= [k (str->base64 v)] (kv-get :local k :string? false)))
      (is (true? (kv-del :local k)))
      (is (= [k nil] (kv-get :local k)))
      (is (true? (kv-put :local k v))))
    (testing "kv-keys"
      (is (= #{k} (kv-keys :local "consul-clojure")))
      (is (= #{} (kv-keys :local "x")))
      (kv-put :local k-2 "x")
      (is (= #{k k-2} (kv-keys :local "consul-clojure"))))))

(deftest ^{:integration true} agent-test
  (testing "registering and removing checks"
    (let [check-id (str "consul-clojure.check." (UUID/randomUUID))]
      (is (nil? (get-in (agent-checks :local) [check-id])))
      (is (true? (agent-register-check :local {:name "test-check" :interval "10s" :http "http://127.0.0.1:8500/ui/" :id check-id})))
      (is (map? (get-in (agent-checks :local) [check-id])))
      (is (true? (agent-deregister-check :local check-id)))
      (is (nil? (get-in (agent-checks :local) [check-id])))))
  (testing "registering and deregistering a service"
    (let [service-id (str "consul-clojure.service." (UUID/randomUUID))]
      (is (nil? (get-in (agent-services :local) [service-id])))
      (is (true? (agent-register-service :local {:name service-id})))
      (is (map? (get-in (agent-services :local) [service-id])))
      (is (true? (agent-deregister-service :local service-id)))
      (is (nil? (get-in (agent-services :local) [service-id])))))
  (testing "registering a service with a ttl check"
    (let [service-id (str "consul-clojure.service.ttl." (UUID/randomUUID))]
      (is (true? (agent-register-service :local {:name service-id :check {:ttl "1s"}})))
      (is (map? (get-in (agent-services :local) [service-id])))
      (is (= 1 (count (filter (comp #{service-id} :ServiceID) (vals (agent-checks :local))))))))
  (testing "registering a service with two ttl checks"
    (let [service-id (str "consul-clojure.service.ttl." (UUID/randomUUID))]
      (is (true? (agent-register-service :local {:name service-id :checks [{:ttl "1s"} {:ttl "5s"}]})))
      (is (map? (get-in (agent-services :local) [service-id])))
      (is (= 2 (count (filter (comp #{service-id} :ServiceID) (vals (agent-checks :local)))))))))

(defn clean []
  (kv-del :local "consul-clojure" :recurse? true)
  (doseq [service-id (filter #(re-seq #"^consul-clojure.*" %) (keys (agent-services :local)))]
    (agent-deregister-service :local service-id)))

(defn cleanup [f]
  (clean)
  (f)
  (clean))

(use-fixtures :each cleanup)
