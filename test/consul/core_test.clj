(ns consul.core-test
  (:require [clojure.test :refer :all]
            [consul.core :refer :all]))


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







