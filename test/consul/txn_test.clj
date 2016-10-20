(ns consul.txn-test
  (:require [consul.txn :as txn]
            [consul.core :as core]
            [clojure.test :refer :all])
  (:import (java.util UUID)))

(defn clean []
  (core/kv-del :local "consul-clojure" {:recurse? true})
  (doseq [service-id (filter #(re-seq #"^consul-clojure.*" %) (keys (core/agent-services :local)))]
    (core/agent-deregister-service :local service-id)))

(defn cleanup [f]
  (clean)
  (f)
  (clean))

(use-fixtures :each cleanup)

(defn create-key [key] (str "consul-clojure." key))

(deftest ^{:integration true} kv-get-set-test
  (testing "basic get/set operations"
    (let [results    (txn/put :local (fn [tx]
                                       (txn/kv-set tx (create-key "key") "value")
                                       (txn/kv-get tx (create-key "key"))))
          set-result (first results)
          get-result (last results)]
      (is (= 2 (count results)))
      (is (= (:create-index set-result) (:create-index get-result)))
      (is (= (:flags set-result) (:flags get-result)))
      (is (= "consul-clojure.key" (:key set-result) (:key get-result)))
      (is (= (:lock-index set-result) (:lock-index get-result)))
      (is (= (:modify-index set-result) (:modify-index get-result)))
      (is (nil? (:value set-result)))
      (is (= "value" (:value get-result))))

    (let [result (first (txn/put :local (fn [tx] (txn/kv-set tx (create-key "key") "value" :flags 10))))]
      (is (= 10 (:flags result))))))

(deftest ^{:integration true} kv-cas-test
  (testing "check-and-set semantics with the correct index"
    (let [index  (-> (txn/put :local (fn [tx] (txn/kv-set tx (create-key "key") "value"))) first :modify-index)
          result (-> (txn/put :local (fn [tx] (txn/kv-set-cas tx (create-key "key") "value" index))) first)]
      (is (integer? (:lock-index result)))
      (is (= "consul-clojure.key" (:key result)))
      (is (= 0 (:flags result)))
      (is (nil? (:value result)))
      (is (integer? (:create-index result)))
      (is (integer? (:modify-index result)))
      (is (not (= index (:modify-index result))))
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (txn/put :local (fn [tx] (txn/kv-set-cas tx (create-key "key") "value" (- index 1)))))))))

(deftest ^{:integration true} kv-lock-unlock-test
  (testing "locking and unlocking with a session"
    (let [session-key   (core/session-create :local)
          results       (txn/put :local (fn [tx]
                                          (txn/kv-lock tx (create-key "key") "value" session-key)
                                          (txn/kv-unlock tx (create-key "key") "value" session-key)))
          lock-result   (first results)
          unlock-result (last results)]
      (is (= (:lock-index lock-result) (:lock-index unlock-result)))
      (is (= "consul-clojure.key" (:key lock-result) (:key unlock-result)))
      (is (= (:flags lock-result) (:flags unlock-result)))
      (is (= nil (:value lock-result) (:value unlock-result)))
      (is (= (:create-index lock-result) (:create-index unlock-result)))
      (is (= (:modify-index lock-result) (:modify-index unlock-result)))
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (txn/put :local (fn [tx] (txn/kv-lock tx (create-key "key") "value" "not-uuid")))))
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (txn/put :local (fn [tx] (txn/kv-lock tx (create-key "key") "value" (str (UUID/randomUUID))))))))))

(deftest ^{:integration true} kv-get-tree-test
  (testing "getting all items with a prefix"
    (do (core/kv-put :local (create-key "prefix/key1") "value")
        (core/kv-put :local (create-key "prefix/key2") "value")
        (core/kv-put :local (create-key "other/key3") "value"))
    (let [results (txn/put :local (fn [tx] (txn/kv-get-tree tx (create-key "prefix/"))))]
      (is (= 2 (count results)))
      (is (= "consul-clojure.prefix/key1" (:key (first results))))
      (is (= "consul-clojure.prefix/key2" (:key (last results)))))))

(deftest ^{:integration true} kv-check-index-test
  (testing "checking the index"
    (let [index (:modify-index (first (txn/put :local (fn [tx] (txn/kv-set tx (create-key "key") "val")))))
          result (first (txn/put :local (fn [tx] (txn/kv-check-index tx (create-key "key") index))))]
      (is (= index (:modify-index result)))
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (txn/put :local (fn [tx] (txn/kv-check-index tx (create-key "key") (+ 1 index)))))))))

(deftest ^{:integration true} kv-check-session-test
  (testing "checking if the key is locked by a session"
    (let [session-key (core/session-create :local)
          results (txn/put :local (fn [tx]
                                    (txn/kv-lock tx (create-key "key") "value" session-key)
                                    (txn/kv-check-session tx (create-key "key") session-key)))]
      (is (= 2 (count results)))
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (txn/put :local (fn [tx] (txn/kv-check-session tx (create-key "key") (str (UUID/randomUUID))))))))))

(deftest ^{:integration true} kv-delete-test
  (testing "deletion of a key"
    (let [key (create-key "key")]
      (core/kv-put :local key "val")
      (let [del-result (txn/put :local (fn [tx] (txn/kv-delete tx key)))]
        (is (= 0 (count del-result)))
        (is (nil? (:value (core/kv-get :local key))))))))

(deftest ^{:integration true} kv-delete-tree-test
  (testing "deletion of multiple keys in a tree"
    (core/kv-put :local (create-key "prefix/key1") "val")
    (core/kv-put :local (create-key "prefix/key2") "val")
    (core/kv-put :local (create-key "other/key3") "val")
    (let [del-result (txn/put :local (fn [tx] (txn/kv-delete-tree tx (create-key "prefix/"))))]
      (is (= 0 (count del-result)))
      (is (nil? (:value (core/kv-get :local (create-key "prefix/key1")))))
      (is (nil? (:value (core/kv-get :local (create-key "prefix/key2")))))
      (is (= "val" (:value (core/kv-get :local (create-key "other/key3"))))))))

(deftest ^{:integration true} kv-delete-cas-test
  (testing "deletion of a key with a specified index"
    (let [index  (-> (txn/put :local (fn [tx] (txn/kv-set tx (create-key "key") "value"))) first :modify-index)
          result (-> (txn/put :local (fn [tx] (txn/kv-delete-cas tx (create-key "key") index))) first)]
      (is (= 0 (count result)))
      (is (nil? (:value (core/kv-get :local (create-key "key"))))))
    (let [index  (-> (txn/put :local (fn [tx] (txn/kv-set tx (create-key "key") "value"))) first :modify-index)]
      (is (thrown-with-msg? Exception #"Transaction failure"
                            (-> (txn/put :local (fn [tx] (txn/kv-delete-cas tx (create-key "key") (+ 1 index)))) first))))))

