(ns jookeeper.write-ahead-log-test
  (:require [clojure.test :refer :all])
  (:require [jookeeper.write-ahead-log :refer :all]))

(deftest wal-test
      (testing "should create a file"
        (let [root-directory (.getCanonicalPath (clojure.java.io/file "."))]
          (create root-directory)
          (is = (slurp (str root-directory "/wal-0.log")))
          (.delete (clojure.java.io/file ^String root-directory "wal-0.log"))))

      (testing "should write wal entry in to the log file"
        (let [root-directory (.getCanonicalPath (clojure.java.io/file "."))
              wal-channel (create root-directory)]
          (write-entry wal-channel (.getBytes "some-test-data"))
          (.delete (clojure.java.io/file ^String root-directory "wal-0.log")))))
