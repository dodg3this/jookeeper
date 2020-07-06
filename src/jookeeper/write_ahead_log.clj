(ns jookeeper.write-ahead-log
  (:require [jookeeper.wal-entry :refer :all])
  (:import (java.io RandomAccessFile)
           (java.nio.channels FileChannel)
           (java.nio ByteBuffer)
           ;(jookeeper.wal_entry WalEntry)
           ))

(def log-id (atom 0))

(defn create [directory]
  (let [file (clojure.java.io/file ^String directory (str "wal-" @log-id ".log"))
        random-access-file (RandomAccessFile. file "rw")]
    (.getChannel random-access-file)))

(def last-log-entry-id (atom 0))
(def entry-offsets (atom {0 0}))

(defn- write-to-channel [^FileChannel channel ^ByteBuffer buffer]
  (.flip buffer)
  (.write channel buffer)
  (.force channel true)
  (.position channel))

(defn write-wal-entry [^FileChannel wal-channel wal-entry]
  (let [buffer (serialize wal-entry)
        file-position (write-to-channel wal-channel buffer)]
    (reset! last-log-entry-id (:entry-id wal-entry))
    (swap! entry-offsets assoc (:entry-id wal-entry) file-position)
    @last-log-entry-id))

(defn write-entry [wal-channel data]
  (swap! last-log-entry-id inc)
  (write-wal-entry wal-channel (make-wal-entry {:entry-id @last-log-entry-id :data data })))

(comment
  (:use 'jookeeper.write-ahead-log)

  (def root-directory (.getCanonicalPath (clojure.java.io/file ".")))

  (create root-directory)

  (.delete (clojure.java.io/file (str root-directory "/wal-0.log")))
  )