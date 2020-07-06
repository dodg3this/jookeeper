(ns jookeeper.wal-entry
  (:import (java.nio ByteBuffer)))

(def size-of-long 8)
(def size-of-int 4)

(defrecord WalEntry [^Long entry-id  data ^Integer entry-type ^Long leader-time])

(defn make-wal-entry [m]
  (->WalEntry (:entry-id m) (:data m) (:entry-type m 0) (:leader-time m (System/nanoTime))))

(defn- entry-size [^WalEntry wal-entry]
  (+ (alength (:data wal-entry)) size-of-long size-of-int size-of-long))

(defn serialize [^WalEntry wal-entry]
  (let [entry-size (entry-size wal-entry)
        buffer-size (+ entry-size 4)            ;;4 bytes for record length
        buffer (ByteBuffer/allocate buffer-size)]
    (.clear buffer)
    (.putInt buffer entry-size)
    (.putInt buffer (:entry-type wal-entry))
    (.putLong buffer (:entry-id wal-entry))
    (.putLong buffer (:leader-time wal-entry))
    (.put buffer (:data wal-entry))))
