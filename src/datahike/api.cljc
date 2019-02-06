(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core :as hc]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]
            [clojure.core.cache :as cache])
  (:import [java.net URI]))

(def memory (atom {}))

(defn parse-uri [uri]
  (let [base-uri (URI. uri)
        m (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        proto (.getScheme sub-uri)
        path (.getPath sub-uri)]
    [m proto path]))

(defn connect [uri]
  (let [[m proto path] (parse-uri uri) #_(re-find #"datahike:(.+)://(/.+)" uri)
        _ (when-not m
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (case proto
                  "mem"   (@memory uri)
                  "file"  (<?? S (fs/new-fs-store path))
                  "level" (<?? S (kl/new-leveldb-store path)))
                (atom (cache/lru-cache-factory {} :threshold 1000))))]
    {:uri uri
     :proto proto
     :store store}))

(defn get-db
  ([connection]
   (get-db connection nil))
  ([connection tx-id]
   (let [{:keys [uri proto store]} connection
         stored-db (<?? S (k/get-in store (if tx-id
                                            [:db-tx tx-id]
                                            [:db])))
         _ (when-not stored-db
             (case proto
               "level" (kl/release store)
               nil)
             (throw (ex-info "DB does not exist." {:type :db-does-not-exist
                                                   :uri uri})))
         {:keys [eavt-key aevt-key avet-key schema rschema max-tx]} stored-db
         empty (db/empty-db)
         max-tx (or max-tx (:max-tx empty))
         eavt-durable eavt-key]
     (d/conn-from-db
      (assoc empty
             :schema schema
             :max-eid (db/init-max-eid (:eavt empty) eavt-durable)
             :max-tx max-tx
             :eavt-durable eavt-durable
             :aevt-durable aevt-key
             :avet-durable avet-key
             :rschema rschema
             :store store
             :uri uri)))))

(defn create-database-with-schema [uri schema]
  (let [[m proto path] (parse-uri uri)
        _ (when-not m
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
        store (kc/ensure-cache
               (case proto
                 "mem"   (let [store (<?? S (mem/new-mem-store))]
                           (swap! memory assoc uri store)
                           store)
                 "file"  (kons/add-hitchhiker-tree-handlers
                          (<?? S (fs/new-fs-store path)))
                 "level" (kons/add-hitchhiker-tree-handlers
                          (<?? S (kl/new-leveldb-store path))))
               (atom (cache/lru-cache-factory {} :threshold 1000)))
        stored-db (<?? S (k/get-in store [:db]))
        _ (when stored-db
            (throw (ex-info "DB already exist." {:type :db-already-exists
                                                 :uri uri})))
        {:keys [eavt-durable aevt-durable avet-durable rschema] :as new-db} (db/empty-db schema)
        backend (kons/->KonserveBackend store)]
    (<?? S (k/assoc-in store [:db]
                        {:schema schema
                         :eavt-key (:tree (hc/<?? (hc/flush-tree-without-root eavt-durable backend)))
                         :aevt-key (:tree (hc/<?? (hc/flush-tree-without-root aevt-durable backend)))
                         :avet-key (:tree (hc/<?? (hc/flush-tree-without-root avet-durable backend)))
                         :rschema rschema}))
    (case proto
      "level"
      (kl/release store)
      nil)
    nil))

(defn create-database [uri]
  (create-database-with-schema uri nil))

(defn delete-database [uri]
  (let [[m proto path] (parse-uri uri)]
    (case proto
      "mem"   (swap! memory dissoc uri)
      "file"  (fs/delete-store path)
      "level" (kl/delete-store path))))

(defn transact [db tx-data]
  {:pre [(d/conn? db)]}
  (future
    (locking db
      (let [{:keys [db-after tempids] :as tx-report} @(d/transact db tx-data)
            {:keys [db/current-tx]} tempids
            {:keys [eavt-durable aevt-durable avet-durable schema rschema]} db-after
            store (:store @db)
            backend (kons/->KonserveBackend store)
            eavt-flushed (:tree (hc/<?? (hc/flush-tree-without-root eavt-durable backend)))
            aevt-flushed (:tree (hc/<?? (hc/flush-tree-without-root aevt-durable backend)))
            avet-flushed (:tree (hc/<?? (hc/flush-tree-without-root avet-durable backend)))
            new-stored-db {:schema schema
                           :rschema rschema
                           :eavt-key eavt-flushed
                           :aevt-key aevt-flushed
                           :avet-key avet-flushed
                           :max-tx current-tx}]
        (<?? S (k/assoc-in store [:db] new-stored-db))
        (<?? S (k/assoc-in store [:db-tx current-tx] new-stored-db))
        (reset! db (assoc db-after
                          :eavt-durable eavt-flushed
                          :aevt-durable aevt-flushed
                          :avet-durable avet-flushed))
        tx-report))))


(defn release [connection]
  (let [[m proto path] (re-find #"datahike:(.+)://(/.+)" (:uri connection))]
    (case proto
      "mem"   nil
      "file"  nil
      "level" (kl/release (:store connection)))))


(def pull d/pull)

(def pull-many d/pull-many)

(def q d/q)

(def seek-datoms d/seek-datoms)

(def tempid d/tempid)

(def entity d/entity)

(def entity-db d/entity-db)

(def filter d/filter)

(defn db [db]
  @db)

(def with d/with)
