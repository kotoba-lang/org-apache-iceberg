(ns iceberg.manifest
  "Manifests and manifest lists: the Avro files that say which data files a
  table snapshot consists of.

  A snapshot points at ONE manifest list. The manifest list names manifests.
  Each manifest names data files. The indirection is what lets a commit that
  adds one file reuse every existing manifest by reference instead of
  rewriting the table's file list.

  ## Both files carry metadata the reader needs before the rows

  Avro's container metadata is a map of string to bytes, and Iceberg puts the
  table's schema, the partition spec and the format version in it. A manifest
  whose rows are perfect but whose metadata is missing is not readable as an
  Iceberg manifest -- the reader has nowhere to learn what partition the
  `partition` struct's members mean."
  (:require [avro.file :as avro]
            [iceberg.schema :as s]
            [json.core :as json]))

(def ^:const status-added 1)
(def ^:const content-data 0)

(defn data-file
  "One data file's entry, as the `data_file` struct.

  `:file-path` must be the location a reader will resolve -- the same string
  the metadata's `location` prefixes, not a path relative to anything."
  [{:keys [file-path record-count file-size-bytes file-format]
    :or {file-format "PARQUET"}}]
  {"content" content-data
   "file_path" file-path
   "file_format" file-format
   "partition" {}
   "record_count" record-count
   "file_size_in_bytes" file-size-bytes})

(defn write-manifest
  "The bytes of one manifest file.

  Every entry is written with status `ADDED`: this writer only appends, and
  saying `EXISTING` about a file it did not carry forward would be a claim
  about a previous snapshot it never read."
  [{:keys [schema spec-id spec-fields snapshot-id sequence-number data-files]
    :or {spec-id 0 spec-fields [] sequence-number 1}}]
  (let [entry-schema (s/manifest-entry-schema spec-fields)]
    (avro/write
     {:schema entry-schema
      :codec "deflate"
      :meta {"schema" (json/encode schema)
             "schema-id" (str (get schema "schema-id" 0))
             "partition-spec" (json/encode (vec spec-fields))
             "partition-spec-id" (str spec-id)
             "format-version" (str s/format-version)
             "content" "data"}
      :records (mapv (fn [df]
                       {"status" status-added
                        "snapshot_id" snapshot-id
                        "sequence_number" sequence-number
                        "file_sequence_number" sequence-number
                        "data_file" df})
                     data-files)})))

(defn manifest-file-entry
  "One row of a manifest list, describing a manifest that was just written."
  [{:keys [manifest-path manifest-length spec-id snapshot-id sequence-number
           added-files added-rows]
    :or {spec-id 0 sequence-number 1}}]
  {"manifest_path" manifest-path
   "manifest_length" manifest-length
   "partition_spec_id" spec-id
   "content" content-data
   "sequence_number" sequence-number
   "min_sequence_number" sequence-number
   "added_snapshot_id" snapshot-id
   "added_files_count" added-files
   "existing_files_count" 0
   "deleted_files_count" 0
   "added_rows_count" added-rows
   "existing_rows_count" 0
   "deleted_rows_count" 0
   "partitions" nil
   "key_metadata" nil})

(defn write-manifest-list
  "The bytes of a manifest list."
  [{:keys [snapshot-id parent-snapshot-id sequence-number entries]
    :or {sequence-number 1}}]
  (avro/write
   {:schema s/manifest-file-schema
    :codec "deflate"
    :meta (cond-> {"snapshot-id" (str snapshot-id)
                   "sequence-number" (str sequence-number)
                   "format-version" (str s/format-version)}
            parent-snapshot-id (assoc "parent-snapshot-id" (str parent-snapshot-id)))
    :records (vec entries)}))
