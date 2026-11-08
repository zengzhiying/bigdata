-- Metadata JSON versions are table-level commits. The latest metadata file can be newer
-- than the latest data snapshot because ALTER TABLE changed only table properties.
SELECT timestamp, file, latest_snapshot_id, latest_schema_id, latest_sequence_number
FROM ice.learning_metadata.orders.metadata_log_entries
ORDER BY timestamp;

SELECT snapshot_id, parent_id, committed_at, operation,
       manifest_list, summary
FROM ice.learning_metadata.orders.snapshots
ORDER BY committed_at;

SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM ice.learning_metadata.orders.history
ORDER BY made_current_at;

-- Every row points to one manifest file referenced by a current snapshot.
SELECT path, length, partition_spec_id, added_snapshot_id,
       added_data_files_count, existing_data_files_count, deleted_data_files_count,
       added_delete_files_count, existing_delete_files_count, deleted_delete_files_count
FROM ice.learning_metadata.orders.manifests
ORDER BY added_snapshot_id, path;

-- all_entries preserves historical adds and removals; files is only the current live set.
SELECT status, snapshot_id, data_file.content AS content,
       data_file.file_path AS file_path, data_file.record_count AS record_count
FROM ice.learning_metadata.orders.all_entries
ORDER BY snapshot_id, status, file_path;

SELECT content, file_path, record_count, file_size_in_bytes
FROM ice.learning_metadata.orders.files
ORDER BY content, file_path;
