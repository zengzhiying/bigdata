-- content: 0=data, 1=position delete, 2=equality delete.
SELECT content, file_path, record_count, file_size_in_bytes
FROM ice.learning_cdc.orders_changelog_upsert.files
ORDER BY content, file_path;

SELECT content, file_path, record_count, equality_ids, referenced_data_file
FROM ice.learning_cdc.orders_changelog_upsert.delete_files
ORDER BY content, file_path;

SELECT status,
       data_file.content AS content,
       data_file.file_path AS file_path,
       snapshot_id
FROM ice.learning_cdc.orders_changelog_upsert.all_entries
WHERE data_file.content <> 0
ORDER BY snapshot_id, data_file.file_path;
