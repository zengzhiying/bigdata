-- 当前 Snapshot 可见的文件。content: 0=data, 1=position delete, 2=equality delete。
-- 多次删除之后删除文件并不是多个增量文件方式出现的，而是会以全新的文件形式合并进去之前所有的删除，之前的文件只是保留快照查询使用
SELECT content, file_path, file_size_in_bytes, record_count
FROM ice.learning_delete_files.events.files
ORDER BY content, file_path;

SELECT content, file_path, file_size_in_bytes, record_count,
       equality_ids, referenced_data_file
FROM ice.learning_delete_files.events.delete_files
ORDER BY content, file_path;

-- all_delete_files 返回历史 Snapshot 涉及的 Delete File 文件属性。
-- 它是文件视图，字段已展开，但不包含 snapshot_id。
SELECT content, file_path, record_count
FROM ice.learning_delete_files.events.all_delete_files
ORDER BY content, file_path;

-- entries 只包含当前 Snapshot 的 manifest entry。
-- snapshot_id 位于 entry 顶层；文件属性位于 data_file 结构体中。
SELECT status,
       data_file.content AS content,
       data_file.file_path AS file_path,
       snapshot_id
FROM ice.learning_delete_files.events.entries
WHERE data_file.content <> 0
ORDER BY snapshot_id, data_file.file_path;

-- all_entries 包含所有历史 Snapshot 的 manifest entry，
-- 可用于观察 Delete File 与其关联 Snapshot 的演进。
SELECT status,
       data_file.content AS content,
       data_file.file_path AS file_path,
       data_file.record_count AS record_count,
       snapshot_id
FROM ice.learning_delete_files.events.all_entries
WHERE data_file.content <> 0
ORDER BY snapshot_id, data_file.file_path;
