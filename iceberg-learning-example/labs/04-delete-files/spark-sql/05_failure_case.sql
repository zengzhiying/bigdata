-- 预期失败：Delete mode 只能使用 copy-on-write 或 merge-on-read。
-- 失败后确认当前数据和 Delete File 元数据没有变化。
ALTER TABLE ice.learning_delete_files.events
SET TBLPROPERTIES ('write.delete.mode' = 'unsupported-mode');
