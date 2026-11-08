-- This is intentionally dry-run only. Prefix listing uses the catalog S3FileIO
-- instead of Hadoop FileSystem, which has no built-in handler for s3://.
-- The table argument limits the scan to this experiment table's location.
CALL ice.system.remove_orphan_files(
    table => 'learning_maintenance.events_binpack',
    dry_run => true,
    prefix_listing => true
);
