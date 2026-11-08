SELECT COUNT(*) AS manifests_before_rewrite
FROM ice.learning_maintenance.events_binpack.manifests;

CALL ice.system.rewrite_manifests(
    table => 'learning_maintenance.events_binpack',
    use_caching => true
);

SELECT COUNT(*) AS manifests_after_rewrite
FROM ice.learning_maintenance.events_binpack.manifests;

SELECT snapshot_id, committed_at, operation, manifest_list, summary
FROM ice.learning_maintenance.events_binpack.snapshots
ORDER BY committed_at;

