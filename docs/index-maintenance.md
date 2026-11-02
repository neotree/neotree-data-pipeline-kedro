# PostgreSQL index maintenance

The pipeline checks for a valid equivalent index before creating each simple
btree index it requests. Index names are deterministic. If the requested name
is occupied by a different object, the run fails with a reviewable error
instead of creating a numbered index.

Duplicate index cleanup is a separate maintenance operation. Run it after a
successful pipeline execution:

```bash
PYTHONPATH=src:. python -m data_pipeline.index_management --schema derived
```

This is a dry run. It records each index's OID, definition fingerprint,
`pg_stat_user_indexes.idx_scan`, database statistics reset time, first and
latest observation, and table index count and size. Run it periodically over
at least 30 days. Index growth between observations is logged as a warning.
Review the log for duplicate groups and retained indexes.

To permit deletion after the observation interval, use both flags:

```bash
PYTHONPATH=src:. python -m data_pipeline.index_management --schema derived \
  --delete --verified-statistics
```

`--verified-statistics` is an operator attestation that no individual index
statistics counters were reset during the observation interval. PostgreSQL's
database-level `stats_reset` does not reveal individual index counter resets.
If that history cannot be verified, continue using the dry run and review
indexes manually. The command also retains every index with recorded scans,
every constraint or dependency index, replica identity indexes, partitioned
indexes, and any index without a valid equivalent survivor. Drops use
`DROP INDEX CONCURRENTLY ... RESTRICT` and each affected table is analyzed.

Run this only in a maintenance window with appropriate database privileges.
Cleanup errors are logged and do not affect normal pipeline execution.
