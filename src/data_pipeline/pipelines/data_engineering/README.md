# Data Engineering pipeline

## PII Safety

The pipeline contains regex-based PII redaction for Malawi and Zimbabwe phone numbers and national ID values.

Because this logic mutates source JSON in `public.sessions` and `public.clean_sessions`, production rollout must
always follow a canary process before a full run.

### Pre-Prod Checklist

1. Confirm a fresh database backup exists.
2. Deploy code to a staging/snapshot environment first.
3. Run the SQL canary queries against `public.sessions` and `public.clean_sessions`.
4. Review:
   - candidate row count
   - changed row count
   - changed keys
   - sample before/after values
   - suspicious fragment report
5. Only proceed to prod if protected shapes such as timestamps, UUIDs, unique keys, and top-level metadata are unchanged.

### Canary Queries

These helpers are defined in:

- `data_pipeline.pipelines.data_engineering.queries.assorted_queries.pii_canary_summary_query`
- `data_pipeline.pipelines.data_engineering.queries.assorted_queries.pii_redaction_preview_query`
- `data_pipeline.pipelines.data_engineering.queries.assorted_queries.pii_redaction_key_telemetry_query`
- `data_pipeline.pipelines.data_engineering.queries.assorted_queries.pii_suspicious_fragments_query`

Recommended review order:

1. `pii_canary_summary_query(...)`
2. `pii_redaction_key_telemetry_query(...)`
3. `pii_redaction_preview_query(...)`
4. `pii_suspicious_fragments_query(...)`

### Recovery Notes

If a bad deployment corrupts values:

1. Stop the pipeline.
2. Deploy the fixed build before any rerun.
3. Restore `public.sessions` from a clean source.
4. Restore or rebuild `public.clean_sessions`.
5. Rebuild downstream derived tables.

If rows must be reprocessed by a newer redaction ruleset, reset only the versioned tracking field for the affected rows:

```sql
UPDATE public.sessions
SET pii_cleaned = FALSE,
    pii_cleaned_version = 0
WHERE id IN (...);

UPDATE public.clean_sessions
SET pii_cleaned = FALSE,
    pii_cleaned_version = 0
WHERE id IN (...);
```
