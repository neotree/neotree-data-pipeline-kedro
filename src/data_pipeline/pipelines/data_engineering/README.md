# Data Engineering pipeline

## PII Safety

The pipeline contains regex-based PII redaction for Malawi and Zimbabwe phone numbers and national ID values.

The raw `public.sessions` table is not redacted. It is kept as the recovery source.

PII cleanup is applied only to `public.clean_sessions`, after rows have been copied from `public.sessions`.

### Current Behavior

1. `insert_sessions_data()` populates `public.clean_sessions` from `public.sessions`.
2. known confidential entry keys are removed from `public.clean_sessions`
3. regex-based PII redaction runs only on `public.clean_sessions`
4. if a redacted result looks suspicious, the original `data` payload is restored for that row
5. suspicious rows are skipped and left as:
   - `pii_cleaned = FALSE`
   - `pii_cleaned_version = 0`
6. a summary of skipped suspicious rows is written to the logs

Suspicious output currently means any redacted result containing patterns like:

- `[PII_REMOVED]:`
- `T[PII_REMOVED]`
- `-[PII_REMOVED]-`

### Recovery Notes

If a bad deployment corrupts `public.clean_sessions`:

1. stop the pipeline
2. deploy the fixed build before any rerun
3. rebuild or restore `public.clean_sessions` from `public.sessions`
4. rebuild downstream derived tables as needed

If rows must be reprocessed by a newer redaction ruleset, reset only the versioned tracking field for the affected rows:

```sql
UPDATE public.clean_sessions
SET pii_cleaned = FALSE,
    pii_cleaned_version = 0
WHERE id IN (...);
```
