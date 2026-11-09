"""PostgreSQL index reuse and controlled duplicate-index maintenance."""

import argparse
import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone


LOG = logging.getLogger(__name__)
OBSERVATIONS = '"derived"."pipeline_index_observations"'


def quote(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


INDEX_QUERY = """
SELECT n.nspname AS schema_name, t.relname AS table_name,
       ic.relname AS index_name, i.indexrelid AS index_oid,
       i.indrelid AS table_oid, am.amname AS access_method,
       i.indisunique, i.indisexclusion, i.indisprimary, i.indimmediate,
       i.indisclustered, i.indisvalid, i.indisready, i.indislive,
       i.indisreplident,
       i.indnkeyatts, i.indnatts, i.indkey::text AS keys,
       i.indclass::text AS opclasses, i.indcollation::text AS collations,
       i.indoption::text AS options, i.indexprs::text AS expressions,
       i.indpred::text AS predicate,
       COALESCE(to_jsonb(i)->>'indnullsnotdistinct', 'false') AS nulls_not_distinct,
       COALESCE((SELECT array_agg(x ORDER BY x)::text FROM unnest(ic.reloptions) x), '') AS reloptions,
       ic.reltablespace, COALESCE(s.idx_scan, 0) AS idx_scan,
       s.indexrelid IS NOT NULL AS has_statistics,
       pg_relation_size(ic.oid) AS bytes,
       EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid) AS constrained,
       EXISTS (SELECT 1 FROM pg_depend d
               WHERE d.refclassid = 'pg_class'::regclass
                 AND d.refobjid = i.indexrelid) AS referenced,
       t.relkind AS table_kind,
       ARRAY(SELECT pg_get_indexdef(i.indexrelid, k, true)
             FROM generate_series(1, i.indnatts) k ORDER BY k) AS key_definitions
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_am am ON am.oid = ic.relam
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
"""


def _rows(cursor, schema=None, table=None):
    cursor.execute(INDEX_QUERY + " AND (%s IS NULL OR n.nspname = %s)"
                   + " AND (%s IS NULL OR t.relname = %s)",
                   (schema, schema, table, table))
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _signature(row):
    fields = ('table_oid', 'access_method', 'indisunique', 'indisexclusion',
              'indimmediate',
              'indnkeyatts', 'indnatts', 'keys', 'opclasses', 'collations',
              'options', 'expressions', 'predicate', 'nulls_not_distinct',
              'reloptions', 'reltablespace')
    return tuple(str(row[field]) for field in fields)


def _fingerprint(row):
    return hashlib.sha256(json.dumps(_signature(row)).encode()).hexdigest()


def _retention_reason(row, keeper, previous, stats_reset, now,
                      minimum_days, verified_statistics):
    if row is keeper:
        return 'retained as valid equivalent'
    if row['idx_scan'] > 0:
        return 'recorded usage'
    if (row['constrained'] or row['referenced'] or row['indisprimary']
        or row['indisreplident'] or row['indisclustered']):
        return 'constraint, cluster setting, replica identity, or dependency'
    if row['table_kind'] == 'p':
        return 'partitioned table index'
    if not keeper:
        return 'no valid equivalent'
    if (not row['has_statistics'] or stats_reset is None or previous is None
        or previous[0] != _fingerprint(row) or previous[2] != stats_reset
        or previous[3] != 0 or previous[4] < 2
        or (now - previous[1]).days < minimum_days):
        return 'usage history uncertain or observation period incomplete'
    if not verified_statistics:
        return 'individual statistics resets not verified'
    return None


def _connection():
    from conf.common.sql_functions import engine
    if engine is None:
        raise RuntimeError('Database engine not initialized')
    return engine.raw_connection()


def ensure_index(schema, table, columns, index_name):
    """Reuse a valid equivalent plain btree index, or create a named one."""
    if not columns:
        raise ValueError('An index needs at least one column')
    connection = _connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass(format('%%I.%%I', %s, %s))", (schema, table))
            if cursor.fetchone()[0] is None:
                LOG.warning('Skipping index %s.%s: table is absent', schema, index_name)
                return
            cursor.execute("""SELECT attname FROM pg_attribute
                WHERE attrelid = to_regclass(format('%%I.%%I', %s, %s))
                  AND attname = ANY(%s) AND attnum > 0 AND NOT attisdropped""",
                (schema, table, columns))
            found = {row[0] for row in cursor.fetchall()}
            if found != set(columns):
                LOG.warning('Skipping index %s.%s: missing columns %s',
                            schema, index_name, set(columns) - found)
                return
            cursor.execute('SELECT format(\'%%I\', name) FROM unnest(%s::text[]) name',
                           (columns,))
            expected = [record[0] for record in cursor.fetchall()]
            for row in _rows(cursor, schema, table):
                if (row['indisvalid'] and row['indisready'] and row['indislive']
                    and row['access_method'] == 'btree' and not row['indisunique']
                    and not row['indisexclusion'] and row['indnkeyatts'] == len(columns)
                    and row['indnatts'] == len(columns) and row['expressions'] is None
                    and row['predicate'] is None and row['key_definitions'] == expected
                    and row['reloptions'] == '' and row['reltablespace'] == 0):
                    LOG.info('Reusing index %s.%s on %s.%s', schema,
                             row['index_name'], schema, table)
                    return
            cursor.execute("""SELECT t.relname FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s""", (schema, index_name))
            if cursor.fetchone():
                raise RuntimeError('Index name %s.%s is occupied by a different object; '
                                   'manual review required' % (schema, index_name))
            ddl = 'CREATE INDEX %s ON %s.%s (%s)' % (
                quote(index_name), quote(schema), quote(table),
                ', '.join(quote(column) for column in columns))
            cursor.execute(ddl)
            connection.commit()
            LOG.info('Created index %s.%s on %s.%s', schema, index_name, schema, table)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _observations(cursor):
    cursor.execute('CREATE SCHEMA IF NOT EXISTS derived')
    cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        index_oid oid PRIMARY KEY, fingerprint text NOT NULL,
        first_seen timestamptz NOT NULL, stats_reset timestamptz NOT NULL,
        first_scan bigint NOT NULL, samples integer NOT NULL,
        last_seen timestamptz NOT NULL)''' % OBSERVATIONS)
    cursor.execute('''CREATE TABLE IF NOT EXISTS "derived"."pipeline_index_inventory" (
        table_oid oid PRIMARY KEY, schema_name text NOT NULL, table_name text NOT NULL,
        index_count integer NOT NULL, index_bytes bigint NOT NULL,
        observed_at timestamptz NOT NULL)''')


def maintain_indexes(schema=None, dry_run=True, verified_statistics=False,
                     minimum_days=30):
    """Record usage and optionally remove verified, redundant indexes.

    verified_statistics is operator attestation that index counters were not
    reset individually during the entire observation interval.
    """
    if minimum_days < 30:
        raise ValueError('minimum_days must be at least 30')
    connection = _connection()
    removed = []
    try:
        with connection.cursor() as cursor:
            _observations(cursor)
            cursor.execute('SELECT stats_reset FROM pg_stat_database WHERE datid = '
                           '(SELECT oid FROM pg_database WHERE datname = current_database())')
            stats_reset = cursor.fetchone()[0]
            rows = _rows(cursor, schema)
            cursor.execute('SELECT index_oid, fingerprint, first_seen, stats_reset, '
                           'first_scan, samples FROM ' + OBSERVATIONS)
            observations = {int(r[0]): r[1:] for r in cursor.fetchall()}
            now = datetime.now(timezone.utc)
            groups = defaultdict(list)
            inventory = defaultdict(list)
            for row in rows:
                groups[_signature(row)].append(row)
                inventory[int(row['table_oid'])].append(row)
                oid = int(row['index_oid'])
                fingerprint = _fingerprint(row)
                previous = observations.get(oid)
                if stats_reset is not None and row['has_statistics']:
                    if (previous is None or previous[0] != fingerprint
                        or previous[2] != stats_reset
                        or row['idx_scan'] < previous[3]):
                        cursor.execute('''INSERT INTO %s
                            (index_oid, fingerprint, first_seen, stats_reset,
                             first_scan, samples, last_seen)
                            VALUES (%%s, %%s, %%s, %%s, %%s, 1, %%s)
                            ON CONFLICT (index_oid) DO UPDATE SET
                            fingerprint=EXCLUDED.fingerprint,
                            first_seen=EXCLUDED.first_seen,
                            stats_reset=EXCLUDED.stats_reset,
                            first_scan=EXCLUDED.first_scan,
                            samples=1, last_seen=EXCLUDED.last_seen''' % OBSERVATIONS,
                            (oid, fingerprint, now, stats_reset, row['idx_scan'], now))
                    else:
                        cursor.execute('''UPDATE %s SET samples=samples+1,
                            last_seen=%%s WHERE index_oid=%%s''' % OBSERVATIONS,
                            (now, oid))
            for table_oid, indexes in inventory.items():
                count = len(indexes)
                size = sum(row['bytes'] for row in indexes)
                cursor.execute('''SELECT index_count, index_bytes FROM
                    "derived"."pipeline_index_inventory" WHERE table_oid=%s''',
                    (table_oid,))
                previous_inventory = cursor.fetchone()
                if previous_inventory and (count > previous_inventory[0]
                                           or size > previous_inventory[1] * 1.25):
                    LOG.warning('Index growth on %s.%s: count %s -> %s, bytes %s -> %s',
                                indexes[0]['schema_name'], indexes[0]['table_name'],
                                previous_inventory[0], count, previous_inventory[1], size)
                LOG.info('Index inventory %s.%s: %s indexes, %s bytes',
                         indexes[0]['schema_name'], indexes[0]['table_name'], count, size)
                cursor.execute('''INSERT INTO "derived"."pipeline_index_inventory"
                    (table_oid, schema_name, table_name, index_count, index_bytes, observed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (table_oid) DO UPDATE SET
                    schema_name=EXCLUDED.schema_name, table_name=EXCLUDED.table_name,
                    index_count=EXCLUDED.index_count, index_bytes=EXCLUDED.index_bytes,
                    observed_at=EXCLUDED.observed_at''',
                    (table_oid, indexes[0]['schema_name'], indexes[0]['table_name'],
                     count, size, now))
            connection.commit()
            for group in groups.values():
                if len(group) < 2:
                    continue
                group.sort(key=lambda r: (not r['constrained'],
                                           not r['indisreplident'],
                                           -r['idx_scan'], r['index_name']))
                LOG.info('Equivalent indexes on %s.%s: %s', group[0]['schema_name'],
                         group[0]['table_name'],
                         [(r['index_name'], r['idx_scan'], r['bytes']) for r in group])
                valid = [r for r in group if r['indisvalid'] and r['indisready'] and r['indislive']]
                keeper = valid[0] if valid else None
                for row in group:
                    name = '%s.%s' % (row['schema_name'], row['index_name'])
                    previous = observations.get(int(row['index_oid']))
                    reason = _retention_reason(row, keeper, previous,
                                               stats_reset, now, minimum_days,
                                               verified_statistics)
                    if reason:
                        LOG.info('Retained %s: %s', name, reason)
                        continue
                    LOG.info('Eligible redundant index %s (%s bytes)', name, row['bytes'])
                    if dry_run:
                        LOG.info('Dry run: retained %s', name)
                        continue
                    try:
                        # Concurrent DROP requires autocommit; recheck immediately before it.
                        with connection.cursor() as check:
                            fresh = {int(r['index_oid']): r for r in _rows(
                                check, row['schema_name'], row['table_name'])}
                        target = fresh.get(int(row['index_oid']))
                        survivor = fresh.get(int(keeper['index_oid']))
                        if (not target or target['idx_scan'] != 0
                            or _signature(target) != _signature(row)
                            or target['constrained'] or target['referenced']
                            or target['indisreplident'] or target['indisclustered']
                            or not survivor
                            or not survivor['indisvalid'] or not survivor['indisready']
                            or _signature(survivor) != _signature(row)):
                            LOG.warning('Skipped %s: state changed before drop', name)
                            continue
                        connection.commit()
                        connection.autocommit = True
                        with connection.cursor() as drop:
                            drop.execute('DROP INDEX CONCURRENTLY %s.%s RESTRICT' %
                                         (quote(row['schema_name']), quote(row['index_name'])))
                        removed.append(row)
                        LOG.info('Removed redundant index %s', name)
                    except Exception:
                        LOG.exception('Could not remove %s; data processing can continue', name)
                        connection.rollback()
                    finally:
                        connection.autocommit = False
            for schema_name, table_name in sorted({(r['schema_name'], r['table_name'])
                                                    for r in removed}):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute('ANALYZE %s.%s' % (quote(schema_name), quote(table_name)))
                    connection.commit()
                    LOG.info('Refreshed statistics for %s.%s', schema_name, table_name)
                except Exception:
                    connection.rollback()
                    LOG.exception('Could not refresh statistics for %s.%s', schema_name, table_name)
    finally:
        connection.close()
    return [r['index_name'] for r in removed]


def main():
    parser = argparse.ArgumentParser(description='Controlled duplicate-index maintenance')
    parser.add_argument('--schema', default=None)
    parser.add_argument('--delete', action='store_true', help='Enable actual deletion')
    parser.add_argument('--verified-statistics', action='store_true',
                        help='Attest no individual index statistics resets in observation period')
    parser.add_argument('--minimum-days', type=int, default=30)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    maintain_indexes(args.schema, dry_run=not args.delete,
                     verified_statistics=args.verified_statistics,
                     minimum_days=args.minimum_days)


if __name__ == '__main__':
    main()
