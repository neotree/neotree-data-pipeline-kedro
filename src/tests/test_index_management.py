"""Safety rules for controlled PostgreSQL index maintenance."""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_pipeline.index_management import _fingerprint, _retention_reason, _signature, ensure_index


def index(**changes):
    row = dict(table_oid=1, access_method='btree', indisunique=False,
               indisexclusion=False, indimmediate=True, indnkeyatts=2,
               indnatts=2, keys='1 2',
               opclasses='1 1', collations='0 0', options='0 0',
               expressions=None, predicate=None, nulls_not_distinct='false',
               reloptions='', reltablespace=0, idx_scan=0, constrained=False,
               referenced=False, indisprimary=False, indisreplident=False,
               indisclustered=False,
               table_kind='r', has_statistics=True)
    row.update(changes)
    return row


class IndexManagementTests(unittest.TestCase):
    def test_ensure_index_escapes_postgres_format_tokens_for_psycopg(self):
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.return_value = ('public.sessions',)
        cursor.fetchall.return_value = []
        cursor.execute.side_effect = lambda sql, params: sql % tuple(map(str, params))
        connection = MagicMock()
        connection.cursor.return_value = cursor

        with patch('data_pipeline.index_management._connection', return_value=connection):
            ensure_index('public', 'sessions', ['pii_cleaned'], 'idx_sessions_pii_cleaned')

        self.assertEqual(cursor.execute.call_count, 2)
        for call in cursor.execute.call_args_list:
            self.assertIn("format('%%I.%%I'", call.args[0])
        connection.close.assert_called_once()

    def test_signature_ignores_name_but_detects_structural_differences(self):
        base = index(index_name='first')
        self.assertEqual(_signature(base), _signature(index(index_name='second')))
        for change in (dict(keys='2 1'), dict(indisunique=True),
                       dict(predicate='{partial}'), dict(expressions='{expr}'),
                       dict(opclasses='2 1'), dict(options='1 0'),
                       dict(reloptions='{fillfactor=80}')):
            self.assertNotEqual(_signature(base), _signature(index(**change)))

    def test_used_and_dependent_indexes_are_retained(self):
        now = datetime.now(timezone.utc)
        reset = now - timedelta(days=90)
        keeper = index()
        for row in (index(idx_scan=1), index(constrained=True),
                    index(referenced=True), index(indisreplident=True),
                    index(indisclustered=True)):
            previous = (_fingerprint(row), now - timedelta(days=60), reset, 0, 3)
            self.assertIsNotNone(_retention_reason(
                row, keeper, previous, reset, now, 30, True))

    def test_uncertain_history_and_missing_survivor_are_retained(self):
        now = datetime.now(timezone.utc)
        reset = now - timedelta(days=90)
        row = index()
        old = now - timedelta(days=60)
        self.assertIsNotNone(_retention_reason(row, None, None, reset, now, 30, True))
        self.assertIsNotNone(_retention_reason(row, index(), None, reset, now, 30, True))
        for previous in ((_fingerprint(row), old, reset, 1, 3),
                         (_fingerprint(row), old, reset, 0, 1),
                         (_fingerprint(row), now, reset, 0, 3)):
            self.assertIsNotNone(_retention_reason(
                row, index(), previous, reset, now, 30, True))
        eligible = (_fingerprint(row), old, reset, 0, 3)
        self.assertIsNotNone(_retention_reason(
            row, index(), eligible, reset, now, 30, False))
        self.assertIsNone(_retention_reason(
            row, index(), eligible, reset, now, 30, True))


if __name__ == '__main__':
    unittest.main()
