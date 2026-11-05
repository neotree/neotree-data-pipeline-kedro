"""Offline behavior checks for independent table processing."""

import ast
import hashlib
import logging
import re
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[4]
PIPELINE = ROOT / 'src/data_pipeline/pipelines/data_engineering'


def load_function(path, name, namespace):
    """Load one function without importing the module's live DB catalog."""
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name]


class FailureIsolationTests(unittest.TestCase):
    def test_schema_error_is_not_retried_for_every_row(self):
        class MissingColumn(Exception):
            pgcode = '42703'

        inject = Mock(side_effect=MissingColumn())
        namespace = {
            'inject_sql': inject, 'hashlib': hashlib, 'logging': logging,
            '_redact_sql_for_log': str,
        }
        write = load_function(ROOT / 'conf/common/sql_functions.py',
                              'execute_rows_with_fallback', namespace)
        with self.assertRaises(MissingColumn):
            write(['one', 'two', 'three'], lambda rows: ','.join(rows), 'test')
        inject.assert_called_once()

    def test_bulk_update_commits_good_rows_and_reports_bad_row(self):
        committed = []

        def inject(query, _):
            if "'BAD'" in query:
                raise ValueError('invalid value')
            committed.append(query)

        namespace = {
            'pd': pd, 'np': np, 'datetime': datetime, 'logging': logging,
            'validate_sql_dataframe_columns': lambda *_: None,
            'inject_sql_with_return': lambda *_: [
                ('uid', 'text'), ('facility', 'text'), ('unique_key', 'text'), ('note', 'text')],
            'column_exists': lambda *_: False,
            'escape_special_characters': str,
            'assert_sql_value_alignment': lambda *_: None,
            'formatError': str,
            'inject_sql': inject, 'hashlib': hashlib, '_redact_sql_for_log': str,
        }
        load_function(ROOT / 'conf/common/sql_functions.py',
                      'execute_rows_with_fallback', namespace)
        update = load_function(ROOT / 'conf/common/sql_functions.py',
                               'generateAndRunUpdateQuery', namespace)
        df = pd.DataFrame([
            {'uid': 'one', 'facility': 'f', 'unique_key': 'k1', 'note': 'OK'},
            {'uid': 'two', 'facility': 'f', 'unique_key': 'k2', 'note': 'BAD'},
            {'uid': 'three', 'facility': 'f', 'unique_key': 'k3', 'note': 'OK'},
        ])
        with self.assertRaisesRegex(RuntimeError, '1 row'):
            update('derived.example', df)
        self.assertTrue(any("'one'" in query for query in committed))
        self.assertTrue(any("'three'" in query for query in committed))

    def test_bulk_insert_commits_good_rows_and_reports_bad_row(self):
        committed = []

        def inject(query, _):
            if "'BAD'" in query:
                raise ValueError('invalid value')
            committed.append(query)

        namespace = {
            'logging': logging, 'hashlib': hashlib,
            'transform_dataframe_with_field_info': lambda df, _: df,
            'drop_all_null_dataframe_columns': lambda df, _: (df, []),
            'validate_sql_dataframe_columns': lambda *_: None,
            'get_table_column_names': lambda *_: [('uid',), ('note',)],
            'get_table_columns': lambda *_: [('uid', 'text'), ('note', 'text')],
            'format_insert_value_for_column': lambda value, *_: "'" + str(value) + "'",
            'assert_sql_value_alignment': lambda *_: None,
            'inject_sql': inject, '_redact_sql_for_log': str,
        }
        load_function(ROOT / 'conf/common/sql_functions.py',
                      'execute_rows_with_fallback', namespace)
        insert = load_function(ROOT / 'conf/common/sql_functions.py',
                               'generate_postgres_insert', namespace)
        df = pd.DataFrame([
            {'uid': 'one', 'note': 'OK'},
            {'uid': 'two', 'note': 'BAD'},
            {'uid': 'three', 'note': 'OK'},
        ])
        self.assertEqual(insert(df, 'derived', 'example'), 1)
        self.assertTrue(any("'one'" in query for query in committed))
        self.assertTrue(any("'three'" in query for query in committed))

    def test_failed_row_is_skipped_while_other_rows_commit(self):
        committed = []

        def inject(query, _):
            if 'BAD' in query:
                raise ValueError('invalid input')
            committed.extend(query.split(','))

        namespace = {
            'inject_sql': inject, 'hashlib': hashlib, 'logging': logging,
            '_redact_sql_for_log': str,
        }
        write = load_function(ROOT / 'conf/common/sql_functions.py',
                              'execute_rows_with_fallback', namespace)
        written, failed = write(['one', 'BAD', 'three', 'four'],
                                lambda rows: ','.join(rows), 'test insert',
                                ['id1', 'id2', 'id3', 'id4'])
        self.assertEqual((written, failed), (3, 1))
        self.assertEqual(committed, ['one', 'three', 'four'])

    def test_failed_sql_rolls_back_pooled_connection(self):
        cursor = Mock()
        cursor.execute.side_effect = ValueError('invalid column')
        connection = Mock()
        connection.cursor.return_value = cursor
        engine = Mock()
        engine.raw_connection.return_value = connection
        namespace = {
            'engine': engine, 'text': object(), 'logging': logging,
            '_redact_sql_for_log': str,
        }
        inject = load_function(ROOT / 'conf/common/sql_functions.py', 'inject_sql', namespace)
        with self.assertRaises(ValueError):
            inject('SELECT missing_column;;', 'test')
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()

    def test_missing_optional_maternal_column_becomes_null(self):
        namespace = {
            'get_table_column_names': lambda *_: [('uid',), ('facility',), ('Gestation.value',), ('BWTDis.value',)],
            'table_exists': lambda *_: True,
            'params': {'country': 'zimbabwe'},
            'logging': logging,
            're': re,
        }
        query = load_function(PIPELINE / 'queries/create_summary_maternal_outcomes_sql.py',
                              'summary_maternal_outcomes_query', namespace)()
        self.assertIn('NULL::text AS "Presentation"', query)
        self.assertIn('DATE(NULL::timestamp)', query)
        self.assertNotIn('derived.maternal_outcomes."Presentation.label"', query)
        self.assertIn('derived.maternal_outcomes."Gestation.value"', query)

    def test_failed_summary_does_not_prevent_later_summaries(self):
        calls = []

        def step(name, failure=False):
            def run():
                calls.append(name)
                if failure:
                    raise SystemExit(1)
            return run

        namespace = {
            'env': 'prod', 'params': {'country': 'zimbabwe'}, 'logging': logging,
            'table_exists': lambda *_: False,
            'create_summary_vitalsigns': step('vitalsigns'),
            'create_summary_joined_vitalsigns': step('joined_vitalsigns'),
            'create_summary_maternal_outcomes': step('maternal_outcomes', True),
            'create_summary_neolabs': step('neolabs'),
        }
        create = load_function(PIPELINE / 'nodes_grouped/step_2_nodes/summary_tables.py',
                               'create_summary_tables', namespace)
        result = create({}, {})
        self.assertEqual(calls, ['vitalsigns', 'joined_vitalsigns', 'maternal_outcomes', 'neolabs'])
        self.assertEqual(result['status'], 'Partial Success')
        self.assertIn('maternal_outcomes', result['message'])

    def test_timestamp_backfill_casts_and_continues_after_failed_column(self):
        statements = []

        def read(query):
            if 'information_schema.columns' in query:
                return [(True, True, 'text', 'timestamp without time zone')]
            if 'ctid::text' in query:
                return [('(1,1)',), ('(1,2)',)]
            return [(True,)]

        def write(query, name):
            statements.append((query, name))
            if 'First' in name and ('ctid IN' not in query or "'(1,1)'::tid" in query):
                raise ValueError('bad timestamp')

        namespace = {
            'table_exists': lambda *_: True,
            'inject_sql_with_return': read,
            'inject_sql': write,
            '_escape_sql_literal': lambda value: value.replace("'", "''"),
            '_quote_identifier': lambda value: '"' + value + '"',
            'logging': logging,
            're': re,
            'execute_rows_with_fallback': load_function(
                ROOT / 'conf/common/sql_functions.py', 'execute_rows_with_fallback',
                {'inject_sql': write, 'hashlib': hashlib, 'logging': logging,
                 '_redact_sql_for_log': str}),
        }
        backfill = load_function(PIPELINE / 'queries/data_fix.py', 'backfill_renamed_keys', namespace)
        backfill('discharges', [('Old', 'First'), ('Old', 'Second')])
        self.assertGreater(len(statements), 4)
        self.assertIn('"Old.value"::timestamp without time zone', statements[0][0])
        self.assertTrue(any("ctid IN ('(1,2)'::tid)" in sql for sql, _ in statements))
        self.assertTrue(any('Second' in name for _, name in statements))


if __name__ == '__main__':
    unittest.main()
