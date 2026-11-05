"""Offline behavior checks for independent table processing."""

import ast
import logging
import re
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[4]
PIPELINE = ROOT / 'src/data_pipeline/pipelines/data_engineering'


def load_function(path, name, namespace):
    """Load one function without importing the module's live DB catalog."""
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name]


class FailureIsolationTests(unittest.TestCase):
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
            return [(True,)]

        def write(query, name):
            statements.append((query, name))
            if 'First' in name:
                raise ValueError('bad timestamp')

        namespace = {
            'table_exists': lambda *_: True,
            'inject_sql_with_return': read,
            'inject_sql': write,
            '_escape_sql_literal': lambda value: value.replace("'", "''"),
            '_quote_identifier': lambda value: '"' + value + '"',
            'logging': logging,
        }
        backfill = load_function(PIPELINE / 'queries/data_fix.py', 'backfill_renamed_keys', namespace)
        backfill('discharges', [('Old', 'First'), ('Old', 'Second')])
        self.assertEqual(len(statements), 4)
        self.assertIn('"Old.value"::timestamp without time zone', statements[0][0])
        self.assertTrue(any('Second' in name for _, name in statements))


if __name__ == '__main__':
    unittest.main()
