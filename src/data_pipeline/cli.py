
"""Command line tools for manipulating a Kedro project.
Intended to be invoked via `kedro`."""
import logging
import os
import sys
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, Tuple

import click
from kedro.framework.cli import main as kedro_main
from kedro.framework.cli.catalog import catalog as catalog_group
from kedro.framework.cli.jupyter import jupyter as jupyter_group
from kedro.framework.cli.pipeline import pipeline as pipeline_group
from kedro.framework.cli.project import project_group
from kedro.framework.cli.utils import KedroCliError, env_option, split_string
from kedro.framework.session import KedroSession
from kedro.utils import load_obj

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# get our package onto the python path
PROJ_PATH = Path(__file__).resolve().parent

ENV_ARG_HELP = """Run the pipeline in a configured environment. If not specified,
pipeline will run using environment `local`."""
FROM_INPUTS_HELP = (
    """A list of dataset names which should be used as a starting point."""
)
FROM_NODES_HELP = """A list of node names which should be used as a starting point."""
TO_NODES_HELP = """A list of node names which should be used as an end point."""
NODE_ARG_HELP = """Run only nodes with specified names."""
RUNNER_ARG_HELP = """Specify a runner that you want to run the pipeline with.
Available runners: `SequentialRunner`, `ParallelRunner` and `ThreadRunner`.
This option cannot be used together with --parallel."""
PARALLEL_ARG_HELP = """Run the pipeline using the `ParallelRunner`.
If not specified, use the `SequentialRunner`. This flag cannot be used together
with --runner."""
ASYNC_ARG_HELP = """Load and save node inputs and outputs asynchronously
with threads. If not specified, load and save datasets synchronously."""
TAG_ARG_HELP = """Construct the pipeline using only nodes which have this tag
attached. Option can be used multiple times, what results in a
pipeline constructed from nodes having any of those tags."""
LOAD_VERSION_HELP = """Specify a particular dataset version (timestamp) for loading."""
CONFIG_FILE_HELP = """Specify a YAML configuration file to load the run
command arguments from. If command line arguments are provided, they will
override the loaded ones."""
PIPELINE_ARG_HELP = """Name of the modular pipeline to run.
If not set, the project pipeline is run by default."""
PARAMS_ARG_HELP = """Specify extra parameters that you want to pass
to the context initializer. Items must be separated by comma, keys - by colon,
example: param1:value1,param2:value2. Each parameter is split by the first comma,
so parameter values are allowed to contain colons, parameter keys are not."""


def _config_file_callback(ctx, param, value):  # pylint: disable=unused-argument
    """Config file callback, that replaces command line options with config file
    values. If command line options are passed, they override config file values.
    """
    # for performance reasons
    import anyconfig  # pylint: disable=import-outside-toplevel

    ctx.default_map = ctx.default_map or {}
    section = ctx.info_name

    if value:
        config = anyconfig.load(value)[section]
        ctx.default_map.update(config)

    return value


def _get_values_as_tuple(values: Iterable[str]) -> Tuple[str, ...]:
    return tuple(chain.from_iterable(value.split(",") for value in values))


def _reformat_load_versions(  # pylint: disable=unused-argument
    ctx, param, value
) -> Dict[str, str]:
    """Reformat data structure from tuple to dictionary for `load-version`, e.g:
    ('dataset1:time1', 'dataset2:time2') -> {"dataset1": "time1", "dataset2": "time2"}.
    """
    load_versions_dict = {}

    for load_version in value:
        load_version_list = load_version.split(":", 1)
        if len(load_version_list) != 2:
            raise KedroCliError(
                f"Expected the form of `load_version` to be "
                f"`dataset_name:YYYY-MM-DDThh.mm.ss.sssZ`,"
                f"found {load_version} instead"
            )
        load_versions_dict[load_version_list[0]] = load_version_list[1]

    return load_versions_dict


def _split_params(ctx, param, value):
    if isinstance(value, dict):
        return value
    result = {}
    for item in split_string(ctx, param, value):
        item = item.split(":", 1)
        if len(item) != 2:
            ctx.fail(
                f"Invalid format of `{param.name}` option: "
                f"Item `{item[0]}` must contain "
                f"a key and a value separated by `:`."
            )
        key = item[0].strip()
        if not key:
            ctx.fail(
                f"Invalid format of `{param.name}` option: Parameter key "
                f"cannot be an empty string."
            )
        value = item[1].strip()
        result[key] = _try_convert_to_numeric(value)
    return result


def _try_convert_to_numeric(value):
    try:
        value = float(value)
    except ValueError:
        return value
    return int(value) if value.is_integer() else value


def _bootstrap_legacy_env_arg(env: str) -> None:
    """Keep legacy config parsing working for utility commands."""
    normalized_env_arg = f"--env={env}"
    current_argv = list(sys.argv)

    rebuilt_argv = current_argv[:2] + [normalized_env_arg]
    skip_next = False

    for arg in current_argv[2:]:
        if skip_next:
            skip_next = False
            continue
        if arg == "--env":
            skip_next = True
            continue
        if arg.startswith("--env="):
            continue
        rebuilt_argv.append(arg)

    sys.argv = rebuilt_argv


@click.group(context_settings=CONTEXT_SETTINGS, name=__file__)
def cli():
    """Command line tools for manipulating a Kedro project."""


@cli.command()
@click.option(
    "--from-inputs", type=str, default="", help=FROM_INPUTS_HELP, callback=split_string
)
@click.option(
    "--from-nodes", type=str, default="", help=FROM_NODES_HELP, callback=split_string
)
@click.option(
    "--to-nodes", type=str, default="", help=TO_NODES_HELP, callback=split_string
)
@click.option("--node", "-n", "node_names", type=str, multiple=True, help=NODE_ARG_HELP)
@click.option(
    "--runner", "-r", type=str, default=None, multiple=False, help=RUNNER_ARG_HELP
)
@click.option("--parallel", "-p", is_flag=True, multiple=False, help=PARALLEL_ARG_HELP)
@click.option("--async", "is_async", is_flag=True, multiple=False, help=ASYNC_ARG_HELP)
@env_option
@click.option("--tag", "-t", type=str, multiple=True, help=TAG_ARG_HELP)
@click.option(
    "--load-version",
    "-lv",
    type=str,
    multiple=True,
    help=LOAD_VERSION_HELP,
    callback=_reformat_load_versions,
)
@click.option("--pipeline", type=str, default=None, help=PIPELINE_ARG_HELP)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help=CONFIG_FILE_HELP,
    callback=_config_file_callback,
)
@click.option(
    "--params", type=str, default="", help=PARAMS_ARG_HELP, callback=_split_params
)
def run(
    tag,
    env,
    parallel,
    runner,
    is_async,
    node_names,
    to_nodes,
    from_nodes,
    from_inputs,
    load_version,
    pipeline,
    config,
    params,
):
    """Run the pipeline."""
    if parallel and runner:
        raise KedroCliError(
            "Both --parallel and --runner options cannot be used together. "
            "Please use either --parallel or --runner."
        )
    runner = runner or "SequentialRunner"
    if parallel:
        runner = "ParallelRunner"
    runner_class = load_obj(runner, "kedro.runner")

    tag = _get_values_as_tuple(tag) if tag else tag
    node_names = _get_values_as_tuple(node_names) if node_names else node_names

    package_name = str(Path(__file__).resolve().parent.name)
    # Preflight cleanup for known test UIDs before running the pipeline.
    if env:
        _bootstrap_legacy_env_arg(env)
        from data_pipeline.pipelines.data_engineering.queries.data_fix import (
            purge_known_test_uids_source_only as purge_known_test_uids_source_only_job,
        )
        purge_known_test_uids_source_only_job()

    with KedroSession.create(package_name, env=env, extra_params=params) as session:
        session.run(
            tags=tag,
            runner=runner_class(is_async=is_async),
            node_names=node_names,
            from_nodes=from_nodes,
            to_nodes=to_nodes,
            from_inputs=from_inputs,
            load_versions=load_version,
            pipeline_name=pipeline,
        )


@cli.command("purge-uid")
@click.option("--env", required=True, help=ENV_ARG_HELP)
@click.option(
    "--uid",
    "target_uid",
    required=True,
    help="UID/NUID value to scan for and remove.",
)
@click.option(
    "--schema",
    "schemas",
    multiple=True,
    help="Schema to scan. Defaults to public and derived.",
)
@click.option(
    "--column",
    "candidate_columns",
    multiple=True,
    help="Candidate UID column name. Defaults to uid/nuid/neotree_id variants.",
)
@click.option(
    "--execute",
    is_flag=True,
    help="Delete matching rows. Without this flag, the command only reports matches.",
)
@click.option(
    "--confirm-uid",
    default="",
    help="Required with --execute. Must exactly match --uid.",
)
def purge_uid(env, target_uid, schemas, candidate_columns, execute, confirm_uid):
    """Scan for a test UID/NUID and optionally delete it across database tables."""
    if execute and confirm_uid != target_uid:
        raise KedroCliError(
            "--execute requires --confirm-uid to exactly match the value passed to --uid."
        )

    _bootstrap_legacy_env_arg(env)

    from data_pipeline.pipelines.data_engineering.queries.data_fix import (
        purge_uid_records,
    )
    try:
        scan_results = purge_uid_records(
            uid=target_uid,
            schemas=schemas or None,
            candidate_columns=candidate_columns or None,
            dry_run=True,
        )

        if not execute:
            click.echo(
                f"Matched {scan_results['affected_rows']} row(s) across "
                f"{scan_results['affected_tables']} table(s); "
                f"scanned {scan_results['scanned_tables']} table(s)."
            )
            for table_name in scan_results["affected_table_names"]:
                click.echo(table_name)
            click.echo("Dry run only. Re-run with --execute to delete matching rows.")
            return

        if not scan_results["matches"]:
            click.echo("No matching rows found. Nothing to delete.")
            return

        delete_results = purge_uid_records(
            uid=target_uid,
            schemas=schemas or None,
            candidate_columns=candidate_columns or None,
            dry_run=False,
            table_matches=scan_results["matches"],
        )
        click.echo(
            f"Deleted {delete_results['affected_rows']} row(s) across "
            f"{delete_results['affected_tables']} table(s); "
            f"scanned {scan_results['scanned_tables']} table(s)."
        )
        for table_name in delete_results["affected_table_names"]:
            click.echo(table_name)
    except Exception:
        logging.exception("Manual UID cleanup failed for uid '%s'", target_uid)
        raise KedroCliError(
            "UID cleanup failed. Check the existing error log for details."
        )


@cli.command("purge-known-test-uids")
@click.option("--env", required=True, help=ENV_ARG_HELP)
@click.option(
    "--schema",
    "schemas",
    multiple=True,
    help="Schema to scan. Defaults to public, derived, and scratch.",
)
@click.option(
    "--column",
    "candidate_columns",
    multiple=True,
    help="Candidate UID column name. Defaults include uid and NeoTree ID variants.",
)
@click.option(
    "--max-rows-per-uid",
    default=100,
    show_default=True,
    type=int,
    help="Safety cutoff. Skip deletion when a known test UID exceeds this match count.",
)
def purge_known_test_uids(env, schemas, candidate_columns, max_rows_per_uid):
    """Purge the scheduled allowlist of known test UIDs."""
    _bootstrap_legacy_env_arg(env)

    from data_pipeline.pipelines.data_engineering.queries.data_fix import (
        KNOWN_TEST_UIDS,
        purge_known_test_uids as purge_known_test_uids_job,
    )

    try:
        results = purge_known_test_uids_job(
            schemas=schemas or None,
            candidate_columns=candidate_columns or None,
            max_rows_per_uid=max_rows_per_uid,
        )
        click.echo(
            f"Checked {results['uids_checked']} known test UID(s); "
            f"deleted {results['uids_deleted']}, skipped {results['uids_skipped']}, "
            f"removed {results['total_rows_deleted']} row(s)."
        )
        click.echo(f"Allowlist: {', '.join(KNOWN_TEST_UIDS)}")
        for result in results["results"]:
            click.echo(
                f"{result['uid']}: {result['status']} ({result['rows']} row(s))"
            )
    except Exception:
        logging.exception("Scheduled known test UID cleanup failed")
        raise KedroCliError(
            "Known test UID cleanup failed. Check the existing error log for details."
        )


@cli.command("purge-known-test-uids-source-only")
@click.option("--env", required=True, help=ENV_ARG_HELP)
@click.option(
    "--column",
    "candidate_columns",
    multiple=True,
    help="Candidate UID column name. Defaults include uid and NeoTree ID variants.",
)
@click.option(
    "--max-rows-per-uid",
    default=100,
    show_default=True,
    type=int,
    help="Safety cutoff. Skip deletion when a known test UID exceeds this match count.",
)
def purge_known_test_uids_source_only(env, candidate_columns, max_rows_per_uid):
    """Purge known test UIDs only from source session tables before pipeline execution."""
    _bootstrap_legacy_env_arg(env)

    from data_pipeline.constants import SOURCE_UID_CLEANUP_TABLES
    from data_pipeline.pipelines.data_engineering.queries.data_fix import (
        KNOWN_TEST_UIDS,
        purge_known_test_uids_source_only as purge_known_test_uids_source_only_job,
    )

    try:
        results = purge_known_test_uids_source_only_job(
            candidate_columns=candidate_columns or None,
            max_rows_per_uid=max_rows_per_uid,
        )
        click.echo(
            f"Checked {results['uids_checked']} known test UID(s); "
            f"deleted {results['uids_deleted']}, skipped {results['uids_skipped']}, "
            f"removed {results['total_rows_deleted']} row(s) from source tables."
        )
        click.echo(f"Allowlist: {', '.join(KNOWN_TEST_UIDS)}")
        click.echo(
            "Source tables: "
            + ", ".join(f"{schema}.{table}" for schema, table in SOURCE_UID_CLEANUP_TABLES)
        )
        for result in results["results"]:
            click.echo(
                f"{result['uid']}: {result['status']} ({result['rows']} row(s))"
            )
    except Exception:
        logging.exception("Source-only known test UID cleanup failed")
        raise KedroCliError(
            "Source-only known test UID cleanup failed. Check the existing error log for details."
        )


cli.add_command(pipeline_group)
cli.add_command(catalog_group)
cli.add_command(jupyter_group)

for command in project_group.commands.values():
    cli.add_command(command)


if __name__ == "__main__":
    os.chdir(str(PROJ_PATH))
    kedro_main()
