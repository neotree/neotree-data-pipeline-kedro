from conf.common.config import config
from conf.common.format_error import formatError
from crontab import CronTab
import logging
import os
import sys


DEFAULT_PIPELINE_INTERVAL_HOURS = 6
DEFAULT_SOURCE_UID_PREFLIGHT_MAX_ROWS = 100


def _build_kedro_command(project_dir, env, subcommand, extra_args=""):
    command = f"cd {project_dir} && env/bin/python -m kedro {subcommand} --env={env}"
    if extra_args:
        command = f"{command} {extra_args}"
    return command


def _build_pipeline_preflight_command(project_dir, env, max_rows_per_uid):
    preflight_command = _build_kedro_command(
        project_dir,
        env,
        "purge-known-test-uids-source-only",
        f"--max-rows-per-uid {max_rows_per_uid}",
    )
    pipeline_command = _build_kedro_command(project_dir, env, "run")
    return f"{preflight_command} && {pipeline_command}"


def _replace_job(cron, comment, command, interval_hours):
    cron.remove_all(comment=comment)
    job = cron.new(command=command, comment=comment)
    job.every(interval_hours).hours()
    return job


params = config()

mode = params["env"]
pipeline_interval = int(params.get("cron_interval", DEFAULT_PIPELINE_INTERVAL_HOURS))
source_preflight_max_rows = int(
    params.get("known_test_uid_cleanup_max_rows", DEFAULT_SOURCE_UID_PREFLIGHT_MAX_ROWS)
)
cron_dir = os.getcwd()

try:
    cron = CronTab(user=True)

    if not cron_dir:
        logging.info(
            "Please specify directory to find your kedro project in your database.ini file"
        )
        sys.exit()

    pipeline_comment = f"neotree-pipeline-{mode}"
    pipeline_command = _build_pipeline_preflight_command(
        cron_dir,
        mode,
        source_preflight_max_rows,
    )
    _replace_job(cron, pipeline_comment, pipeline_command, pipeline_interval)

    cron.write(user=True)

except Exception as e:
    logging.error("!!Cron Job Failed To Start Due To Errors: ")
    logging.error(formatError(e))
    sys.exit(1)
