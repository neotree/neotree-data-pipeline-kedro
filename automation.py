from conf.common.config import config
from conf.common.format_error import formatError
from crontab import CronTab
import logging
import sys
import os

params = config()

mode = params['env']
interval = 1
cronDir = os.getcwd();
#The number of hours before next execution of the next job as set in the database.ini file
if 'cron_interval'in params:
    interval = int(params['cron_interval'])

try:
# Set The User To Run The Cron Job
    cron = CronTab(user=True)
# Set The Command To Run The Data Pipeline script and activate the virtual environment
    if cronDir is not None:
        job = cron.new(command='cd {0} && env/bin/python -m kedro run --env={1}'.format(cronDir,mode))
    else:
        logging.info('Please specify directory to find your kedro project in your database.ini file')
        sys.exit()
    # Set The Time For The Cron Job
    # Use job.minute for quick testing
    job.every(interval).hours()
    # Write the Job To CronTab
    cron.write( user=True )

except Exception as e:
    logging.error("!!Cron Job Failed To Start Due To Errors: ")
    logging.error(formatError(e))
    sys.exit(1)

