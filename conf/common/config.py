# Taken from https://www.postgresqltutorial.com/postgresql-python/connect/
#!/usr/bin/python
from configparser import ConfigParser
#import libraries
import sys
import logging
import os
import stat
from pathlib import Path

env = None
if len(sys.argv) >= 3:
    env = sys.argv[2]
    env = env.split('=')
    if(len(env)) >1:
        env = env[1];
else:
    logging.error("Please include environment arguement (e.g. $ kedro run --env=dev)")
    sys.exit()
if env is not None:       
    def config(filename='conf/local/database.ini'):
        if env == "prod":
            section = 'postgresql_prod'
        elif env == "stage":
            section = 'postgresql_stage'
        elif env == "dev":
            section = 'postgresql_dev'

        else:
            logging.error("{0} is not a valid arguement: Valid arguements are (dev or stage or prod)".format(env))
            sys.exit()

         # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
       
        if parser.has_section(section):
            params = parser.items(section)
        # add environment to global params for use by other functions
            db['env'] = env
            for param in params:
                db[param[0]] = param[1]
        else:
            logging.error('Section {0} not found in the {1} file'.format(section, filename))
            sys.exit()
        return db

   
