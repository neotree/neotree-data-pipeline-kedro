# Taken from https://www.postgresqltutorial.com/postgresql-python/connect/
#!/usr/bin/python
from configparser import ConfigParser
#import libraries
import sys
import logging
import os
import stat
from pathlib import Path

#Configure General Logging

log = logging.getLogger('');

#Create Default Logs Directory If It doesnt Exist
if Path(os.getcwd()+'/logs').exists() and Path(os.getcwd()+'/logs').is_dir():
    pass;
else:
    logs_dir = Path(str(os.getcwd()+'/logs'))
    logs_dir.mkdir(exist_ok=True);

env = None
if len(sys.argv) >= 3:
    env = sys.argv[2]
    env = env.split('=')
    if(len(env)) >1:
        env = env[1];
else:
    log.error("Please include environment arguement (e.g. $ kedro run --env=dev)")
    sys.exit()
if env is not None:       
    def config(filename='conf/local/database.ini'):
        cwd = os.getcwd();
        #logs_dir = str(cwd+'/logs')
        
        if env == "prod":
            section = 'postgresql_prod'
        elif env == "stage":
            section = 'postgresql_stage'
        elif env == "dev":
            section = 'postgresql_dev'

        else:
            log.error("{0} is not a valid arguement: Valid arguements are (dev or stage or prod)".format(env))
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
                db[param[0]] = param[1];
        else:
            log.error('Section {0} not found in the {1} file'.format(section, filename))
            sys.exit()
        return db

   
