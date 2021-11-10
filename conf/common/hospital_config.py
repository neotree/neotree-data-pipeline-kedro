from configparser import ConfigParser
#import libraries
import sys
import logging
import os
from pathlib import Path


log = logging.getLogger('');

    ##log.error("Please include environment arguement (e.g. $ kedro run --env=dev)")
    ##sys.exit()
    
def hospital_conf(filename='conf/local/hospitals.ini'):
        cwd = os.getcwd();
        #Create Default Logs Directory If It doesnt Exists
        if Path(cwd+'/conf/local/hospitals.ini').exists():
           
            parser = ConfigParser()
             # read config file
            parser.read(filename)

            conf = {}
            for section in parser.sections():
                section_params = {}
                params = parser.items(section)
            # add environment to global params for use by other functions
                for param in params:
                    section_params[param[0]] = param[1];
                conf[section] = section_params 
        else:
            log.error('The file hospital_config.ini does not exist, please put it in the folder conf/local')
            sys.exit()
        return conf

   
