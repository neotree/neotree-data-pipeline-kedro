# NEOTREE DATAPIPELINE KEDRO VERSION

## OVERVIEW
This is a python code base used to manipulate raw exported scripts data from postgres database into derived tables that are easier to injest for business intelligence tools like Metabase.
This code base should run on any operating system that can run python, but comprehensive tests have been run on Ubuntu 18.04 and Windows 10.

## PREREQUISITES
==Python Version 3.XX or Higher 
== Working Connection To A Postgres Database Instance Containing The Scripts Data

## RULES AND GUIDELINES

In order to get the best out of the source code:

* Don't remove any lines from the `.gitignore` file we provide
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## INSTALLING DEPENDENCIES
At this stage the assumption is that we have cloned the code base to our local machine
1. cd into cwd ie (Current Working Directory or Project Directory)
2. Create a virtual environment using `virtualenv`,`pipenv` or easily by taking advantage of the venv package that was added to python 3 using the command 
`python3 -m venv env`  name    it `env` 
3. Activate the `env` virtual environment
4. Run `pip install kedro`
5. Validate if kedro has been successfully installed by running `kedro info` ,This should produce a graphical display of the word `Kedro`
6. cd into `src/` and run `pip install -r requirements.txt`
7. If you face any error, it is recommended to install the erronous dependencies independently using pip

* All dependencies are declared in `src/requirements.txt` 

## CONFIGURATIONS

> ## LOCAL CONFIGURATION:

The `conf/local` folder should be used for configuration that is either user-specific (e.g. IDE configuration) or protected (e.g. security keys).

* *Note:* Please do not check in any local configuration to version control.
In the case of this repository, this folder contains the important files known as `database.ini` and `hospitals.ini`
The `database.ini` file contains all sensitive and user specific configurations some which are optional and some which are required.
The `hospitals.ini` file contains hospitals script ids configurations
*Note* BOTH FILES ARE MANDATORY
## DATABASE.INI 
> ## Configuration Items:
* *Note:* You can define multiple Environments in the same `database.ini` file
1. Section:    Determines The Environment To Be Used, Valid section Names Are (`[postgresql_dev]`,`][postgresql_stage]` and `[postgresql_prod]`)
> The rule to name a section implies that it should have `postgresql` followed by an underscore `_` then the environment name (`dev`,`stage` or `prod`) All enclosed in square brackets `[]` e.g `[postgresql_dev]`

2. host :     The host for the postgres database e.g `localhost` -- REQUIRED FIELD
3. database:  The database name  e.g `neotree_data` -- REQUIRED FIELD
4. user :     The username used to connect to the database e.g `neotree_postgres_user` -- REQUIRED FIELD
5. password:  Connection password for the database user e.g `connection_pw1@5Fi` -- REQUIRED
6. country:   The country for which you are running the pipeline. Currently valid options are `zimbabwe` for Zimbabwe and                `malawi` for Malawi. *This Field is `MANDATORY` and should exactly match the country of choice specified in the            `hospitals.ini` file so that the correct scripts for the country of choice are picked.

7. mode:       This is the mode to be used when running the pipeline. *This field is `OPTIONAL` and if it is not                     included the pipeline will default to the `no_import` mode*. If mode = `import` the pipeline will look                at the specified `files_dir` to look for new data to import into the database before starting to run                  the stages of the datapipeline. I.E if set to `import` the pipeline will check if there are any new                   scripts in the `files_dir` that are not in the database , then import those first. *Please note that                  it looks at `scripts` not files , hence all duplicate scripts are eliminated during the importation                   stage*
8. files_dir:  Works hand in hand with the `mode` field. It is the path to the directory containing scripts that                     need to be imported into the database. The path should be specified using the path pattern of the                     operating system being in use. By Default it is `OPTIONAL` however it becomes `REQUIRED` the moment we                set `mode` to `import`

9. cron_interval: An `OPTIONAL` number value used to determine the number of hours to be used before the next running                   of the automated data pipeline. If not specified, the automation script will default to `6 hours`

## EXAMPLE OF FULL `database.ini` FILE:
    [postgresql_dev]
    host= localhost
    database=  neotree_data
    user= user
    password= password
    country = zimbabwe
    mode = import
    #DIRECTORY FOR RAW JSON FILES
    files_dir = C:\/Users\/morris\/Documents\/Data ### WINDOWS EXAMPLE, FOR OTHER OSs USE THE OSs PATH PATTERNS       cron_interval = 5  

## HOSPITALS.INI 
This contains all configurations for new hospitals
Initialy it should contain a set of configuration files for at least one hospital
The hospital's initials are used as the section name, and will be used as the facility in the database
Hence it is mandatory to have unique hospitals initials
> ## Configuration Items:
1. Section :  The initials of the hospital, enclosed in square brackets e.g `[SMCH]`
2. name    :  The full name of the hospital e.g `Sally Mugabe Central Hospital`
3. country :  The country name as specified in the `database.ini` file eg `zimbabwe`
4. admissions : Script Id For Admissions Script
5. discharges : Script Id For Discharges Script
6. maternals  : Script Id For Maternal Script
7. vital_signs : Script Id For Vital Signs Script
8. neolabs     : Script Id For Neolab Data
9. baselines   : Script Id For Baseline Data
10. maternal_dev: Script Id For Maternal Outcomes Dev (Special Case, else Dev Script Ids Should Be The Same As Prod Ones)
>Where the item doesn't have a value, please put the item, an equal sign and nothing after, as demonstrated in the example below

## EXAMPLE OF FULL `hospitals.ini` FILE:

[PGH]
name= Parirenyatwa Group of Hospitals
country=  zimbabwe
admissions = DNPARDEMOSERTDOOO
discharges=  DEOMENOEDEMODEOIO
maternals =  -JEKIO12OKDEMOdDE
vital_signs = -DEjSKILOKOLLLS-
neolabs =   -SJKKKDDKKKKSSSLS-
baselines = 

[BPH]
name= Bindura Provincial Hospital
country=  zimbabwe
admissions = -GGCDEMO122DKK0W
discharges=  -GGCDEMO122DKK0W
maternals = 
vital_signs =
neolabs = 
baselines = -MHDEMODEMO9nmtfb

> ## BASE CONFIGURATION:

The `conf/base` folder is for shared configuration, such as non-sensitive and project-related configuration that may be shared across team members. It also contains the mappings of scriptIds to their respective countries, and queries that map the scriptids to their respective health facilities as prescribed in the file `conf/base/catalog`. 
*IT IS RECOMMENDED NOT TO CHANGE ANYTHING IN THIS FOLDER*

## RUNNING THE PROJECT
> At this point the assumption is that:
1. You have installed kedro
2. You have installed all dependencies
3. You have a working postgres database, and a valid username and password to connect to the database, and your          database consists of at least 3 schemas i.e `public`,`derived` and `scratch`
4. You have setup your configurations in `database.ini` and `hosptals.ini`
5. You have data in `sessions` table which should be in the `public` schema, (or if the table is empty, you have some     json files in your `files_dir` and your `mode` is set to `import`
6. You have activated the virtual environment that you created earlier and (*MAKE SURE THIS IS THE VIRTUAL                ENVIRONMENT IN WHICH YOU HAVE INSTALLED YOUR DEPENDENCIES*)
> If all the above mentioned requirements are met then:
1. cd into the Project directory
2. Run the command `kedro run --env=specified_env --parallel` where `specified_env` is the environment as specified in the         `SECTION` part of the `database.ini` file.
Using the example of the `database.ini` specified in the section `EXAMPLE OF FULL `database.ini` FILE` my command will be as follows:      `kedro run --env=dev --parallel`
-- The `parallel` flag is used for multiprocessing
After running the above command, logs should start appearing on your screen, detailing the steps that are being run.
> On completion, we should check in the logs if the number of steps that have been run tallies with the total number    of steps expected to run. For Example, if you are expecting 13 steps to run, there should be a line in the logs showing on your screen which shows the statement `Completed 13 out of 13 tasks`
> After a successful running of the pipeline, if you login to your `postgresql database` you should expect to see some derived tables and exploded tables in the `derived` schema of the database.

## AUTOMATION 
> Currently this has been tested against Ubuntu
> Within the datapipeline there is an automation script, which writes cron jobs to the cron service
## Before Running The Automation Script:
1. Make sure that the cron service is running on your machine
2. Make sure that the currently logged in user is added to the cron users(*Doing This Requires `Root` access*)
3. Make sure the name of your virtual environment is `env` as there is a reference of the virtual environment name in   the automation script
> If the above are satisfied :
1. cd into the project directory
2. activate your virtual environment
3. Run the command `python automation.py kedro --env=specified_env` where `specified_env` is the environment as specified in the `SECTION` part of the `database.ini` file. e.g `python automation.py kedro --env=dev` or 
`python automation.py kedro --env=stage`
*Note*: You can run the automation script on the same machine for all the 3 available environments i.e (`dev,stage,prod`) if the configurations are available.All you have to do is to run the command in point `3` changing the variable `env` to suit the required environment.
4. To confirm that your entries have been written to the crontab file, run `crontab -e` then check if your entries are available
>It is important to specify the time zone in the `crontab` file before starting to run the automation script so that you won't have challenges with differences in server time against the time zone that you want the automation script to run.
>To set the time zone append the following line at the top of your `crontab` file: `TZ="SPECIFY_TIMEZONE` e.g `TZ= "Africa/Harare"`

## ALTERNATIVELY ##
> If you have knowledge with the linux operating system, you can write the automation command directly to the cron service by following the the steps below:
1. Make sure that the cron service is running on your machine
2. Make sure that the currently logged in user is added to the cron users(*Doing This Requires `Root` access*) 
3. Run `crontab -e` and a file to write your automation command will be opened. *Please Note:**When you first open the file, it will prompt you on the text editor that you want to use for opening the file, and that will be used as the default editor during subsequent opening of the file
4. Set Time zone as specified in the section above.
5. Write your automantion command, save your changes and exit.

## THE AUTOMATION SCRIPT COMMAND ##
>The automation command follows the general cron format containing the time,as well as the command to be run
>The time is specified in the format `mm hh dd MM yy` where :
`mm`= Minutes and takes values `0 to  59` or `*` for every minute or `*/2 to 59` for any minute which is divisible by the number specified in the denominator
`hh`= Hours and takes values between `0 to 23` or `*` for every hour or `*/2 to 23` for any hour which is divisible by the number specified in the denominator
`MM` = Months and takes any value from `1 to 12` or `*` for every month or `*/2 to 12` for any month which is divisible by the number specified in the denominator
`YY` = For the year
## EXAMPLES ##
1. ` 0  */4 * * * `  implies that the script runs at on the zeroeth minute after every 4 hours, every day , every month, every year
2. `30 * * * * ` implies that the script runs every 30 minutes, every hour, every day, every month, every year

>. The complete automation script command, contains :
1. The Time
2. getting into the project directory through the `cd` command
3. The kedro command to run the pipeline, specifying the environment specified in the `database.ini` file

## EXAMPLE OF THE COMPLETE AUTOMATION SCRIPT COMMAND
` 0 */6 * * *  cd /home/ubuntu/neotree-datapipeline-kedro  env/bin/python -m kedro run --env=specified_env --parallel` where `specified_env` is an environment specified in the `database.ini` file. 
*NOTE*: *Your python environment should be named `env` else change the command `env/bin/python` to suit the name of your environment*
*NOTE*: *The automation scripts should be set or run after all the setup process including the installation of dependencies has been completed*

## LOGS
There are 3 (three) main log files that are generated by the data pipeline when it runs:
1. kedro_info.log => This documents all the steps that are run during the exercution process of the data pipeline.                        Each time the data pipeline runs, the new logs are appended to the existing log file, hence it is                     advised to read the logs at the `tail` of the document for the latest logs

2. kedro_error.log => This documents all the errors emanating from running the data pipeline. The error logs have a                       time stamp just like all the other logs, hence it is advisable to check, the time of the log as                       well as to read the logs at the `tail` of the document.

3. data_pipeline_cron.log => This documents a summary for running the data pipeline, it includes, the time stamp, the                        environment i.e `dev,stage,or prod` , the status i.e whether it executed to completion or it                          failed. If it failed , at what stage it failed.

>By default all the log files are found in `/logs` directory which is in the project directory.
> *HOWEVER:* *If you are running on `Ubuntu` by default the `data_pipeline_cron.log` file will be saved in the default logs directory for `ubuntu` which is `/var/log/`

## COMMON SETUP ERRORS
> `ModuleNotFoundError: No module named 'psycopg2'`
To fix the above error, please install 'psycopg2-binary' in your virtual environment e.g using pip `pip install psycopg2-binary `


