# NEOTREE DATAPIPELINE KEDRO VERSION

## OVERVIEW
This is a python code base used to manipulate raw exported scripts data from postgres database into derived tables that are easier to injest for 
This code base should run on any operating system that can run python, but comprehensive tests have been run on Ubuntu 18.04 and Windows 10.

## PREREQUISITES
==Python Version 3.XX or Better 
== Working Connection To A Postgres Database Instance Containing The Scripts Data

## RULES AND GUIDELINES

In order to get the best out of the source code:

* Don't remove any lines from the `.gitignore` file we provide
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## INSTALLING DEPENDENCIES
At this stage the assumption is that you have cloned the code base to our local machine
1. cd into cwd ie (Current Working Directory or Project Directory)
2. Create a virtual environment using and name it `env` using `virtualenv`,`pipenv` or any other virtual environment creation dependency
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
In the case of this repository, this folder contains the important file known as `database.ini`
The `database.ini` file contains all sensitive and user specific configurations some which are optional and some which are required
> ## Configuration Items:
* *Note:* You can define multiple Environments in the same `database.ini` file
1. Section     ===Determines The Environment To Be Used, Valid section Names Are (`[postgresql_dev]`,`][postgresql_stage]` and `[postgresql_prod]`)
> The rule to name a section implies that it should have `postgresql` followed by an underscore `_` then the environment name (`dev`,`stage` or `prod`) All enclosed in square brackets `[]` e.g `[postgresql_dev]`

2. 

> ## BASE CONFIGURATION:

The `conf/base` folder is for shared configuration, such as non-sensitive and project-related configuration that may be shared across team members.
## RUNNING THE PROJECT

You can run your Kedro project with:

```
kedro run
```
## AUTOMATION 

## LOGS

## INTERCONNECTION OF NODES



