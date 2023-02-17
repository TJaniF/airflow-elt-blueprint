Overview
========

Welcome to this hands-on repository to get started with Apache Airflow! :rocket:

This repository contains a fully functional best practice Airflow ETL pipeline. 


Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Structure
================

This repository contains the following files and folders:

- `.astro`: files necessary for Astro CLI commands.
- `.devcontainer`: the GH codespaces configuration.

-  `dags`: all DAGs in your Airflow environment. Files in this folder will be parsed by the Airflow scheduler when looking for DAGs to add to your environment. You can add your own dagfiles in this folder.
    - `ingestion`: two DAGs performing data ingestion.
    - `load`: one DAG performing data loading from MinIO to DuckDB.
    - `report`: one DAG running a streamlit app using data from DuckDB.
    - `transform`: one DAG using the Astro SDK to transform a table in DuckDB.
    - `start.py`: a DAG to kick off the pipeline.
    - `TOOL_TEST_DAG.py`: a DAG to test the connections to DuckDB, MinIO and Streamlit.

- `include`: supporting files that will be included in the Airflow environment.
    - `climate_data`: two csv files containing climate data.
    - `custom_task_groups`: one python file which contains a class instantiating a task group to create a bucket in MinIO if it does not exist already.
    - `global_variables`: one python file which contains global variables and utility functions.
    - `streamlit_app`: one python file defining a Streamlit app using the data in our pipeline.
    - `tool_testing`: one python file with a demo Streamlit app not dependent on pipeline data for the `TOOL_TEST_DAG`. 
    - (`minio`): folder that is created upon first start of the Airflow environment containing supporting file for the MinIO instance.

- `plugins`: folder to place Airflow plugins. Empty.
- `tests`: folder to place pytests running on DAGs in the Airflow instance. Contains default tests.
- `.dockerignore`: list of files to ignore for Docker.
- `.env`: environment variables. Contains the definition for the DuckDB connection.
- `.gitignore`: list of files to ignore for git. Note that `.env` is not ignored in this project.
- `docker-compose.override.yaml`: Docker override adding a MinIO container to this project, as well as forwarding additional ports.
- `packages.txt`: system-level packages to be installed in the Airflow environment upon building of the Dockerimage.
- `README.md`: this Readme.
- `requirements.txt`: python packages to be installed to be used by DAGs upon building of the Dockerimage.