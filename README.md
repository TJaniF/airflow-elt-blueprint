Overview
========

Welcome to this hands-on repository to get started with Apache Airflow! :rocket:

This repository contains a fully functional best practice Airflow ETL pipeline that can be run in GitHub codespaces (or locally with the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)). You can use it to explore Apache Airflow best practices or as a template to build your own pipeline! 

How to use this repository
==========================

## Setting up

### Codespaces


![Open Airflow UI URL Codespaces](src/open_airflow_ui_codespaces.png)


### With the Astro CLI

1. Run `git clone https://github.com/TJaniF/astronomer-codespaces-test.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli).
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

## Run the project

1. Go to `include/global_variables/global_variables.py` and enter your own info for `MY_NAME`, `MY_CITY`, `MY_COUNTRY`. 
2. Unpause all DAGs by clicking on the toggle on their left hand side. Once the `start` DAG is unpaused it will run once, starting the pipeline. You can also run this DAG manually to trigger further pipeline runs by clicking on the play button on the right side of the DAG.
4. Watch the DAGs run according to their dependencies which have been set using [Datasets](https://docs.astronomer.io/learn/airflow-datasets).

![Dataset and DAG Dependencies](src/dataset_dag_dependency.png)

5. The last DAG in the pipeline `run_streamlit_app`, will stay in a running state as shown in the screenshot below.

![DAGs view after first run](src/click_on_run_streamlit.png)

6. Open the Streamlit app. If you are using codespaces go to the **Ports** tab and open the URL of the forwarded port `8501`. If you are running locally go to `localhost:8501`.

![Open Streamlit URL Codespaces](src/open_streamlit_codespaces.png)

7. View the Streamlit app.

![Streamlit app](src/streamlit_app.png)


How it works
============



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
- `src`: contains images used in this README.
- `tests`: folder to place pytests running on DAGs in the Airflow instance. Contains default tests.
- `.dockerignore`: list of files to ignore for Docker.
- `.env`: environment variables. Contains the definition for the DuckDB connection.
- `.gitignore`: list of files to ignore for git. Note that `.env` is not ignored in this project.
- `docker-compose.override.yaml`: Docker override adding a MinIO container to this project, as well as forwarding additional ports.
- `packages.txt`: system-level packages to be installed in the Airflow environment upon building of the Dockerimage.
- `README.md`: this Readme.
- `requirements.txt`: python packages to be installed to be used by DAGs upon building of the Dockerimage.