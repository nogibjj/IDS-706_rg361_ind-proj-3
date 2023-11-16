# Data Pipeline in Databricks

[![CI](https://github.com/nogibjj/IDS-706_rg361_week-11/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/IDS-706_rg361_week-11/actions/workflows/cicd.yml)

This repositroy contains files to process data in ``Databricks`` using ``PySpark``, ``Python``  and ``SQL``

The repo has been created from [Week-10 Mini-Project](https://github.com/nogibjj/IDS-706_rg361_week-10) and modified as per requirements.

Created on on 08-Nov-2023

## Overview

This project performs a sample End-to-End Data Pipeline in Databricks.
The sample data is loaded from Databricks and the notebooks used for processing are saved in this Github Repository in the ``Notebooks`` folder.

A Databricks ``workflow`` is setup to run the notbooks in sequence to simulate the End-to-End workflow.

``Github`` actions automatically performs the ``CICD`` workflows whenever there is a change in the repository.

![Schema](resources/schema.png)

## Instructions

The Primary data and operations happen in the Databricks platform.

We use the ``million songs`` sample dataset available in databricks for this process.

4 Notebooks have been created to perform the following tasks or steps in the pipeline:

1. ``EDA``: To get an overview of the data, this is only exploratory in nature and is **NOT** a part of the Final Workflow
![EDA](resources/nb_eda.png)

2. ``Ingestion``: This notebook loads the data from the sample dataset and saves it as ``songs`` table in Databricks
![EDA](resources/nb_ingestion.png)

3. ``Preparation``: This notebook contains the SQL code to process the raw data in ``songs`` and stores it as ``songs_prepared`` in databricks.
![EDA](resources/nb_processing.png)

4. ``Analyze``: This notebook has some sample queries to view data from the ``songs_prepared`` dataset.
![EDA](resources/nb_analyze.png)


## Workflows
There are 2 workflows which happen in this project, the Data worflow which happens in Databricks and the CICD worflow which happens in Gtihub.

### Data Worflow
A simple workflow ``Data_Workflow`` has been setup in Databricks which performs the following 3 actions using the corresponding notebooks as mentioned above:
1. Ingestion
2. Preparation
3. Analyze

Sucessful execution of Databricsk Workflow:
![Data Workflow](resources/workflow.png)

**Note**: The workflow is set to be manually triggered to save costs.

### CICD Workflow
github actions are used to automate the following processes whenever a change is made to the files in the repository:
   - ``install`` : installs the packages and libraries mentioned in the requirements.txt
   - ``test`` : uses ``pytest`` to test the python script
      
      **Note:** Currently there is no test setup since the files and workflow runs in Databricks.
     
   - ``format`` : uses ``black`` to format the python files
   - ``lint`` : uses ``ruff`` to lint the python files

**Note** -if all the processes run successfully the following output will be visible in github actions:
   ![Success Build](resources/build.png)

   
## Contents
The Github Repository Contains the following items:

### 1. README.md
   contains the information about the repository and instructions for using it
   
### 2. requirements.txt
   contains the list of packages and libraries which are required for running the project. These are intalled and used in the virtual environment and Github actions.
   
### 3. .github/workflows
  Contains the ``cicd.yml`` file which has the steps and instructions for the Github CICD workflow (using Github Actions)
 
### 4. Makefile
   contains the instructions and sequences for the processes used in github actions and .devcontainer for creating the virtual environment
   
### 5. .devcontainer
   contains the ``dockerfile`` and ``devcontainer.json`` files which are used to build and define the setting of the virtual environment (codespaces - python) for running the codes.

### 6. Notebooks
   The 4 notebooks which are mentioned earlier are stored in the ``Notebooks`` folder in this Github repository so as to have version control and CICD

### 7. resources 
   contains additonal files which are used in the README




  
