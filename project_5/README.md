# Project 5: Data Pipelines

## Introduction

In this project, I am using airflow to build four different operators that will stage the data, transform the data, and run checks on data quality.
By utilizing Airflow's built-in functionalities as connections and hooks as much as possible, I let Airflow do all the heavy-lifting when it is possible.

## Operators(mostly copy from the project instruction)

### Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.
Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

### Operators Conclusion
| Operators             | Details                                                                                                            |
|-----------------------|--------------------------------------------------------------------------------------------------------------------|
| Stage Operators       | stage_events_to_redshift<br>stage_songs_to_redshift                                                                |
| Fact Operator         | load_songplays_table                                                                                               |
| Dimension Operators   | load_user_dimension_table<br>load_song_dimension_table<br>load_artist_dimension_table<br>load_time_dimension_table |
| Data Quality Operator | run_quality_checks                                                                                                 |

## Run Process
After finishing the workspace code part, here is the run process:
- 1 Set up IAM role and Redshift cluster, make sure the permissions, including S3 bucket permissions are proper setup
- 2 Run /opt/airflow/start.sh in workspace to access Airflow 
- 3 Create aws_credential and redshift connections on Airflow Web UI ( needed everytime restart airflow), make sure the inputs are correct
- 4 Turn on and trigger DAG create_table_dag first to create table
- 5 Turn on and trigger DAG data_pipelines_dag to run the full ETL data pipeline

## Airflow Dag (after a success run)
![Graph View](https://github.com/KeuzhiZuo/udacity-de-nanodegree-wip/blob/main/project_5/graph_view.PNG)

![Tree View](https://github.com/KeuzhiZuo/udacity-de-nanodegree-wip/blob/main/project_5/tree_view.PNG)
