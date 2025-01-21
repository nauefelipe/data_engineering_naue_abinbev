# Brewery Data Pipeline Case by Nauê

Hello! I'm Nauê and here's my repository for the Data Engineer position at BEES-AbInbev. 

This project follows the medallion architecture (Bronze, Silver, Gold layers) and implements a data pipeline that gets data from the Open Brewery DB API and builds a data lake in AWS S3 with it, following the medallion architecture (Layers Bronze, Silver and Gold).

Docker is used for containerization, while Apache Airflow handles the job orchestration and Python/Pyspark are in change of fetching, transforming and writing the data. 

# Design Choices and Trade-Offs:
To design the ETL pipeline, I chose to use **Python** for being an widely-used language in data processing tasks, alongside with **Pyspark**. 

Even though Pyspark may seem like overkill for a small dataset like this one, I wanted to create an environment that could offer scalability. PySpark, as a distributed computing framework, can handle large datasets without giving up performance.

In this project, I chose to use **Airflow** to orchestrate the job workflow, using a Directed Acyclic Graph (DAG) to manage the dependencies between tasks and enabling a easy way to visualize the whole process. Also, another container runs a Postgres instance, that acts as a backend for Airflow's metadata

For cloud tools, I used **AWS S3** as the datalake's foundation, since it works so seamlessly with Pyspark, which facilitates data retrieval, storage and processing. For the aggregated view displayed in the S3's gold layer, there's a **AWS Athena** database and table ready for querying after the whole ETL process is over. 

Inside S3, I opted for creating three buckets, one for each layer. In the long run, it would be a better solution to keep every stage of data on their own bucket, facilitating data management and supporting scalability.

Additionaly, all of these resources run in **Docker**, which provides an isolate environment for every application.
This allows Airflow to run the orchestration in a container, while the Spark jobs handle the data-heavy operations in another one, for example.

Here's a walkthrough of the steps:

Docker creates a container for Airflow and another one for postgres, which is used as a storage for Airflow's metadata
With the Airflow container up and running, a DAG will run executing these tasks:

- **Build the Pyspark Image:** Runs the Dockerfile_pypark
- **Tests:** This pre-processing step will run a series of tests to ensure that the following steps are running accordingly
- **Bronze Task:** It will get data form the API URL and store it in its raw form in the bronze-layer bucket in S3
- **Silver Task:** Retrieves the raw data from the bronze layer and applies a series of transformations in order to make it usable (Turns the JSON into a structured format, creates and drops columns, etc..)
- **Gold Task:** It's responsible for getting the data treated from silver layer and aggregating as requested (quantity of breweries broken down by location and brewery type). Plus, it also creates a table on AWS Athena where stakeholders can query the data in S3 

## Table of Contents:
- [Requirements](#requirements)
- [Setting up the project](#setting-up-the-project)
- [Architecture](#architecture)
- [Architecture](#monitoring/alerting)
- [Contact Information](#contact-information)

## Requirements:
- Docker Desktop (Docker Compose version v2.27.0-desktop.2)
- git (to clone this repository)
- AWS S3 (AmazonS3FullAccess)
- AWS Athena (AmazonAthenaFullAccess)
- AWS Access keys
- Three S3 Buckets (With the Bucket Versioning option disabled during creation so we avoid duplicate data)

## Setting up the project

## 1. Open a Git Bash terminal and clone the repository, then access the folder:
With Git and Docker installed, you can proceed to run these commands
```
git clone https://github.com/nauefelipe/data_engineering_naue_abinbev.git
cd data_engineering_naue_abinbeb
```

## 2. If you're running on Windows 10 or 11, you might want to check some Docker Configs (Optional)
- If you're downloading Docker Desktop for the first time, check the option designed to **enable WSL 2** during the installation

After downloading Docker Desktop, you should check if you have this feature enabled:
- In you BIOS, make sure that you have **virtualization enabled**

You can test if your Docker installation is working correctly running the following command on a Command Prompt:
```
docker run hello-world
```

## 3. Update your AWS credentials:
Inside the .env file, you need to fill out the following variables with the ones from your AWS account: 
```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```
(Since it's a private information that compromises the security of a AWS account, I've uploaded this project with a blank space in these fields )

On the .env file, I've also set the names for the three buckets from each layer:
```
BRONZE_LAYER=bronze-layer-naue-abinbev
SILVER_LAYER=silver-layer-naue-abinbev
GOLD_LAYER=gold-layer-naue-abinbev
```
But you can change them as you please, just make sure that it matches the name of the same bucket on S3

## 4. Run the Docker command:
On the git bash terminal located at the project's folder, run the command:
```
docker-compose up --build
```
This can take up to 3 minutes 

## 5. Open Airflow Webserver
The Airflow Webserver is located at: http://localhost:8080

(for default, the user and password for Airflow are **admin**)

Afterwards, in the Airflow UI you can run the DAG through the trigger button and see how it plays out 
Hopefully, all the tasks will be executed with *success* flag

In S3, you'll already be able to see the data in the three buckets

## 7. Stop and remove your containers:
After running the DAG, you'll want to discard the containers, and in order to do that, you'll need to run: 
```
docker-compose down
```

## 8. Go to the AWS Console
There, you'll be able to see the data inserted during the ETL process in the three S3 Buckets
You will also be able to query the data on S3'S gold layer through AWS Athena

If it's your first time using AWS Athena, you'll need to specify an S3 bucket. 

In the Settings section of the Athena Console, you will find an option to specify the Query result location. 

Clicking there, you 'll have to enter an S3 path where the results will be stored
- Enter the S3 bucket path (e.g., s3://gold-layer-naue-abinbev/athena/results/)

On the Athena console, you can run this query to get the results from the gold layer: 
```
SELECT * FROM "gold_layer_naue_abinbev_db"."curated_brewery_data" limit 50 
```


## Monitoring/Alerting

**Alerting:** I would implement an alerting process by setting up automated reports to core stakeholders for this data, informing then if the pipeline has failed or succeeded and how many time it spent in each steps.

**Pipeline Failures:** There are several error handling throughout the ETL pipeline with Try-Except blocks, but a way to improve this even further would be to apply a real-time monitoring tool, like Promotheus, for example.

**Data Quality Checks:** I've applied a series of tests to ensure that the schema received from that batch matches the expected schema and that there's no duplicated data. Still, there's plenty of room to improve the data quality of the whole proccess, such as implementing data checks for a substantial increase or decrease in data volume for that batch, for example. 

## Contact Information
Feel free to reach out to me on LinkedIn!

## [LinkedIn](https://www.linkedin.com/in/nauefelipe/) ##
