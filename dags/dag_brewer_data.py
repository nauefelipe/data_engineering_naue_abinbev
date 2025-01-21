from airflow import DAG
from docker.types import Mount
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(

    dag_id="DAG_brewer_data",
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    # Bash task to build a Pyspark image containing all libraries and configs required
    build_image = BashOperator(
        task_id="build_docker_image",
        bash_command="docker build -f /app/dockerfile_pyspark -t custom-pyspark:latest /app",
        env={
        "HADOOP_AWS_VERSION": "3.3.4",
        "AWS_SDK_VERSION": "1.12.524",
    },
    )

    # Before the ETL pipeline starts, docker will run a test script to check if the functions 
    # are behaving accordingly
    tests = DockerOperator(
        task_id="tests",
        image="custom-pyspark:latest",
        command="pytest -s /app/dags/tests.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )
    
    # First ETL Task: Extract data from the API and load it to S3 in its raw form
    bronze_layer = DockerOperator(
        task_id="bronze_layer",
        image="custom-pyspark:latest",
        command="spark-submit /app/dags/bronze_layer.py",
        docker_url="unix://var/run/docker.sock",
        auto_remove=False,
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # Second ETL Task: Transform the extracted data and S3 and store it on S3's silver layer
    silver_layer = DockerOperator(
        task_id="silver_layer",
        image="custom-pyspark:latest",
        command="spark-submit /app/dags/silver_layer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # Third ETL Task: Using the data treted on the second task, 
    # It'll be created an aggregated view and loaded into S3's gold layer
    gold_layer = DockerOperator(
        task_id="gold_layer",
        image="custom-pyspark:latest",
        command="spark-submit /app/dags/gold_layer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # Setting the job order (It will build the image, run the test and then run the three layers)
    build_image >> tests >> bronze_layer >> silver_layer >> gold_layer
    