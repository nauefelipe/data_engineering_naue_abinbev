#!/bin/bash
set -e

# This is a script to ensure that airflow will only try to connect to Postgres when it's ready
# Wait for PostgreSQL to be ready
while ! pg_isready -h postgres -p 5432 -U airflow; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5
done

# Initialize the Airflow metadata database
airflow db init

# Create the admin user if it doesn't already exist
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start the Airflow webserver
airflow webserver & 
exec airflow scheduler