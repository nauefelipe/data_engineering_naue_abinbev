from pyspark.sql import SparkSession
from pyspark import SparkFiles
from datetime import datetime
from pathlib import Path
import pyspark.sql.functions as F
import logging
import boto3
from variables import bronze_bucket, silver_bucket, gold_bucket, aws_access_key, aws_secret_key, aws_region

# Configure logging once
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldLayer:
    """
    Class to manage the Gold Layer of the data pipeline.
    Handles database and table creation, data aggregation, and Athena integration.
    """
    def __init__(self, database, silver_table, gold_table):
        self.spark = SparkSession.builder \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .getOrCreate()

        self.database = database
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.silver_output_path = f"s3a://{silver_bucket}/{silver_table}"
        self.gold_output_path = f"s3a://{gold_bucket}/{gold_table}"
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        self.athena_client = self.session.client('athena')
        self.logger = logging.getLogger(__name__)
        logger.info("Spark session created!")

    def create_spark_database(self):
        """
        Creating a database based on the data inserted during the execution of the silver layer
        """
        try:
            silver_path = f"s3a://{silver_bucket}/{self.silver_table}"
            self.spark.sql(f"""
                CREATE DATABASE IF NOT EXISTS {self.database}
            """)

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.silver_table}
                USING DELTA
                LOCATION '{silver_path}'
            """)
            return self.logger.info(f"Database {self.database} and table {self.silver_table} created successfully.")
        
        except Exception as e:
            self.logger.error(f"Error creating database {self.database}: {e}")
            raise e

    def create_analytical_view(self):
        """
        Creating the aggregated view requested
        It'll be returned as a DataFrame that will be passed onto the next function
        """
        try:
            gold_df = self.spark.sql(f"""
                
            SELECT brewery_type, state, count(*) as brewer_quantity
                FROM {self.database}.{self.silver_table}
            GROUP BY brewery_type, state
            ORDER BY count(*) DESC,
            CASE 
                WHEN brewery_type = 'Large' THEN 1
                WHEN brewery_type = 'Brewpub' THEN 2
                WHEN brewery_type = 'Micro' THEN 3
                WHEN brewery_type = 'Proprietor' THEN 4
                WHEN brewery_type = 'Closed' THEN 5
            END

            """)
            return gold_df
        except Exception as e:
            logger.error(f"It got an error while querying data from {self.database}.{self.silver_table}: {e}")
            raise

    def inserting_gold_layer_data(self, gold_df):
        """Inserts the aggregated view into the data lake"""
        try:
            gold_df.write.format("delta").mode("overwrite").save(self.gold_output_path)
            logger.info("Gold-layer table created successfully!")
            
            # Storing the aggregated view in spark's metadata
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.gold_table}
            USING DELTA
            LOCATION '{self.gold_output_path}'
            """)

            # Displaying the aggregated view requested:
            self.spark.sql(f"SELECT * FROM {self.database}.{self.gold_table}").show(n=50)
            self.logger.info("Gold-layer table created successfully!")
        except Exception as e:
            self.logger.error(f"Error inserting data into gold layer: {e}")
            raise e

    def vacuum_golden_layer(self):
        """Cleans up data from past batches."""
        logger.info(f"Running VACUUM on ({self.gold_output_path}) ...")
        try:
            self.spark.sql(f"VACUUM delta.`{self.gold_output_path}` RETAIN 0 HOURS")
            logger.info(f"VACUUM operation on ({self.gold_output_path}) was completed.")
        except Exception as e:
            logger.error(f"Error while cleaning up data on the gold layer: {e}")
            raise

    def create_athena_database(self):
        """
        Now it creates a table with the gold-layer data on AWS Athena 
        so stakeholders can run it
        """
        try:
            query = f"CREATE DATABASE IF NOT EXISTS {gold_bucket.replace('-', '_')}_db;"
            self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': f's3://{gold_bucket}/athena/'
                }
            )
            logger.info("AWS Athena database created successfully!")
        except Exception as e:
            self.logger.error("There's been an error while trying to create the Athena database: {e}")
            raise e

    def create_athena_table(self, df):
        """
        Creates an Athena table based on the DataFrame's schema
        """
        try:
            schema = ", ".join([f"{x} {y}" for x, y in df.dtypes])
            query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {gold_bucket.replace('-', '_')}_db.{self.gold_table} (
                {schema}
            )
            STORED AS PARQUET
            LOCATION 's3://{gold_bucket}/{self.gold_table}';
            """
            self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': f"{gold_bucket.replace('-', '_')}_db"},
                ResultConfiguration={
                    'OutputLocation': f's3://{gold_bucket}/athena/results/'
                }
            )
            logger.info("AWS Athena table created successfully!")
        except Exception as e:
            self.logger.error("There's been an error while trying to create the Athena table: {e}")
            raise e

if __name__ == "__main__":
    # Instantiating the GoldLayer class
    gold_layer = GoldLayer(database="database_brewery_data",
                           silver_table="processed_brewer_data",
                           gold_table="curated_brewery_data")

    try:
        gold_layer.create_spark_database()
        gold_df = gold_layer.create_analytical_view()
        gold_layer.inserting_gold_layer_data(gold_df)
        gold_layer.vacuum_golden_layer()
        gold_layer.create_athena_database()
        gold_layer.create_athena_table(gold_df)
    except Exception as e:
        logger.error(f"An error occurred during the execution: {e}")