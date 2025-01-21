import os
import json
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException
import logging
import boto3
import time
from variables import bronze_bucket, silver_bucket, aws_access_key, aws_secret_key, aws_region

class SilverLayer:
    """
    In the Silver Layer, we'll be retrieving the data inserted in the bronze layer
    and perform a series of transformations in order to make the ready for consumption 
    """
    def __init__(self, bronze_table, silver_table, partition):
        """Initializing spark"""
        self.spark = SparkSession.builder \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .getOrCreate()
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.partition = partition
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Spark session created!")

    def get_data_from_bronze(self):
        """
        This function fetches data inserted in the bronze layer and stored it as a Datframe
        """
        self.logger.info("Transforming data...")
        timestamp_batch = datetime.now().strftime('%Y_%m_%d')
        try:
            input_path = f"s3a://{bronze_bucket}/{self.bronze_table}_{timestamp_batch}.json"
            
            # Reading the raw json inserted in S3 on the bronze layer job
            df = self.spark.read.text(input_path)
            self.logger.info("Data retrieved successfully!")
            json_treated = "".join([row["value"] for row in df.collect()])
            parsed_json = json.loads(json_treated)
            rdd = self.spark.sparkContext.parallelize([parsed_json])
            df = self.spark.read.json(rdd)
            return df
        
        except Exception as e:
            self.logger.error(f"Faced the following error: {e}")
            raise e
        
    def checking_column_schema(self, df):
        right_columns = ["address_1", "address_2", "address_3", "brewery_type", "city", "country", "id", 
                   "latitude", "longitude", "name", "phone", "postal_code", "state", "state_province", 
                   "street", "website_url"]
        if [x for x in df.columns] == right_columns:
            self.logger.info("Column schema validation passed.")
        else:
            raise ValueError(f"Column schema mismatch")
        
    def treat_duplicated_ids(self, df):
        """
        This function checks if the 'id' column has any duplicates
        Since it's an id, It's suppose to be a unique identifier
        """
        window_spec = Window.partitionBy("id")
        # Creates a dummy dataframe with a count column on it 
        df_with_counts = df.withColumn("id_count", F.count("id").over(window_spec))
        # Filter for duplicates
        duplicates = df_with_counts.filter(F.col("id_count") > 1)
        # Count the duplicates found, if any
        duplicate_count = duplicates.count()
        
        if duplicate_count > 0:
            # Removing duplicates
            df_unique = df.dropDuplicates(["id"])
            self.logger.info("Duplicates have been removed from the DataFrame.")
            return df_unique
        else:
            return df
            
    def transforming_data(self, df):
        """
        Transforms the input DataFrame by concatenating address, converting data types and setting a partition column
        """
        
        try:
            # I've decided to concatenate the three address columns into a single one, 
            # in order to make our data less redundant

            # Since this data comes overwhelmingly from the US, I chose to partition it by state
            # Since a partition by Country would lead to a big difference in size among partitions
            
            df = df.withColumn("address",
                F.concat_ws(", ",
                F.col("address_1"),
                F.when(F.col("address_2").isNotNull() & (F.length(F.col("address_2")) > 0), F.col("address_2")),
                F.when(F.col("address_3").isNotNull() & (F.length(F.col("address_3")) > 0), F.col("address_3"))))
        
        except AnalysisException as e:
                self.logger.error(f"Error concatenating address columns: {str(e)}")
                raise e
        
        # After creating the new column, I'm dropping the old ones
        df = df.drop("address_1", "address_2", "address_3")

        # Rearraging the dataframe's column so 'address' becomes the first
        df = df.select("address", *[col for col in df.columns if col != "address"])

        try:
        # Changing the data type for the geodata columns from string to double
            df = df.withColumn("latitude", F.col("latitude").cast("double")) \
                .withColumn("longitude", F.col("longitude").cast("double"))
            
        except AnalysisException as e:
            self.logger.error(f"Error converting latitude/longitude to double: {str(e)}")
            raise e

        # Correcting some columns where the first chraracter doens't start with a capital letter
        cols_uppercase = ['brewery_type', 'city']
        for column in cols_uppercase:
            try:
                df = df.withColumn(column, F.initcap(F.col(column)))
            except AnalysisException as e:
                raise e
            
        # Since the column chosen as partition (state_province) has spaces on states that are made up by two words,
        # I've decided to replace the spaces for _
        try:
            df = df.withColumn(f"{self.partition}",
                F.regexp_replace(F.col(f"{self.partition}"), " ", "_"))
        except AnalysisException as e:
            raise e

        self.logger.info("Data transformed successfully!")
        return df
    
    def inserting_silver_layer_data(self, df):
        """
        After the data being transformed on the previous function, it'll be inserted in the silver layer of the datalake.
        
        I chose the delta format since it takes care of metadata handling, enables features as time travel and 
        has a high level performance for large datasets, aiming to achieve scalability if the data becomes too big
        This ensures scalability for future growth, even though the current dataset from the API is relatively small
        """
        # Setting the silver layer's path in S3
        output_path = f"s3a://{silver_bucket}/{self.silver_table}"
        
        self.logger.info(f"Inserting data to S3 ({output_path}) ...")
        try:
            # Exporting the data in the delta table format
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy(f"{self.partition}") \
                .save(output_path)
           
            # Marking success
            self.logger.info(f"Data insertion to ({output_path}) was completed ")
        except Exception as e:
            # Logging this error and marking the task as failed
            self.logger.error(f"Error while inserting data to the silver layer: {e}")
            raise e
        
    def vacuum_silver_layer(self):
        """
        This function is designed to remove old data from the silver layer, keeping only the new one
        It only keeps the data that is referenced in the delta table logs in S3 
        """
        output_path = f"s3a://{silver_bucket}/{self.silver_table}"
        self.logger.info(f"Running VACUUM on ({output_path}) ...")
        try:
            # Runing the VACUUM command
            self.spark.sql(f"VACUUM delta.`{output_path}` RETAIN 0 HOURS")
            self.logger.info(f"VACUUM operation on ({output_path}) was completed ")
        except Exception as e:
            self.logger.error(f"Error while cleaning up data on the silver layer: {e}")
            raise e
        

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Instantiating the class
    silver_layer = SilverLayer(bronze_table = 'raw_brewery_data',
                        silver_table = 'processed_brewer_data',
                        partition = 'state_province')
    df = silver_layer.get_data_from_bronze()
    try:
        silver_layer.checking_column_schema(df)
        silver_layer.treat_duplicated_ids(df)
    except Exception as e:
        raise e
    if df is not None:
        transformed_df = silver_layer.transforming_data(df)
        silver_layer.inserting_silver_layer_data(transformed_df)
        silver_layer.vacuum_silver_layer()

    # In case the dataframe comes empty, it'll raise an exception and the task will be marked as failed
    else:
        logger.error("The dataframe doesn't exist. Exiting the process.")
        raise ValueError("No data retrieved from the bronze layer.")