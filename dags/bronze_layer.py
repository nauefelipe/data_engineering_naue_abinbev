import os
import requests
import json
import boto3
from datetime import datetime
import logging
from variables import bronze_bucket, api_url, aws_access_key, aws_secret_key

class BronzeLayer:
    """
    In this first step of the pipeline, the script will retrieve the raw data from the 
    API URL and then store in S3's bronze layer
    """
    def __init__(self, bronze_file, bucket = bronze_bucket, fetch_api_url = api_url):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.bronze_file = bronze_file
        self.bucket = bucket
        self.fetch_api_url = fetch_api_url
        self.timestamp_batch = datetime.now().strftime('%Y_%m_%d')

    def fetch_data_from_API(self):
        """
        This function uses the requests library to get the json data from the Open Brewery DB API
        """
        try:
            response = requests.get(self.fetch_api_url)
            # It will only process the data whose request's has worked  
            if response.status_code == 200:
                raw_data = json.dumps(response.json())
                self.logger.info("the API request succeeded!!")
                return raw_data
            else:
                self.logger.error(f"Received a response different from 200: {response.status_code}")
                raise requests.exceptions.RequestException()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"There's been an error on the API request: {e}")
            raise e

    def upload_raw_data_to_S3(self, raw_data):
        """
        From the data extracted on the first function, we'll store it in S3
        Since this is the bronze layer, the data will be stored on its raw structure (a JSON file)
        """
        try:
            s3 = boto3.client('s3')
            # Taking into account that the bronze stores raw data, I've added a timestamp_batch tag in the file name
            # So past information on the bronze layer can be retrieved 
            s3_key = f"{self.bronze_file}_{self.timestamp_batch}.json"

            # Since this project uses AWS, S3 Storage Service will act as the datalake's foundation
            s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(raw_data))
            self.logger.info('Raw data inserted to S3')

            # In case there's any issue during the insertion, this exception will be raised and the DAG's task will
            # be tagged as failed
        except Exception as e:
            self.logger.error(f"The S3 upload has failed: {str(e)}")
            raise e

if __name__ == "__main__":
    
    # Instantiating the class
    bronze_layer = BronzeLayer(bronze_file = 'raw_brewery_data', fetch_api_url = api_url)
    
    try:
        json_file = bronze_layer.fetch_data_from_API()
        # Since the fetch_data_from_API function returns the data, 
        # we'll pass it as the upload_raw_data_to_S3's argument
        bronze_layer.upload_raw_data_to_S3(json_file)
    except Exception as e:
        bronze_layer.logger.error(f"There's been an error during the bronze layer: {e}")
        raise e
