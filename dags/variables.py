import os
from dotenv import load_dotenv, find_dotenv
from os.path import dirname, abspath

# Load environment variables from .env file
load_dotenv(find_dotenv())

# Define variables
bronze_bucket = os.getenv('BRONZE_LAYER')
silver_bucket = os.getenv('SILVER_LAYER')
gold_bucket = os.getenv('GOLD_LAYER')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
api_url = os.getenv('API_URL')
