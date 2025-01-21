import pytest
import os, sys
import logging
import requests
from unittest.mock import patch, MagicMock
from variables import api_url

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))
from bronze_layer import BronzeLayer
from silver_layer import SilverLayer

# ------------ TESTING THE BRONZE LAYER ------------

@pytest.fixture
def bronze_layer():
    bronze_class = BronzeLayer(bronze_file='raw_brewery_data', fetch_api_url = api_url)
    bronze_class.logger = MagicMock()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return bronze_class

# Here we'll be mocking the code's response in case the API request fails
def test_fetch_data_from_API_request_exception(bronze_layer):
    with patch('requests.get') as mock_get:
        # Simulate a RequestException
        mock_get.side_effect = requests.exceptions.RequestException("API request failed")

        # Use pytest.raises to catch the specific RequestException
        with pytest.raises(requests.exceptions.RequestException):
            bronze_layer.fetch_data_from_API()

# ------------ TESTING THE SILVER LAYER ------------

@pytest.fixture
def silver_layer():
    return SilverLayer(bronze_table = 'raw_brewery_data',
                        silver_table = 'processed_brewer_data',
                        partition = 'state_province')

# The dummy data used in these tests comes from the conftest.py file in the same /dags folder
# It spins up a spark session that will be used on this code and generates a dummy DataFrame

def test_columns_validation(silver_layer, dummy_data):
    """
    Here we'll be mocking the code's response in case the API request fails
    """
    with pytest.raises(ValueError) as e:
        # Applying the class's function to see if it will raise an exception
        silver_layer.checking_column_schema(dummy_data)


def test_duplicated_data(silver_layer, dummy_data):
    """
    Testing if the function that catches duplicated id's is working
    """
    with pytest.raises(ValueError) as e:
        # Applying the class's function to see if it will raise an exception
        silver_layer.check_unique_id(dummy_data)


def test_transformation(silver_layer, correct_data):
    """
    Testing if all the transformations have gone accordingly
    """

    transformed_df = silver_layer.transforming_data(correct_data)
    results = transformed_df.collect()

    # Testing if the addresses are being properly concatenated
    assert results[0]['address'] == "Av Faria Lima, 456"
        
    # Testing if "-23.5667702" has been converted to double
    assert results[0]['latitude'] == -23.5667702

    # Testing if micro has been corrected to Micro
    assert results[0]['brewery_type'] == "Micro"

    
