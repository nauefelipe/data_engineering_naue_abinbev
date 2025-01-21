import pytest
from pyspark.sql import SparkSession, Row

# File for tests configuration

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("Testing Layers") \
        .getOrCreate()  
    yield spark
    spark.stop()

@pytest.fixture
def dummy_data(spark):
    # Some dummy data with column 'address_3' missing
    dummy_data = [Row(address_1="Av Faria Lima", address_2="456", brewery_type="micro", city="Sao Paulo", country="Brazil",
                id=1, latitude="-23.5667702", longitude="-46.6938257", name="Brewery 1", phone="123-456-7890",
                postal_code="04538-132", state="Sao Paulo", state_province="Sao Paulo", street="Av Faria Lima", 
                website_url="http://brewery1.com"),
                
                Row(address_1="Av Faria Lima", address_2="456", brewery_type="micro", city="Sao Paulo", country="Brazil",
                id=1, latitude="-23.5667702", longitude="-46.6938257", name="Brewery 1", phone="123-456-7890",
                postal_code="04538-132", state="Sao Paulo", state_province="Sao Paulo", street="Av Faria Lima", 
                website_url="http://brewery1.com")]
    dummy_data = spark.createDataFrame(dummy_data)
    return dummy_data

@pytest.fixture
def correct_data(spark):
    # Some dummy data, but this time all checks out
    correct_data = [Row(address_1="Av Faria Lima", address_2="456", address_3="", brewery_type="micro", city="Sao Paulo", country="Brazil",
                id=1, latitude="-23.5667702", longitude="-46.6938257", name="Brewery 1", phone="123-456-7890",
                postal_code="04538-132", state="Sao Paulo", state_province="Sao Paulo", street="Av Faria Lima", 
                website_url="http://brewery1.com")]
    correct_data = spark.createDataFrame(correct_data)
    return correct_data