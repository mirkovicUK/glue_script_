import pytest
import boto3
from moto import mock_aws
from awsglue.context import GlueContext
from pyspark.context import SparkContext

@pytest.fixture(scope="session")
def spark_context():
    sc = SparkContext.getOrCreate()
    yield sc
    sc.stop()

@pytest.fixture(scope="session") 
def glueContext(spark_context):
    yield GlueContext(spark_context)

@pytest.fixture(scope="session")
def df_data(glueContext):
    """Fixture to provide 3 spark df for testing.
    first 2 have same schema and third has different schema.
    Returns:
        list: List of df.
    """
    # First two DataFrames with same schema
    data1 = [{'id': 1, 'name': 'Alice', 'total_floor_area': '100',
              'total_floor_area_known': '1','uprn': '12345', 'property_type': 'flat'},
             {'id': 2, 'name': 'Bob', 'total_floor_area': '150',
              'total_floor_area_known': '1', 'uprn': '12345', 'property_type': 'house'}]
    
    data2 = [{'id': 3, 'name': 'Charlie', 'total_floor_area': '44',
              'total_floor_area_known': '0', 'uprn': '1234567', 'property_type': 'flat'},
             {'id': 4, 'name': 'David', 'total_floor_area': '17',
              'total_floor_area_known': '0', 'uprn': '123456', 'property_type': 'house'}]
    
    # Third DataFrame with different schema
    data3 = [{'user_id': 1, 'email': 'alice@test.com'}]

    # Create Spark DataFrames
    spark = glueContext.spark_session
    df1 = spark.createDataFrame(data1)
    df2 = spark.createDataFrame(data2)
    df3 = spark.createDataFrame(data3)  

    return [df1, df2, df3]