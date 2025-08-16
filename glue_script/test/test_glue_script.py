import unittest
import pytest
import logging
from unittest.mock import patch, MagicMock
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from src.glue_script import get_files_from_s3,\
    partition_files_by_extension, load_to_dyf, consistent_schema, cast_flor_area,\
    over_80_sqr_meters, transform_data, remove_duplicates,\
    concatinate_dataframes, get_flats_houses
from moto import mock_aws
import boto3


@mock_aws
def test_get_files_from_s3_retrieves_files_correctly():
    """Test the get_files_from_s3 function retrieves files from an S3 bucket correctly."""
    # Set up a mock AWS environment
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)

    # Add some mock files to the bucket
    s3.put_object(Bucket=bucket_name, Key='file1.txt', Body='Content of file 1')
    s3.put_object(Bucket=bucket_name, Key='file2.txt', Body='Content of file 2')

    files = get_files_from_s3(bucket_name, s3)
    # Assert that the files are retrieved correctly
    assert isinstance(files, list)
    assert len(files) == 2
    assert 'file1.txt' in files
    assert 'file2.txt' in files

def test_get_files_from_s3_return_empty_list():
    """Test the get_files_from_s3 function handles errors and returns an empty list."""
    # Test error handling when S3 access fails
    s3 = MagicMock()
    s3.list_objects_v2.side_effect = Exception("S3 access error")
    logger = MagicMock()
    
    files = get_files_from_s3('non-existent-bucket', s3, logger)
    
    logger.error.assert_called_once_with("Error accessing bucket non-existent-bucket: S3 access error")
    assert files == []

def test_partition_files_by_extension():
    """Test the partition_files_by_extension function."""
    files = ['file1.csv', 'file2.xlsx', 'file3.csv', 'file4.txt']
    csv_files, xlsx_files = partition_files_by_extension(files)
    
    assert csv_files == ['file1.csv', 'file3.csv']
    assert xlsx_files == ['file2.xlsx']

def test_load_to_dyf_return_corect_numb_dyf():
    """Test the load_to_dyf function to ensure it loads 
    files into DynamicFrames correctly."""    
    # Mock GlueContext and DynamicFrame
    mock_glue_context = MagicMock()
    mock_dyf = MagicMock()
    mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dyf
    s3_bucket = "test-bucket"
    
    mock_logger = MagicMock()
    files = ['file1.csv', 'file2.csv']
    
    # Test the function
    result = load_to_dyf(
        files,
        mock_glue_context,
        s3_bucket,
        mock_logger)
    
    # Assertions
    assert len(result) == 2
    assert mock_glue_context.create_dynamic_frame.from_options\
        .call_count == 2
    mock_logger.info.assert_called()

def test_load_to_dyf_raise_ValueError():

    mock_glue_context = MagicMock()
    mock_glue_context.create_dynamic_frame\
        .from_options.side_effect = Exception("S3 error")
    s3_bucket = "test-bucket"
    
    mock_logger = MagicMock()
    files = ['nonexistent.csv']
    
    with pytest.raises(ValueError, match="No files were loaded into DynamicFrames."):
        load_to_dyf(files, mock_glue_context, s3_bucket, mock_logger)
    
    mock_logger.error.assert_called()
    assert mock_logger.error.call_count == 2

def test_load_to_dyf_partial_failure():
    """Test load_to_dyf when first file fails, second succeeds"""
    mock_glue_context = MagicMock()
    mock_dyf = MagicMock()
    
    # First call raises exception, second call returns mock_dyf
    mock_glue_context.create_dynamic_frame.from_options.side_effect = [
        Exception("First file error"),
        mock_dyf
    ]
    
    mock_logger = MagicMock()
    files = ['bad_file.to_the_bone', 'good_file.csv']
    
    result = load_to_dyf(files, mock_glue_context, 'TestBucket', mock_logger)
    
    # Only one DynamicFrame should be created (second file)
    assert len(result) == 1
    assert result[0] == mock_dyf
    
    # Verify error was logged for first file
    mock_logger.error.assert_called_once_with("Error loading file bad_file.to_the_bone: First file error")
    mock_logger.info.assert_called_once_with("Loaded file good_file.csv into DynamicFrame")

def test_check_schema_consistency_true_and_false(df_data, glueContext):
    """Test the check_schema_consistency function."""

    dyf1 = DynamicFrame.fromDF(df_data[0], glueContext, "dyf1")
    dyf2 = DynamicFrame.fromDF(df_data[1], glueContext, "dyf2")
    dyf3 = DynamicFrame.fromDF(df_data[2], glueContext, "dyf3") 
    
    assert consistent_schema([dyf1, dyf2]) is True
    assert consistent_schema([dyf1, dyf2, dyf3]) is False

def test_cast_flor_area(df_data):
    """Test the cast_flor_area function."""    
    # Cast total_floor_area_known and total_floor_area to appropriate types
    df_list = [df_data[0], df_data[1]]
    casted_df_list = cast_flor_area(df_list)
    
    for df in casted_df_list:
        assert df.schema['total_floor_area_known'].dataType.typeName() == 'integer'
        assert df.schema['total_floor_area'].dataType.typeName() == 'float'

def test_over_80_sqr_meters(df_data):
    """Test the over_80_sqr_meters function."""
    # Cast total_floor_area_known and total_floor_area to appropriate types
    df_list = [df_data[0], df_data[1]]
    filtered_df_list = over_80_sqr_meters(df_list)
    
    assert len(filtered_df_list) == 1 # Only df1 should remain after filtering 
    for df in filtered_df_list:
        assert df.filter(col('total_floor_area') <= 80).count() == 0
        assert df.count() == 2

def test_remove_duplicates(df_data):
    """Test the remove_duplicates function."""
    # Create DynamicFrames from DataFrames
    
    # Remove duplicates based on 'uprn'
    df_list = [df_data[0], df_data[1]]
    filtered_df_list = remove_duplicates(df_list)
    
    assert len(filtered_df_list) == 2
    assert filtered_df_list[0].count() == 1
    assert filtered_df_list[1].count() == 2

def test_concatenate_dataframes(df_data):
    """Test the concatinate_dataframes function."""
    # Create DynamicFrames from DataFrames
    df_list = [df_data[0], df_data[1]]
    
    concatenated_df = concatinate_dataframes(df_list)
    
    assert concatenated_df.count() == 4  # 2 rows from each DataFrame
    assert set(concatenated_df.columns) == {'id', 'name', 'total_floor_area', \
                                            'total_floor_area_known', 'uprn', 'property_type'}

def test_get_flats_houses(df_data):
    """Test the get_flats_houses function."""
    # Create a DataFrame with mixed data
    df = df_data[0].union(df_data[1])
    
    flats, houses = get_flats_houses(df)
    
    assert flats.count() == 2  # All rows should be considered flats
    assert houses.count() == 2  # No houses in this test data
    assert set(flats.columns) == {'id', 'name', 'total_floor_area', \
                                  'total_floor_area_known', 'uprn', 'property_type'}