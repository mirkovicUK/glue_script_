import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql import DataFrame




def get_files_from_s3(bucket_name, s3, logger=None):
    """Retrieve a list of files from an S3 bucket.
    Args:
        bucket_name (str): The name of the S3 bucket.
        s3 (boto3.client): The S3 client to use for accessing the bucket.
        logger (Logger, optional): Logger for logging messages. Defaults to None.
    Returns:
        list: List of file names in the S3 bucket."""
    try:
        # List objects in the specified S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
    except Exception as e:
        if logger:
            logger.error(f"Error accessing bucket {bucket_name}: {e}")
        else:
            # If no logger is provided, print the error
            print(f"Error accessing bucket {bucket_name}: {e}")
        return []
    files = []
    if 'Contents' in response:
        for item in response['Contents']:
            files.append(item['Key'])
    return files

def partition_files_by_extension(files):
    """Partition a list of files by their extension.
    Args:
        files (list): List of file names.
    Returns:
        tuple: Two lists, one for CSV files and one for XLSX files.
    """
    csv_files = []
    xlsx_files = []
    for file in files:
        if file.endswith('.csv'):
            csv_files.append(file)
        elif file.endswith('.xlsx'):
            xlsx_files.append(file)
    return csv_files, xlsx_files

def load_to_dyf(files, glueContext, s3_bucket, logger=None):
    """Load files into a DynamicFrame.
    Args:
        files (list): List of file names to load.
        glueContext (GlueContext): The Glue context for creating DynamicFrames.
        logger (Logger, optional): Logger for logging messages. Defaults to None.
    Returns:
        list: List of DynamicFrames created from the files.
    Raises:
        ValueError: If no files are loaded into DynamicFrames.
    """
    dyf_list = []
    for file in files:
        try:
            # print(glueContext,'<--->')
            dyf = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                format="csv",
                connection_options={"paths": [f"s3://{s3_bucket}/{file}"]},
                format_options={"withHeader": True}
            )
            dyf_list.append(dyf)
            logger.info(f"Loaded file {file} into DynamicFrame")
        except Exception as e:
            if logger:
                logger.error(f"Error loading file {file}: {e}")
            else:
                print(f"Error loading file {file}: {e}")

    if not dyf_list:
        if logger:
            logger.error("No files were loaded into DynamicFrames.")
        else:
            print("No files were loaded into DynamicFrames.")
        raise ValueError("No files were loaded into DynamicFrames.")
    return dyf_list

def consistent_schema(dyf_list):
    """Check if all DynamicFrames in the list have the same schema.
    Args:
        dyf_list (list): List of DynamicFrames to check.
    Returns:
        bool: True if all DynamicFrames have the same schema, False otherwise.
    """
    if not dyf_list:
        return True  # Empty list is considered consistent
    first_schema = dyf_list[0].schema()
    for dyf in dyf_list[1:]:
        if dyf.schema() != first_schema:
            return False
    return True

def transform_data(dyf_list, logger=None):
    """Transform the data in the DynamicFrames.
    Args:
        dyf_list (list): List of DynamicFrames to transform.
        spark (SparkSession): The Spark session for DataFrame operations.
        logger (Logger, optional): Logger for logging messages. Defaults to None.
    Returns:
        tuple: Two DataFrames, one for houses and one for flats.
    """
    # Convert to DataFrames
    df_list = []
    for dyf in dyf_list:
        df_list.append(dyf.toDF())
    # Cast total_floor_area_known and total_floor_area to appropriate types
    df_list = cast_flor_area(df_list)
    
    

def cast_flor_area(df_list:list[DataFrame]) -> list[DataFrame]:
    """Cast total_floor_area_known and total_floor_area to appropriate types.
    Args:
        df_list (list): List of DataFrames to transform.
    Returns:
        list: List of transformed DataFrames.
    """
   #cast total_floor_area_known, total_floor_area to float
    cast_df_list = []
    for df in df_list:
        # Filter out rows with null total_floor_area_known
        df = df.filter(F.col('total_floor_area_known').isNotNull())\
        .filter(F.col('total_floor_area').isNotNull())
        df = df.withColumns({
            'total_floor_area_known': F.col('total_floor_area_known').cast(IntegerType()),
            'total_floor_area': F.col('total_floor_area').cast(FloatType())
        })
        cast_df_list.append(df)
    return cast_df_list
    


if __name__ == "__main__":
    params = ['BUCKET_NAME']
    if '--JOB_NAME' in sys.argv:
        params.append('JOB_NAME')
    args = getResolvedOptions(sys.argv, params)

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    if 'JOB_NAME' in args:
        jobname = args['JOB_NAME']
    else:
        jobname = "police_data_job"
    job.init(jobname, args)

    #get logger for this glue job
    logger = glueContext.get_logger()
    logger.info(f"Job {jobname} started with args: {args}")

    # Get the S3 bucket name from the arguments
    s3_bucket = args['BUCKET_NAME']
    s3_client = boto3.client('s3')

    
    # Retrieve csv data files from the S3 bucket
    logger.info(f"Retrieving files from S3 bucket: s3://{s3_bucket}")
    files = get_files_from_s3(s3_bucket, s3_client, logger)
    csv_files, xlsx_files = partition_files_by_extension(files)

    #load CSV files into DynamicFrames
    dyf_list = load_to_dyf(csv_files, glueContext, s3_bucket, logger)
    logger.info(f"Loaded {len(dyf_list)} DynamicFrames from CSV files.")

    # Check if all DynamicFrames have consistent schema and apply transformation
    if consistent_schema(dyf_list):
        logger.info("All DynamicFrames have the same schema.")
        dyf_houses, dyf_flats = transform_data(dyf_list, logger)
    else:
        logger.error("DynamicFrames do not have consistent schemas.")
        raise ValueError("Unable to transform data, inconsistent schemas.")
    
