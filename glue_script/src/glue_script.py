import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3



def get_files_from_s3(bucket_name, s3):
    """Retrieve a list of files from an S3 bucket."""
    try:
        # List objects in the specified S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
    except Exception as e:
        print(f"Error accessing bucket {bucket_name}: {e}")
        # Return an empty list if the bucket cannot be accessed
        return []
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = []
    if 'Contents' in response:
        for item in response['Contents']:
            files.append(item['Key'])
    return files


if __name__ == "__main__":
    param = ['BUCKET_NAME']
    if '--JOB_NAME' in sys.argv:
        param.append('JOB_NAME')
    args = getResolvedOptions(sys.argv, param)

    sc = SparkContext().getOrCreate()
    GlueContext = GlueContext(sc)
    spark = GlueContext.spark_session
    job = Job(GlueContext)
    job.init(args['JOB_NAME'], args)

    s3_bucket = args['BUCKET_NAME']
    s3_client = boto3.client('s3')
    # Retrieve files from the specified S3 bucket
    files = get_files_from_s3(s3_bucket, s3_client)
