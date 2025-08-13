"""
data_loader.py

This module provides utilities for handling data files and managing AWS S3 buckets.
Functions include:
- Listing local data files ../data/*
- Creating S3 buckets in a specified region
- Generating unique bucket names
- Uploading data files to S3 buckets


Usage:
    Import the module and call the desired functions.
"""


import glob 
import boto3
import uuid
import os
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
import os

load_dotenv()
config = TransferConfig(multipart_threshold=1024 * 1024 * 25, max_concurrency=10,
                        multipart_chunksize=1024 * 1024 * 25, use_threads=True) # 25 Mb file

def get_data_files():
    """List all data files in the local 'data' directory"""
    return glob.glob('./data1/*')

def create_bucket(bucket_name, region='eu-west-2'):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 
    default region (eu-east-2).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'eu-west-2'
    :return: boto s3 client
    """
    #Create Bucket
    try:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)   
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f'Bucket {bucket_name} already exists and is owned by you')
        return s3_client
    except Exception as e:
        print(e)
        raise e
    
    print(f'Bucket {bucket_name} created')
    return s3_client
def generate_unique_bucket_name(prefix='data-bucket-properties'):
    """Generate a unique bucket name with a specified prefix"""
    return f'{prefix}-{str(uuid.uuid4())[:8]}'

def get_bucket_name(): 
    """Get the bucket name from environment variables or generate a new one"""
    if os.getenv('BUCKET_NAME') is None:
        bucket_name = generate_unique_bucket_name()
        with open('.env', 'a') as env_file:
            env_file.write(f'BUCKET_NAME={bucket_name}\n')
    else:
        bucket_name = os.getenv('BUCKET_NAME')
    return bucket_name

class ProgressPercentage:
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percentage = (self._seen_so_far / self._size) * 100
        print(f"\r{self._filename}: {percentage:.1f}% ({self._seen_so_far}/{int(self._size)} bytes)", end='')
        if self._seen_so_far == self._size:
            print()  # New line when complete

def upload_data(data, bucket_name, s3):
    """Upload data files to the specified S3 bucket with progress
    :param data: List of file paths to upload
    :param bucket_name: Name of the S3 bucket to upload to
    """
    uploaded_files = []
    try:
        for file in data:
            progress = ProgressPercentage(file)
            s3.upload_file(file, bucket_name, os.path.basename(file), 
                          Config=config, Callback=progress)
            uploaded_files.append(os.path.basename(file))
    except Exception as e:
        print(f'Error uploading files: {e}')
        raise e
    return uploaded_files

if __name__=='__main__':
    bucket_name = get_bucket_name()
    s3 = create_bucket(bucket_name)
    data = get_data_files()
    print(data)
    for file in upload_data(data, bucket_name, s3):
        print(f'Uploaded {file}')
    