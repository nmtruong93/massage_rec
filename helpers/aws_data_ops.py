import io
import os
import pandas as pd

from config.config import settings
from config.log_config import logger
from helpers.connection import create_session


def read_data_file_from_s3(bucket_object, s3_file_name, is_parquet=True):
    """
    Read data file from s3 bucket

    :param bucket_object: object, boto3 s3 bucket object
    :param s3_file_name: str, the s3 file name
    :param is_parquet: bool, whether the file is parquet or csv
    :return: pd.DataFrame, the dataframe of the file
    """

    obj = bucket_object.Object(s3_file_name)
    data = obj.get()["Body"].read()
    if is_parquet:
        return pd.read_parquet(io.BytesIO(data))

    logger.info(f"Data read from {os.path.join(bucket_object.name, s3_file_name)}")
    return pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)


def upload_file_to_s3(s3_client, file_name, bucket_name, object_name=None):
    """
    Upload file to s3 bucket

    :param s3_client: object, boto3 s3 client object
    :param file_name: str, the file name
    :param bucket_name: str, boto3 s3 bucket name
    :param object_name: str, the object name in s3 bucket, default None
    :return:
    """
    if object_name is None:
        object_name = file_name

    s3_client.upload_file(file_name, bucket_name, object_name)
    logger.info(f"File {file_name} uploaded to {bucket_name}/{object_name}")


def put_object_to_s3(s3_client, bucket_name, object_name, data):
    """
    Put object to s3 bucket

    :param s3_client: object, boto3 s3 client object
    :param bucket_name: str, boto3 s3 bucket name
    :param object_name: str, the object name in s3 bucket
    :param data: Any, the data to upload
    :return:
    """
    s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
    logger.info(f"Object {object_name} uploaded to {bucket_name}")


def upload_folder_to_s3(s3_client, local_folder_path, bucket_name):
    """
    Upload folder to s3 bucket

    :param s3_client: object, boto3 s3 client object
    :param local_folder_path: str, the local folder path
    :param bucket_name: str, boto3 s3 bucket name
    :return:
    """
    for subdir, dirs, files in os.walk(local_folder_path):
        for file in files:
            if file.startswith("."):
                continue
            full_path = os.path.join(subdir, file)
            upload_file_to_s3(
                s3_client,
                full_path,
                bucket_name,
                full_path[len(local_folder_path) + 1:],
            )

    logger.info(f"Folder {local_folder_path} uploaded to {bucket_name}")


def create_bucket(bucket_name, profile_name=None):
    """
    Create a bucket with a prefix

    :param profile_name: str, the profile name in ~/.aws/credentials
    :param bucket_name: str, the name of the bucket
    :return: object, boto3 s3 bucket object
    """

    session = create_session(profile_name=profile_name)
    region = session.region_name
    s3_client = session.client("s3", region_name=region)

    # Check if bucket exist
    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        if bucket["Name"] == bucket_name:
            logger.info(f"Bucket {bucket_name} already exists in {region} region")
            return bucket_name

    s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}
    )

    logger.info(f"Bucket {bucket_name} created in {region} region")
    return bucket_name
