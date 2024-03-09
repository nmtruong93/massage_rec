import os

import boto3
from config.config import settings


def create_session(profile_name=None):
    session = boto3.Session(profile_name=profile_name)
    if session.region_name != os.getenv("AWS_REGION"):
        session = boto3.session.Session(
            region_name=os.getenv("AWS_REGION"),
        )
    return session


def connect_to_s3_client(profile_name=None):
    """
    Connect to s3

    :param profile_name: profile name in ~/.aws/credentials
    :return: object, S3 connection
    """
    session = create_session(profile_name=profile_name)
    s3_client = session.client("s3")
    return s3_client


def connect_to_s3_resource(profile_name=None):
    """
    Connect to s3 resource

    :param profile_name: profile name in ~/.aws/credentials
    :return: object, S3 resource
    """
    session = create_session(profile_name=profile_name)
    s3_resource = session.resource(
        "s3",
        region_name=os.getenv("AWS_REGION"),
    )

    return s3_resource


def connect_to_s3_bucket(s3_bucket_name, profile_name=None):
    """
    Connect to s3 bucket

    :param profile_name: profile name in ~/.aws/credentials
    :param s3_bucket_name:
    :return: object, S3 bucket
    """
    s3_resource = connect_to_s3_resource(profile_name=profile_name)

    return s3_resource.Bucket(s3_bucket_name)


def connect_to_sagemaker_client(profile_name=None):
    """
    Connect to sagemaker
    :param profile_name: profile name in ~/.aws/credentials
    :return: object, sagemaker connection
    """

    session = create_session(profile_name=profile_name)
    sagemaker_client = session.client("sagemaker")

    return sagemaker_client


def connect_to_sagemaker_runtime(profile_name=None):
    """
    Connect to sagemaker runtime
    :param profile_name: profile name in ~/.aws/credentials
    :return: object, sagemaker runtime connection
    """

    session = create_session(profile_name=profile_name)
    sagemaker_runtime_client = session.client("runtime.sagemaker")

    return sagemaker_runtime_client


def connect_to_iam_resource(profile_name=None):
    """
    Connect to iam
    :param profile_name: profile name in ~/.aws/credentials
    :return: object, iam connection
    """

    session = create_session(profile_name=profile_name)
    iam_client = session.resource("iam")

    return iam_client


def connect_to_personalize(profile_name=None):
    """
    Connect to personalize
    :param profile_name: profile name in ~/.aws/credentials
    :return: object, personalize connection
    """

    session = create_session(profile_name=profile_name)
    personalize = session.client("personalize")

    return personalize


def connect_to_personalize_runtime(profile_name=None):
    """
    Connect to personalize runtime
    :param profile_name: profile name in ~/.aws/credentials
    :return: object, personalize runtime connection
    """

    session = create_session(profile_name=profile_name)
    personalize_runtime = session.client("personalize-runtime")

    return personalize_runtime
