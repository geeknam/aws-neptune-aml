import os
import boto3
import requests
import json
import logging

logger = logging.getLogger(__name__)

NEPTUNE_LOADER_ENDPOINT = os.getenv('NEPTUNE_LOADER_ENDPOINT', '')
S3_DATA_BUCKET_NAME = os.getenv('S3_DATA_BUCKET_NAME', '')
S3_LOADER_ROLE = os.getenv('S3_LOADER_ROLE', '')

glue_client = boto3.client('glue')


def job_succeeded(event, context):
    """
    Trigger Neptune loader when Glue job is successful
    """

    payload = {
        "source": "s3://%s/transformed" % S3_DATA_BUCKET_NAME,
        "format": "csv",
        "iamRoleArn": S3_LOADER_ROLE,
        "region": "us-east-1",
        "failOnError": "false"
    }
    response = requests.post(
        NEPTUNE_LOADER_ENDPOINT,
        data=json.dumps(payload),
        headers={
            'Content-Type': 'application/json'
        }
    ).json()
    logger.info(response)
