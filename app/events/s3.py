import os
import boto3
import logging

logger = logging.getLogger(__name__)

GLUE_CRAWLER_NAME = os.getenv('GLUE_CRAWLER_NAME', 'aml-graph-crawler')
GLUE_JOB_NAME = os.getenv('GLUE_JOB_NAME', 'aml-etl-job')


s3_client = boto3.client('s3')
glue_client = boto3.client('glue')


def file_received(event, context):
    """
    Process a file upload.
    """
    # Get the uploaded file's information
    # bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    if '.gz' in key:
        crawler_resp = glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
        logger.info('Crawler started: %s', crawler_resp)

        job_resp = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        logger.info('Job Run started: %s', job_resp)
