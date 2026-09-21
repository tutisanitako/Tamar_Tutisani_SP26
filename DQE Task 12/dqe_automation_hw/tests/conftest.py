import pytest
import boto3
import requests
from botocore import UNSIGNED
from botocore.config import Config
from google.cloud import storage


@pytest.fixture(scope='session')
def base_url():
    return "https://jsonplaceholder.typicode.com"


@pytest.fixture(scope='function')
def provide_posts_data(base_url):
    """Fetch posts for user 3 from JSONPlaceholder."""
    response = requests.get(f"{base_url}/posts", params={"userId": 3})
    response.raise_for_status()
    return response.json()


@pytest.fixture(scope='function')
def provide_config():
    config = {
        'gcp_prefix': '2015/05/06/KTLX/',
        'aws_prefix': 'csv/by_year/',
        'gcp_bucket_name': "gcp-public-data-nexrad-l2",
        'aws_bucket_name': 'noaa-ghcn-pds',
        's3_anon_client': boto3.client(
            's3',
            region_name='us-east-1',
            config=Config(signature_version=UNSIGNED)
        ),
        'gcp_storage_anon_client': storage.Client.create_anonymous_client()
    }
    return config


@pytest.fixture(scope='function')
def list_gcs_blobs(provide_config):
    config = provide_config
    blobs = config['gcp_storage_anon_client'].list_blobs(
        config['gcp_bucket_name'], prefix=config['gcp_prefix']
    )
    return [blob.name for blob in blobs]


@pytest.fixture(scope='function')
def list_aws_blobs(provide_config):
    config = provide_config
    response = config['s3_anon_client'].list_objects_v2(
        Bucket=config['aws_bucket_name'],
        Prefix=config['aws_prefix']
    )
    return [content['Key'] for content in response.get('Contents', [])]