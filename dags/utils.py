import logging
import boto3
import json 


logger = logging.Logger(__name__)


def get_secret(secret_name, region_name = 'ap-south-1'):
    """ Retrives secrets from AWS secret manager"""
    session = boto3.session.Session()
    client = session.client(service_name = 'secretsmanager', region_name = region_name)
    try:
        response = client.get_secret_value(SecretId = secret_name)
        secret_str = response['SecretString']
        return json.loads(secret_str)
    except Exception as e:
        logger.error(f"Secret retrieval error: {e}")
        raise

