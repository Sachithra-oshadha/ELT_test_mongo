import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 27017)),
    'database': os.getenv('DB_NAME', 'load_profiles_db'),
    'username': os.getenv('DB_USERNAME', None),
    'password': os.getenv('DB_PASSWORD', None)
}

S3_CONFIG = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'region_name': os.getenv('AWS_REGION')
}

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_BUCKET_PREFIX = os.getenv('S3_BUCKET_PREFIX', '')

REQUIRED_ENV_VARS = {
    'DB': ['DB_NAME', 'DB_HOST', 'DB_PORT'],
    'S3': ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'S3_BUCKET_NAME']
}

OUTPUT_BASE_DIR = "customer_outputs_bilstm_day"