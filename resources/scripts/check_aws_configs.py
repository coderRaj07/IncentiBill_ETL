# Checking boto3 automatically loads aws credentials from .env
# We don't have to explicitly pass through functions manually
import boto3
from resources.dev import config # If this is commented then boto3 will be unable to detect aws creds
session = boto3.Session()
credentials = session.get_credentials()

# Print access key and session token to check the source
print("Access Key:", credentials.access_key)
print("Region:", session.region_name)

# Check the AWS credentials file path and config path being used
print("AWS Credentials file:", session.get_credentials().method)