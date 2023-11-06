from S3Processor import S3Processor
from dynamoDBProcessor import dynamoDBProcessor
import boto3
import os
import logging

class credsManager():
    def __init__(self, sourceBucket, destinationBucket):
        self.sourceBucket = sourceBucket
        self.destinationBucket = destinationBucket 
        self.source_session = None
        self.dest_session = None
        try:
            self.creds = self.get_aws_creds()
        except Exception as e:
            logging.error(f"Error, could not find AWS Credentials in ~/.aws/credentials\n {e}")

        # Connect to the sources
        self.connect_to_sources()

        
    """
    Opens the .aws file located in the home directory and gets the keys
    """
    def get_aws_creds(self):
        # declare dict to return, and get the path of aws credentials file
        aws_creds = {}

        if 'AWS_ACCESS_KEY_ID' in os.environ:
            aws_creds['aws_access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']

        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            aws_creds['aws_secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']

        if 'AWS_SESSION_TOKEN' in os.environ:
            aws_creds['aws_session_token'] = os.environ['AWS_SESSION_TOKEN']

        # If the required environment variables are not set, try reading from the credentials file
        if not all(key in aws_creds for key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token')):
            home_dir = os.path.expanduser("~")
            aws_creds_file_path = os.path.join(home_dir, ".aws", "credentials")
            # open file and get the keys from the file
            with open(aws_creds_file_path) as creds:
                data = creds.readlines()
                aws_access_key_id = data[1].split("=")[1].strip()
                aws_secret_access_key = data[2].split("=")[1].strip()
                aws_session_token = data[3].split("=")[1].strip()
                aws_creds["aws_access_key_id"] = aws_access_key_id
                aws_creds["aws_secret_access_key"] = aws_secret_access_key
                aws_creds["aws_session_token"] = aws_session_token
        return aws_creds
    

    def connect_to_sources(self):
        try:
            self.source_session = self.get_session(self.sourceBucket)
            self.dest_session = self.get_session(self.destinationBucket)
        except Exception as e:
            logging.error(f"Error, could not get session\n {e}")
            logging.info(f"Running consumer with source bucket: {self.sourceBucket} and destination bucket: {self.destinationBucket}")


    """
    With creds, get the session and return the s3 client or dynamo client
    """ 
    def get_session(self, bucket_name: str):
        session = boto3.Session(
            aws_access_key_id=self.creds["aws_access_key_id"],
            aws_secret_access_key=self.creds["aws_secret_access_key"],
            aws_session_token=self.creds["aws_session_token"],
            region_name='us-east-1'
        )
        if dynamoDBProcessor.does_dynamo_table_exist(bucket_name, 'us-east-1'):
            return session.client('dynamodb')
        elif S3Processor.does_s3_bucket_exist(bucket_name, 'us-east-1'):
            return session.client('s3')

