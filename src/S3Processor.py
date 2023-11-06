import json
import boto3
from botocore.exceptions import NoCredentialsError
import logging

class S3Processor():
    def __init__():
        pass 

        
    """
    Downloads the bucket data and decodes it into a dictionary
    """
    def downloadBucket(session, bucket, key):
        requestObject = session.get_object(Bucket=bucket, Key=key)
        data = requestObject['Body'].read().decode('utf-8')
        return json.loads(data)

        
    """
    Functions to determine if bucket is an s3 or dynamo table
    """
    def does_s3_bucket_exist(bucket_name, region_name):
        try:
            s3 = boto3.client('s3', region_name=region_name)
            s3.head_bucket(Bucket=bucket_name)
            return True
        except NoCredentialsError:
            return False  
        except Exception as e:
            return False  


    """
    Get request from source bucket and return a queue
    """
    def getRequestQueue(session, sourceBucket):
        requestQueue = []
        logging.info("Listing objects in bucket")
        requests = session.list_objects(Bucket=sourceBucket)
        if 'Contents' not in requests:
            pass
        else: # sort widgets with smallest key first and ad to queue 
            requestQueue = sorted(requests['Contents'], key=lambda x: x['Key'])
        return requestQueue


