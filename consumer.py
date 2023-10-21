"""
Simple Consumer program that will read from a bucket, process the requests
and update the destination bucket/table 

Created by: Karl Poulson
A02264961
Oct 20 2023
"""
import argparse
import boto3
from botocore.exceptions import NoCredentialsError
import time
import json
import logging
import os

# Configure logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='logs/consumer.log', level=logging.INFO, format=log_format)
console_handler = logging.StreamHandler()  # Add a stream handler to log to the console
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)  # Add the console handler to the root logger



"""
Opens the .aws file located in the home directory and gets the keys
"""
def get_aws_creds():
    # declare dict to return, and get the path of aws credentials file
    aws_creds = {}
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


def does_dynamo_table_exist(table_name, region_name):
    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        response = dynamodb.describe_table(TableName=table_name)
        response = dict(response)
        if response.items():
            return True
        return False
    except NoCredentialsError:
        return False
    except Exception as e:
        return False


"""
With creds, get the session and return the s3 client or dynamo client
""" 
def get_session(aws_creds: dict, bucket_name: str):
    session = boto3.Session(
        aws_access_key_id=aws_creds["aws_access_key_id"],
        aws_secret_access_key=aws_creds["aws_secret_access_key"],
        aws_session_token=aws_creds["aws_session_token"],
        region_name='us-east-1'
    )
    if does_dynamo_table_exist(bucket_name, 'us-east-1'):
        return session.client('dynamodb')
    elif does_s3_bucket_exist(bucket_name, 'us-east-1'):
        return session.client('s3')


"""
Downloads the bucket data and decodes it into a dictionary
"""
def downloadBucket(session, bucket, key):
    requestObject = session.get_object(Bucket=bucket, Key=key)
    data = requestObject['Body'].read().decode('utf-8')
    return json.loads(data)


"""
Processes json data, removes some fields
"""
def processData(data):
    returnDict = {}
    returnDict['id'] = data['widgetId']
    returnDict['owner'] = data['owner'].replace(" ", "-").lower()
    returnDict['description'] = data['description']
    returnDict['otherAttributes'] = data['otherAttributes']
    return returnDict


"""
Break down other attributes into a singular dictionary
Return dictionary after converted to dynamodb format
"""
def processDynamoData(data):
    returnDict = {}
    for attribute in data["otherAttributes"]:
        returnDict[attribute['name']] = attribute['value']
    data.pop('otherAttributes')
    for key in returnDict:
        data[key] = returnDict[key]
    return convert_dict_to_dynamodb_format(data)


"""
Converts a regular dictionary to a dynamodb dictionary
"""
def convert_dict_to_dynamodb_format(data_dict):
    dynamodb_data = {}
    for key, value in data_dict.items():
        if isinstance(value, int):
            dynamodb_data[key] = {'N': str(value)}
        elif isinstance(value, list):
            dynamodb_data[key] = {'L': value}
        else:
            dynamodb_data[key] = {'S': str(value)}
    return dynamodb_data


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


"""
Main loop to run the consumer
"""
def run(source_session, sourceBucket, dest_session, destBucket, dynamoTable=None):
    requestQueue = getRequestQueue(source_session, sourceBucket)
    stop_times = 10 # will retry to populate queue 10 times before stopping program 

    while True:
        # if no more requests, check if there are more to process
        if len(requestQueue) == 0:
            logging.info("No requests to process, checking for more")
            requestQueue = getRequestQueue(source_session, sourceBucket)
            stop_times -= 1
            time.sleep(0.5)
            if stop_times == 0:
                logging.info("Finished processing requests, Stopping")
                break
            else:
                continue

        # pop the first request from the queue
        request = requestQueue.pop(0)

        # Download the request from S3 bucket2 and decode it into a dict
        requestKey = request['Key']
        logging.info(f"Downloading request: {requestKey}")
        jsonData = downloadBucket(source_session, sourceBucket, requestKey)

        # delete the request from the bucket
        source_session.delete_object(Bucket=sourceBucket, Key=request['Key'])

        # get the type of request
        requestType = jsonData['type']
        logging.info(f"Request type: {requestType}")

        # Logic to create widget to s3 or dynamoDB
        if requestType == 'create':
            data = processData(jsonData)
            requestKey = f"widgets/{data['owner']}/{jsonData['requestId']}"
            logging.info(f"Creating a new widget with id: {jsonData['requestId']}")

            if dest_session.meta.service_model.service_name == "dynamodb": # save to dynamoDB
                data = processDynamoData(data)
                dest_session.put_item(TableName="widgets", Item=data)
            else: # save to s3
                dest_session.put_object(Bucket=destBucket, Key=requestKey, Body=json.dumps(data))

        elif requestType == 'update':
            logging.info("Updating a widget")

        elif requestType == 'delete':
            logging.info("Deleting a widget")


"""
Authenticate the user, get the session, and run the consumer
Try Catch blocks to catch errors and logs them
"""
def run_consumer(sourceBucket='usu-cs5250-quartz-requests', destinationBucket='usu-cs5250-quartz-web'):
    try:
        creds = get_aws_creds()
    except Exception as e:
        logging.error(f"Error, could not find AWS Credentials in ~/.aws/credentials\n {e}")
    try:
        source_session = get_session(creds, sourceBucket)
        dest_session = get_session(creds, destinationBucket)
    except Exception as e:
        logging.error(f"Error, could not get session\n {e}")
    logging.info(f"Running consumer with source bucket: {sourceBucket} and destination bucket: {destinationBucket}")
    try:
        run(source_session, sourceBucket, dest_session, destinationBucket)
    except Exception as e:
        logging.error(f"Error, could not run consumer\n {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Your program description")
    parser.add_argument('-rb', '--request_bucket', type=str, help="Specify the storage strategy")
    parser.add_argument('-wb', '--widget_bucket', type=str, help="Specify the resources to use")
    args = parser.parse_args()

    storage_strategy = args.request_bucket
    resources_to_use = args.widget_bucket

    # Run consumer with args
    try:
        run_consumer(storage_strategy, resources_to_use)
    except Exception as e:
        logging.error(f"Error: {e}")

"""
Example command: python3 consumer.py -rb usu-cs5250-quartz-requests -wb usu-cs5250-quartz-web
Example with Dynamo: python3 consumer.py -rb usu-cs5250-quartz-requests -wb widgets 
Example producer command: java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests
"""
