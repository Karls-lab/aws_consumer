"""
Specs: need to provide cmd line args for: storage strategy, and resources to use
Example of args: python consumer.py -rb usu-cs5250-blue-requests -dwt widget 

Instructions: 
in this assignment, you will write a program similar to the Consumer program used in CS5250.
Specifically, this Consumer program will read objects (Widget Requests) from an S3 bucket
(namely, Bucket 2) and then process those requests. Each request specifies results in a single
Widget creation, update, or deletion in either another S3 bucket (Bucket 3) or in a DynamoDB
table.
"""
# example command to run producer: java -jar consumer.jar -rb usu-cs5250-quartz-requests -dwt widgets

import argparse
import boto3
import time
import json
import logging
import os


"""
Opens the .aws file located in the home directory
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
    s3 = boto3.client('s3', region_name=region_name)
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except s3.exceptions.ClientError as e:
        return False

def does_dynamo_table_exist(table_name, region_name):
    dynamodb = boto3.client('dynamodb', region_name=region_name)
    try:
        dynamodb.describe_table(TableName=table_name)
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        return False


"""
With creds, get the session and return the s3 client
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


# Downloads the bucket data and decodes it into a dictionary
def downloadBucket(session, bucket, key):
    requestObject = session.get_object(Bucket=bucket, Key=key)
    data = requestObject['Body'].read().decode('utf-8')
    return json.loads(data)


# Processes json data, removes some fields
def processData(data):
    returnDict = {}
    returnDict['id'] = data['widgetId']
    returnDict['owner'] = data['owner'].replace(" ", "-").lower()
    returnDict['description'] = data['description']
    returnDict['otherAttributes'] = data['otherAttributes']
    return returnDict


# if saving to dynamoDB, other attributes need to be in own col
def processOtherAttributes(attributes):
    returnDict = {}
    for attribute in attributes:
        returnDict[attribute['name']] = attribute['value']
    return returnDict


def run(session, sourceBucket, destBucket, dynamoTable=None):
    requestQueue = []
    stop_times = 10

    while True:
        # if no more requests, check if there are more to process
        if len(requestQueue) == 0:
            try:
                requests = session.list_objects(Bucket=sourceBucket)
            except Exception as e:
                print(f"Error: {e}")
            if 'Contents' not in requests:
                pass
            else:
                # sort widgets with smallest key first and ad to queue 
                requestQueue = sorted(requests['Contents'], key=lambda x: x['Key'])
            time.sleep(0.1)  # Wait for 100ms

        # if no more requests, end program 
        if len(requestQueue) == 0:
            print("Done! No more requests to process")
            stop_times -= 1
            if stop_times == 0:
                print("Stopping program")
                break
            continue

        # pop the first request from the queue
        request = requestQueue.pop(0)

        # Download the request from S3 bucket2 and decode it into a dict
        requestKey = request['Key']
        jsonData = downloadBucket(session, sourceBucket, requestKey)

        # delete the request from the bucket
        session.delete_object(Bucket=sourceBucket, Key=request['Key'])

        # get the type of request
        requestType = jsonData['type']

        # Logic to create widget to s3 or dynamoDB
        if requestType == 'create':
            data = processData(jsonData)
            requestKey = f"widgets/{data['owner']}/{jsonData['requestId']}"
            print(f"Creating a new widget with id: {jsonData['requestId']}")
            if dynamoTable != None:
                data['otherAttributes'] = processOtherAttributes(data['otherAttributes'])
                session.put_item(TableName=dynamoTable, Item=data)
            else: 
                session.put_object(Bucket=destBucket, Key=requestKey, Body=json.dumps(data))

        elif requestType == 'update':
            print("Updating a widget")

        elif requestType == 'delete':
            print("Deleting a widget")



def run_consumer(sourceBucket='usu-cs5250-quartz-requests', destinationBucket='usu-cs5250-quartz-web'):
    creds = get_aws_creds()
    session = get_session(creds, destinationBucket)
    run(session, sourceBucket, destinationBucket)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Your program description")
    parser.add_argument('-rb', '--request_bucket', type=str, help="Specify the storage strategy")
    parser.add_argument('-wb', '--widget_bucket', type=str, help="Specify the resources to use")
    args = parser.parse_args()

    storage_strategy = args.request_bucket
    resources_to_use = args.widget_bucket

    # Run consumer with args
    run_consumer(storage_strategy, resources_to_use)

"""
Example command: python3 consumer.py -rb usu-cs5250-quartz-requests -wb usu-cs5250-quartz-web
Example producer command: java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests
"""
