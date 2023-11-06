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
from credsManager import credsManager
from S3Processor import S3Processor
from dynamoDBProcessor import dynamoDBProcessor


# Create the log directory if it doesn't exist
log_dir = '../logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure the logger
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='../logs/consumer.log', level=logging.INFO, format=log_format)
console_handler = logging.StreamHandler()  # Add a stream handler to log to the console
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)  # Add the console handler to the root logger


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
Main loop to run the consumer
"""
def run(source_session, sourceBucket, dest_session, destBucket, dynamoTable=None):
    requestQueue =  S3Processor.getRequestQueue(source_session, sourceBucket)
    stop_times = 10 # will retry to populate queue 10 times before stopping program 

    while True:
        # if no more requests, check if there are more to process
        if len(requestQueue) == 0:
            logging.info("No requests to process, checking for more")
            requestQueue = S3Processor.getRequestQueue(source_session, sourceBucket)
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
        jsonData = S3Processor.downloadBucket(source_session, sourceBucket, requestKey)

        # delete the request from the bucket
        source_session.delete_object(Bucket=sourceBucket, Key=request['Key'])

        # get the type of request
        requestType = jsonData['type']
        logging.info(f"Request type: {requestType}")

        # Initialize the dynamoDBProcessor
        dynamoProcessor = dynamoDBProcessor()

        """
        Logic to Create, Update, or Delete a widget
        """
        if requestType == 'create':
            data = processData(jsonData)
            requestKey = f"widgets/{data['owner']}/{jsonData['requestId']}"
            logging.info(f"Creating a new widget with id: {jsonData['requestId']}")

            if dest_session.meta.service_model.service_name == "dynamodb": # save to dynamoDB
                data = dynamoProcessor.processData(data)
                dest_session.put_item(TableName="widgets", Item=data)
            else: # save to s3
                dest_session.put_object(Bucket=destBucket, Key=requestKey, Body=json.dumps(data))

        elif requestType == 'update':
            data = processData(jsonData)
            widget_id = jsonData['widgetId']  
            logging.info(f"Updating a widget with ID: {widget_id}")

            if dest_session.meta.service_model.service_name == "dynamodb":
                key = {'widgetId': {'S': widget_id}}  
                dest_session.update_item(TableName="widgets", Key=key, UpdateExpression='SET ...')
            else:
                data_bytes = bytes(json.dumps(jsonData), 'utf-8')
                dest_session.put_object(Bucket=destBucket, Key=f"widgets/{data['owner']}/{widget_id}", Body=data_bytes)


        elif requestType == 'delete':
            widget_id = jsonData['widgetId']  # Assuming 'widgetId' is the unique identifier for the item
            logging.info(f"Deleting a widget with ID: {widget_id}")

            if dest_session.meta.service_model.service_name == "dynamodb":
                dest_session.delete_item(TableName="widgets", Key={'widgetId': widget_id})
            else:
                dest_session.delete_object(Bucket=destBucket, Key=f"widgets/{jsonData['owner']}/{widget_id}")


"""
Authenticate the user, get the session, and run the consumer
Try Catch blocks to catch errors and logs them
"""
def run_consumer(sourceBucket='usu-cs5250-quartz-requests', destinationBucket='usu-cs5250-quartz-web'):
    # Initialize the credentials manager
    manager = credsManager(sourceBucket, destinationBucket)
    try:
        print(manager.source_session)
        print(manager.dest_session)
        run(manager.source_session, manager.sourceBucket, 
            manager.dest_session, manager.destinationBucket)
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
