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
from SQS import SQSHandler


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
Create, Update, and Delete widget functions
"""
def create_widget(data, dest_session, destBucket, dynamoProcessor):
    requestKey = f"widgets/{data['owner']}/{data['requestId']}"
    logging.info(f"Creating a new widget with id: {data['requestId']}")

    if dest_session.meta.service_model.service_name == "dynamodb":
        processed_data = dynamoProcessor.processData(data)
        dest_session.put_item(TableName="widgets", Item=processed_data)
    else:
        dest_session.put_object(Bucket=destBucket, Key=requestKey, Body=json.dumps(data))


def update_widget(data, dest_session, destBucket, dynamoProcessor):
    widget_id = data['widgetId']
    logging.info(f"Updating a widget with ID: {widget_id}")

    if dest_session.meta.service_model.service_name == "dynamodb":
        key = {'widgetId': {'S': widget_id}}
        dest_session.update_item(TableName="widgets", Key=key, UpdateExpression='SET ...')
    else:
        data_bytes = bytes(json.dumps(data), 'utf-8')
        dest_session.put_object(Bucket=destBucket, Key=f"widgets/{data['owner']}/{widget_id}", Body=data_bytes)


def delete_widget(data, dest_session, destBucket):
    widget_id = data['widgetId']
    logging.info(f"Deleting a widget with ID: {widget_id}")

    if dest_session.meta.service_model.service_name == "dynamodb":
        dest_session.delete_item(TableName="widgets", Key={'widgetId': widget_id})
    else:
        dest_session.delete_object(Bucket=destBucket, Key=f"widgets/{data['owner']}/{widget_id}")



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

        if requestType == 'create':
            create_widget(processData(jsonData), dest_session, destBucket, dynamoDBProcessor())
        elif requestType == 'update':
            update_widget(processData(jsonData), dest_session, destBucket, dynamoDBProcessor())
        elif requestType == 'delete':
            delete_widget(jsonData, dest_session, destBucket)


"""
Authenticate the user, get the session, and run the consumer
Try Catch blocks to catch errors and logs them
"""
def run_consumer(source, destination, queue_url=None):
    # Initialize the credentials manager
    manager = credsManager(source, destination)

    try:
        if queue_url:
            run_consumer_with_sqs(queue_url, manager.dest_session, manager.destinationBucket)

        else:
            run(manager.source_session, manager.sourceBucket, 
            manager.dest_session, manager.destinationBucket)
    except Exception as e:
        logging.error(f"Error, could not run consumer\n {e}")


"""
SQS run consumer logic
"""
def run_consumer_with_sqs(queue_url, dest_session, destBucket):
    logging.info(f"Running consumer with SQS queue: {queue_url}")
    sqs_handler = SQSHandler(queue_url)  # Pass the SQS queue URL
    # Receive 10 messages at a time, get another 10 if the queue is still full
    while True:
        messages = sqs_handler.receive_messages()
        if not messages:
            logging.info("No messages to process, stopping")
            break
        process_messages_from_sqs(messages, sqs_handler, dest_session, destBucket)


def process_messages_from_sqs(messages, sqs_handler, dest_session, destBucket):
    for message in messages:
        logging.info(f"Processing message: {message['MessageId']}")
        message_body = json.loads(message['Body'])
        request_type = message_body['type']

        dynamo_processor = dynamoDBProcessor()
        if request_type == 'create':
            create_widget(message_body, dest_session, destBucket, dynamo_processor)
        elif request_type == 'update':
            update_widget(message_body, dest_session, destBucket, dynamo_processor)
        elif request_type == 'delete':
            delete_widget(message_body, dest_session, destBucket)
        sqs_handler.delete_message(message['ReceiptHandle'])

"""
Main function and command line arguments
"""
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Your program description")
    parser.add_argument('-rb', '--request_bucket', type=str, help="Specify the storage strategy")
    parser.add_argument('-wb', '--widget_bucket', type=str, help="Specify the resources to use")
    parser.add_argument('-q', '--queue_url', type=str, help="Specify the SQS Queue URL")
    args = parser.parse_args()

    source = args.request_bucket
    resources_to_use = args.widget_bucket
    queue_url = args.queue_url

    # Run consumer with args
    try:
        run_consumer(source, resources_to_use, queue_url)
    except Exception as e:
        logging.error("Unable to run Consumer")
        logging.error(f"Error: {e}")

"""
Example command: 
    python3 consumer.py -rb usu-cs5250-quartz-requests -wb usu-cs5250-quartz-web
Example with Dynamo: 
    python3 consumer.py -rb usu-cs5250-quartz-requests -wb widgets 
Example consumer SQS command: 
    python3 consumer.py -q https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests -wb usu-cs5250-quartz-web
Example consumer SQS command (dynamo): 
    python3 consumer.py -q https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests -wb widgets
Example producer command: 
    java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests
Example producer with 100 requests:
    java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests -mwr=100
"""
