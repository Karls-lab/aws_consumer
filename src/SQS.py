import boto3
import logging

class SQSHandler:
    def __init__(self, queue_url, region_name='us-east-1'):
        self.sqs = boto3.client('sqs', region_name=region_name)
        self.queue_url = queue_url

    def receive_messages(self, max_number=10):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_number
            )
            messages = response.get('Messages', [])
            return messages
        except Exception as e:
            logging.error(f"Error receiving messages from SQS: {e}")
            return []

    def delete_message(self, receipt_handle):
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            logging.error(f"Error deleting message from SQS: {e}")

    # Other SQS-related methods can be added here based on your requirements

# Example execution in your main function
def run_consumer_with_sqs(queue_url):
    # Initialize the SQSHandler with the queue URL
    sqs_handler = SQSHandler(queue_url)  # Pass the SQS queue URL
    # Receive messages from SQS
    messages = sqs_handler.receive_messages()
    # Process received messages
    process_messages_from_sqs(messages, sqs_handler)  # Pass the SQS handler to the processing function

def process_messages_from_sqs(messages, sqs_handler):
    for message in messages:
        # Process and delete messages as before, but now with the SQS handler
        # Implement your message processing logic here
        sqs_handler.delete_message(message['ReceiptHandle'])
        # ...

import boto3
import logging

class SQSHandler:
    def __init__(self, queue_url, region_name='us-east-1'):
        self.sqs = boto3.client('sqs', region_name=region_name)
        self.queue_url = queue_url

    def receive_messages(self, max_number=10):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_number
            )
            messages = response.get('Messages', [])
            return messages
        except Exception as e:
            logging.error(f"Error receiving messages from SQS: {e}")
            return []

    def delete_message(self, receipt_handle):
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            logging.error(f"Error deleting message from SQS: {e}")

    # Other SQS-related methods can be added here based on your requirements

# Example execution in your main function
def run_consumer_with_sqs(queue_url):
    # Initialize the SQSHandler with the queue URL
    sqs_handler = SQSHandler(queue_url)  # Pass the SQS queue URL
    # Receive messages from SQS
    messages = sqs_handler.receive_messages()
    # Process received messages
    process_messages_from_sqs(messages, sqs_handler)  # Pass the SQS handler to the processing function

def process_messages_from_sqs(messages, sqs_handler):
    for message in messages:
        # Process and delete messages as before, but now with the SQS handler
        # Implement your message processing logic here
        sqs_handler.delete_message(message['ReceiptHandle'])
        # ...
