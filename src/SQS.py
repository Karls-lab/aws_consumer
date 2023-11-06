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

