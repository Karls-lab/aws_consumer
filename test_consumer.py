import unittest
from unittest.mock import MagicMock, patch
from consumer import (
    convert_dict_to_dynamodb_format,
    does_s3_bucket_exist,
    does_dynamo_table_exist,
    get_session,
    processData,
    processDynamoData,
)

"""
Various Simple Units tests to make sure the 
functions in consumer.py are working properly
"""
class TestConsumerFunctions(unittest.TestCase):
    def test_convert_dict_to_dynamodb_format(self):
        input_data = {
            'id': 123,
            'name': 'John',
            'tags': ['tag1', 'tag2']
        }
        expected_output = {
            'id': {'N': '123'},
            'name': {'S': 'John'},
            'tags': {'L': ['tag1', 'tag2']}
        }
        self.assertEqual(convert_dict_to_dynamodb_format(input_data), expected_output)


    @patch('consumer.convert_dict_to_dynamodb_format')
    def test_processDynamoData(self, mock_convert):
        json_data = {
            "id": 1,
            "owner": "john-smith",
            "description": "Sample Widget",
            "otherAttributes": [{"name": "color", "value": "red"}],
        }
        mock_convert.return_value = {
            "id": {"N": "1"},
            "owner": {"S": "john-smith"},
            "description": {"S": "Sample Widget"},
            "color": {"S": "red"},
        }
        dynamo_data = processDynamoData(json_data)
        expected_result = {
            "id": {"N": "1"},
            "owner": {"S": "john-smith"},
            "description": {"S": "Sample Widget"},
            "color": {"S": "red"},
        }
        self.assertEqual(dynamo_data, expected_result)



    def test_does_s3_bucket_exist(self):
        with patch('consumer.boto3.client') as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            # Bucket exists
            mock_s3.head_bucket.return_value = None
            self.assertTrue(does_s3_bucket_exist('existing-bucket', 'us-east-1'))

            # Bucket does not exist
            mock_s3.head_bucket.side_effect = Exception('Bucket not found')
            self.assertFalse(does_s3_bucket_exist('non-existing-bucket', 'us-east-1'))


    def test_does_dynamo_table_exist(self):
        with patch('consumer.boto3.client') as mock_client:
            mock_dynamo = MagicMock()
            mock_client.return_value = mock_dynamo

            # Table exists
            mock_dynamo.describe_table.return_value = {'Table': {'TableName': 'existing-table'}}
            self.assertTrue(does_dynamo_table_exist('existing-table', 'us-east-1'))

            # Table does not exist
            mock_dynamo.describe_table.side_effect = mock_dynamo.exceptions.ResourceNotFoundException({'Error': {}}, 'DescribeTable')
            self.assertFalse(does_dynamo_table_exist('non-existing-table', 'us-east-1'))


    def test_processData(self):
        sample_data = {
            "widgetId": 1,
            "owner": "John",
            "description": "Sample",
            "otherAttributes": [{"name": "attr1", "value": "value1"}]
        }
        processed_data = processData(sample_data)
        expected_data = {
            "id": 1,
            "owner": "john",
            "description": "Sample",
            "otherAttributes": [{"name": "attr1", "value": "value1"}]
        }
        self.assertEqual(processed_data, expected_data)


    @patch('boto3.Session.client')
    def test_get_session(self, mock_session_client):
        aws_creds = {
            "aws_access_key_id": "fake_access_key",
            "aws_secret_access_key": "fake_secret_key",
            "aws_session_token": "fake_session_token",
        }
        source_bucket = 'my-bucket'

        # Simulate a DynamoDB session
        mock_session_client.return_value.meta.service_model.service_name = 'dynamodb'
        session = get_session(aws_creds, source_bucket)
        self.assertEqual(session.meta.service_model.service_name, 'dynamodb')

        # Simulate an S3 session
        mock_session_client.return_value.meta.service_model.service_name = 's3'
        session = get_session(aws_creds, source_bucket)
        self.assertEqual(session.meta.service_model.service_name, 's3')


if __name__ == '__main__':
    unittest.main()
