import boto3
from botocore.exceptions import NoCredentialsError


class dynamoDBProcessor():
    def __init__(self):
        pass


    """
    Break down other attributes into a singular dictionary
    Return dictionary after converted to dynamodb format
    """
    def processData(self, data):
        returnDict = {}
        for attribute in data["otherAttributes"]:
            returnDict[attribute['name']] = attribute['value']
        data.pop('otherAttributes')
        for key in returnDict:
            data[key] = returnDict[key]
        return self.convert_dict_to_dynamodb_format(data)


    """
    Converts a regular dictionary to a dynamodb dictionary
    """
    def convert_dict_to_dynamodb_format(self, data_dict):
        dynamodb_data = {}
        for key, value in data_dict.items():
            if isinstance(value, int):
                dynamodb_data[key] = {'N': str(value)}
            elif isinstance(value, list):
                dynamodb_data[key] = {'L': value}
            else:
                dynamodb_data[key] = {'S': str(value)}
        return dynamodb_data


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

