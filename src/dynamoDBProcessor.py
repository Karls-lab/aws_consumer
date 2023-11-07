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
        
        for key in list(data.keys()):  # Iterate over a copy of the keys to avoid changing the dictionary size during iteration
            new_key = key.replace('-', '_')  # Replace hyphens with underscores
            if new_key != key:
                data[new_key] = data.pop(key)  # Update key if there's a replacement
        
        for key in returnDict:
            new_key = key.replace('-', '_')  # Replace hyphens with underscores for new keys
            data[new_key] = returnDict[key]

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
        
    
    def getUpdateExpression(data):
        update_expression = 'SET '
        expression_attribute_values = {}
        expression_attribute_names = {}

        # Construct the update expression excluding 'owner'
        for key, value in data.items():
            if key == 'type':  # Check for the reserved keyword 'type'
                expression_attribute_names['#widgetType'] = 'type'  # Using ExpressionAttributeNames for the reserved keyword
                update_expression += "#widgetType = :widgetType, "
                expression_attribute_values[":widgetType"] = value
            elif key == 'owner':  # Check for the reserved keyword 'owner'
                expression_attribute_names['#widgetOwner'] = 'owner'  # Using ExpressionAttributeNames for the reserved keyword
                update_expression += "#widgetOwner = :widgetOwner, "
                expression_attribute_values[":widgetOwner"] = value
            elif key != 'id':  # Exclude 'widgetId' from the update expression
                update_expression += f"#{key} = :{key}, "
                expression_attribute_values[f":{key}"] = value
                expression_attribute_names[f"#{key}"] = key

        update_expression = update_expression[:-2]  # Remove the trailing comma and space

        return update_expression, expression_attribute_values, expression_attribute_names

