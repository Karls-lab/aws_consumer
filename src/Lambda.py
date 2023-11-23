import json
import boto3


import os
import random
import requests
"""
Returns a random widget request from the sample-requests folder
Loads it into a plaintext dictionary. 3 types, create, delete, update
"""
def getFakeWidgetRequest():
    try:
        filePath = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sample-requests')
        randomWidgetRequest = random.choice(os.listdir(filePath))
        randomFile = os.path.join(filePath, randomWidgetRequest)
        with open(randomFile, 'r') as f:
            json_data = json.load(f)
        # Return the json data as it would show as a POST request
        formatted_json_data = json.dumps(json_data, indent=2)
        print(f"Random Widget Request: {formatted_json_data}")
        return formatted_json_data
    except Exception as e:
        print(e)
        return None


def lambda_handler(event):
    try:
        widget_request = event

        if validate_widget_request(widget_request):
            print("Validation Successful")
            place_in_request_queue(widget_request)
            return {
                'statusCode': 200,
                'body': json.dumps('Request processed successfully')
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid Widget Request')
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing request: {str(e)}')
        }

def validate_widget_request(widget_request):
    try:
        widget_request = str(widget_request).replace("'", "\"")
        widget_request = json.loads(widget_request)
        widget_type = widget_request['type']
        print(f"Validating {widget_type}")

        create_required_fields = ['requestId', 'widgetId', 'owner', 'label', 'description', 'otherAttributes']
        delete_required_fields = ['requestId', 'widgetId', 'owner']
        update_required_fields = ['requestId', 'widgetId', 'owner', 'description', 'otherAttributes']

        if widget_type == 'create':
            if all(field in widget_request for field in create_required_fields):
                return True
            else:
                print("Create Request Validation Failed: Missing required fields")
                return False
        elif widget_type == 'delete':
            if all(field in widget_request for field in delete_required_fields):
                return True
            else:
                print("Delete Request Validation Failed: Missing required fields")
                return False
        elif widget_type == 'update':
            if all(field in widget_request for field in update_required_fields):
                return True
            else:
                print("Update Request Validation Failed: Missing required fields")
                return False
        else:
            print("Validation Failed: Invalid Widget Type")
            return False
    except Exception as e:
        print("Validation Failed")
        print(e)
        return False 

def place_in_request_queue(widget_request):
    print("Placing in request queue:")
    # sqs = boto3.client('sqs')



test = getFakeWidgetRequest()
# deleteTest = "{'type': 'create', 'widgetId': '6cea7243-924a-4bc9-9779-8bc89e91acdf', 'owner': 'Sue Smith', 'label': 'IUGSPN', 'description': 'ETHOQJFDFK', 'otherAttributes': [{'name': 'size', 'value': '586'}, {'name': 'size-unit', 'value': 'cm'}, {'name': 'height', 'value': '545'}, {'name': 'width', 'value': '922'}, {'name': 'width-unit', 'value': 'cm'}, {'name': 'price', 'value': '4.98'}, {'name': 'quantity', 'value': '180'}, {'name': 'vendor', 'value': 'QANSJAELVXENT'}]}"
# updateTest = "{'type': 'update', 'requestId': 'd61c3e72-1a66-4cfa-9162-56712e4580d8', 'widgetId': '6984abeb-5b24-42eb-93cc-3a5bef6b4b8a', 'owner': 'Mary Matthews', 'description': 'PUMMCL', 'otherAttributes': [{'name': 'size', 'value': '745'}, {'name': 'size-unit', 'value': 'cm'}, {'name': 'height', 'value': '879'}, {'name': 'height-unit', 'value': 'cm'}, {'name': 'width-unit', 'value': 'cm'}, {'name': 'length', 'value': '793'}, {'name': 'price', 'value': '50.96'}, {'name': 'quantity', 'value': '311'}, {'name': 'note', 'value': 'MYEVVLRLAWVRZTQIMWRTJFDZTSJNJTWXQBFXOBABMNGJDCWRJMAGVYSWWAPYWDCHSDKFAURWSBHGABSMVKRLQZKXEXJLNXZU'}]}"
print("Validating: \n", test, "\n")
lambda_handler(test, None)


