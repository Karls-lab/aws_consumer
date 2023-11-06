import boto3
import json

def send_widgets_to_sqs(s3_bucket, queue_url):
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')

    response = s3.list_objects_v2(Bucket=s3_bucket)

    if 'Contents' in response:
        for obj in response['Contents']:
            widget_object = s3.get_object(Bucket=s3_bucket, Key=obj['Key'])
            widget_data = widget_object['Body'].read().decode('utf-8')
            widget_data_json = json.loads(widget_data)

            print(f"Sending message to SQS queue, ID: {obj['Key']}")
            print(f"Widget data: {widget_data_json.keys()}")
            # Prepare the message body for the SQS queue

            type = widget_data_json['type']
            
            message_body = {
                'type': widget_data_json['type'],
                'requestId': widget_data_json['requestId'],  
                'widgetId': obj['Key'], 
                'owner': widget_data_json['owner'], 
            }

            if type == 'create':
                message_body['description'] = widget_data_json['description']
                message_body['label'] =  widget_data_json['label']
                message_body['otherAttributes'] = widget_data_json['otherAttributes']
            elif type == 'update':
                message_body['description'] = widget_data_json['description']
                message_body['otherAttributes'] = widget_data_json['otherAttributes']
            elif type == 'delete':
                pass
            # Send the message to the SQS queue
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))


s3_bucket_name = 'usu-cs5250-quartz-requests'
sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests'
send_widgets_to_sqs(s3_bucket_name, sqs_queue_url)

# {"type":"delete","requestId":"21cf74b2-dbf2-46bb-a274-b8e3eac679bf","widgetId":"e50b4381-4332-438f-bcec-bd2b8c0fa5ed","owner":"Sue Smith"}
# {"type":"update","requestId":"a606601e-4861-4cf8-b948-f534505c24d0","widgetId":"0510d371-0191-43d9-81d5-5a163659ae6f","owner":"Mary Matthews","description":"LJEQMYCCJRZDQFJETHKWYRMWTOPRQYEBWFWQHQUPVTGRXURPPKEYAPEEYAAV","otherAttributes":[{"name":"color","value":"blue"},{"name":"size","value":"360"},{"name":"size-unit","value":"cm"},{"name":"height","value":"920"},{"name":"width","value":"103"},{"name":"width-unit","value":"cm"},{"name":"rating","value":"3.6255655"},{"name":"price","value":"98.76"},{"name":"note","value":"YZAVYFSDXEPIARJWLVFLBMSADGGIVUVSTFHUCKOSWFVKWODINGKECEZFKHVLGWFKYHYVQZVHTMROSVTXMSWQGUMHFORJIZYOXPNOZLFPMCFQPKNRWMWUBTTPOVYPZWODIGBDCKZTBBNICWTPRONOMCCBZUJAYCRHFOJLPKHHELORIRLPLZGUQQHPBTRYZCJKZHMDUSFTXXGFTQHALLTPFIESBIUTKNNBYNDYROAEYQUNUFMZDYDKMVTMRPJMSBPLQVYZNEYAEUXIAQOJEXTLISTXIEESSTZYQDSQKLNGSJYMVGDZSSNHLFAIJWEVTRNQAYQPEITGNZSLERCGJIEVCVMB"}]}
# {"type":"create","requestId":"8709c06d-9d32-464a-a015-48495781c455","widgetId":"5c94f104-a9b3-4fed-a039-2cc4c631d042","owner":"John Jones","label":"GEDOQ","description":"KCBCDPEATHQBDXJFSQNGRDTKHGFUUTPXRMQFWALYJPYIUBRGOMQCRVCLKJTKRXTDLCQKCHZCIIZVJQVUZYPTZMDHV","otherAttributes":[{"name":"size","value":"777"},{"name":"height","value":"129"},{"name":"width","value":"772"},{"name":"length","value":"46"},{"name":"rating","value":"1.6434183"},{"name":"note","value":"AZOALXMZWOXIXSAKHWPRGTDVRMFUQGIYTLYISZRVRIWQXIBZEBZNEOJBEKOQHCHRCNRBFGMYAPYOHZDIZOTMQJYPUULPVELSVVYVCYBTFWAEZASLGIKCISEDATXIZZYFQCWUMUOMMLXLEPCQFPIKHOCMHGIFMLIBEUYZCWEHOBTXSMTGXOCHHKKWXRXKGFLUTCRMFVHSLLYSCPMOBXFFDWELHLQWSTAPWMNJMPBIBJVOGOFKPAWLUCGBCVHYBKZIFXZYNDIKNDWKRYGCCGIMCXOSEFNGXRCLONQWFYIKZVMVGFDDXHBCWGS"}]}