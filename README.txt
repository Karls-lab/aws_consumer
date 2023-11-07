OVERVIEW:
    This program is a simple consumer program for Amazon AWS. 
    It is a command line program that takes two arguments, 
    the source and destination bucket names. It then copies all
    the files from the source bucket to the destination bucket.
    The bucket may be an s3 bucket or a dynamodb table.


USAGE:
    Run the produce to populate bucket:
        java -jar producer.jar --request-bucket=<bucket-name>

    Run consumer, get requests from destination bucket and write to source bucket:
        python3 consumer.py -rb <source-bucket> -wb <destination-bucket>

    Run consumer, get requests from destination bucket to dynamodb table:
        python3 consumer.py -rb <source-bucket> -wb <dynamodb-table>

LOGS:
    Logs are saved in the logs directory.
    Logs are track each request, it's type, and it's ID 


DOCKER GUIDE:
    To pass the AWS credentials to the docker container,
    use this command: 
        sudo docker build -t consumer:2.3 .
        sudo docker run --env-file creds.env consumer:2.3



Example command: 
    python3 consumer.py -rb usu-cs5250-quartz-requests -wb usu-cs5250-quartz-web

Example with Dynamo: 
    python3 consumer.py -rb usu-cs5250-quartz-requests -wb widgets 

Example consumer SQS command: 
    python3 consumer.py -q https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests -wb usu-cs5250-quartz-web

Example consumer SQS command (dynamo): 
    python3 consumer.py -q https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests -wb widgets

Example producer command: 
    java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests -mwr 20

Example producer with 100 requests:
    java -jar producer.jar --request-bucket=usu-cs5250-quartz-requests -mwr 100

Example producer populate SQS: 
    java -jar producer.jar -rq https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests -mwr 100
