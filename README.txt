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
        sudo docker run consumer:2.3
