# Use the base image that supports your program's runtime environment
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the necessary files from your local system into the container
COPY src/consumer.py /app/consumer.py
COPY src/credsManager.py /app/credsManager.py
COPY src/dynamoDBProcessor.py /app/dynamoDBProcessor.py
COPY src/S3Processor.py /app/S3Processor.py
COPY src/SQS.py /app/SQS.py
COPY creds.env /app/creds.env  

# Install necessary dependencies
RUN pip install boto3

# Define the command that should be executed when the container starts
CMD ["python3", "consumer.py", "-q", "https://sqs.us-east-1.amazonaws.com/850320733371/cs5260-requests", "-wb", "usu-cs5250-quartz-web"]
