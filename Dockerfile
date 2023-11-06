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

# Set environment variables
ENV AWS_ACCESS_KEY_ID=ASIA4L6YQWS5Z2Y4GBOD
ENV AWS_SECRET_ACCESS_KEY=NCcuk9qtBDdP+vmfBVAiNunYvFBuiUMpZ/HjPU0p
ENV AWS_SESSION_TOKEN=FwoGZXIvYXdzEF4aDNLTo6y3T1Rhva1AniK9AZKHoJPBqUb0beiW/wZFDRy6hb+c2sxba5ACLSFB9jI9RiQQ7xG05qfHAQrATpSiwOCpQ1ONyp4oC4bebXZ3i7Fd8PbdFaGi9dMBi+cDARV7VzeA3t5PkrFhapjMaixZc6Yb/ohTWK1vkvLlni5FxfGN4zvoC3KeTgzSFzU32px/a1FjpWlSENJ+XDS1wxwJ3pSMyQOI+ZL5cGVhhhUf06G3vQ3URfDsrisOEiwRJjB1hj5R0kS2dKuSUnXqQSi61qGqBjIt2x+vbgFI/XoqEewIjcZ1tjh/OCRDVoTC65B+i7zh5ZYfslMS2NtG+iiu0gWA

# Define the command that should be executed when the container starts
CMD ["python3", "consumer.py", "-rb", "usu-cs5250-quartz-requests", "-wb", "usu-cs5250-quartz-web"]
