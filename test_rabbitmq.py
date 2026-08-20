import json
import boto3
from botocore.exceptions import ClientError
import pika

def get_secret_from_aws(secret_name, region_name="us-east-1"):
    print(f"We are trying to getsecret from Secrets Manager : {secret_name} ...")
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"❌ Failed to get Secret error: {e}")
        raise e

    # get the secret from json
    secret_string = response['SecretString']
    return json.loads(secret_string)

def test_rabbitmq_connection(creds):
    username = creds.get('username')
    password = creds.get('password')
    host = creds.get('host', 'localhost') 

    print(f"We are trying to connect RabbitMQ (Host: {host}, User: {username}) ...")

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=5672,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )

    try:
        # create connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        print("✅ Successfully connect RabbitMQ!")

        # declare a test queue
        queue_name = 'test_aws_queue'
        channel.queue_declare(queue=queue_name, durable=False)

        # send a test message
        message = "Hello from EC2 via Secrets Manager & RabbitMQ!"
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message
        )
        print(f"📤 Successfully sent the message: '{message}'")

        # consume the test message
        # method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
        # if method_frame:
        #     print(f"📥 Successfully received the message: '{body.decode()}'")
        # else:
        #     print("⚠️ did not get the message")

        # # close the connection
        # connection.close()
        # print("All Passed！")

    except Exception as e:
        print(f"❌ RabbitMQ failed to connect: {e}")

if __name__ == "__main__":
    
    SECRET_NAME = "secret-for-rabbitmq-int"
    REGION = "us-east-1"  

    try:
        # get the credentials
        secret_data = get_secret_from_aws(SECRET_NAME, REGION)
        
        # test RabbitMQ
        test_rabbitmq_connection(secret_data)
        
    except Exception as e:
        print(f"error : {e}")