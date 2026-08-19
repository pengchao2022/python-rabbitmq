import json
import boto3
from botocore.exceptions import ClientError
import pika

def get_secret_from_aws(secret_name, region_name="us-east-1"):
    """从 AWS Secrets Manager 获取凭据"""
    print(f"正在从 Secrets Manager 获取密钥: {secret_name} ...")
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"❌ 获取 Secret 失败: {e}")
        raise e

    # 解析 JSON 格式的密钥
    secret_string = response['SecretString']
    return json.loads(secret_string)

def test_rabbitmq_connection(creds):
    """测试连接 RabbitMQ 并发送/接收一条测试消息"""
    username = creds.get('username')
    password = creds.get('password')
    # 如果你在本地测试，host 可以是 localhost；如果跨实例，填你的 EC2 私网/公网 IP
    host = creds.get('host', 'localhost') 

    print(f"正在尝试连接 RabbitMQ (Host: {host}, User: {username}) ...")

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=5672,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )

    try:
        # 1. 建立连接
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        print("✅ 成功连接到 RabbitMQ!")

        # 2. 声明一个测试队列 (Queue)
        queue_name = 'test_aws_queue'
        channel.queue_declare(queue=queue_name, durable=False)

        # 3. 发送一条测试消息
        message = "Hello from EC2 via Secrets Manager & RabbitMQ!"
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message
        )
        print(f"📤 已成功发送测试消息: '{message}'")

        # 4. 消费这条测试消息进行验证
        # method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
        # if method_frame:
        #     print(f"📥 成功接收到测试消息: '{body.decode()}'")
        # else:
        #     print("⚠️ 未能接收到刚才发送的消息")

        # # 5. 关闭连接
        # connection.close()
        # print("🎉 测试全部通过！")

    except Exception as e:
        print(f"❌ RabbitMQ 连接或操作失败: {e}")

if __name__ == "__main__":
    # 替换为你实际在 AWS Secrets Manager 中创建的 Secret 名称或 ARN
    SECRET_NAME = "secret-for-rabbitmq-int"
    REGION = "us-east-1"  # 换成你的 AWS 区域

    try:
        # 第一步：拉取凭据
        secret_data = get_secret_from_aws(SECRET_NAME, REGION)
        
        # 第二步：测试 RabbitMQ
        test_rabbitmq_connection(secret_data)
        
    except Exception as e:
        print(f"💥 脚本运行出错: {e}")