import pika
import json
import threading
import time
import random
import boto3
from botocore.exceptions import ClientError

# ==================== 0. 从 AWS Secrets Manager 获取凭证 ====================
def get_secret_from_aws(secret_name="secret-for-rabbitmq-int", region_name="us-east-1"):
    print(f"🔄 正在从 AWS Secrets Manager 获取密钥: {secret_name} ...")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        return secret_data
    except ClientError as e:
        print(f"❌ 获取 AWS 密钥失败: {e}")
        raise e

# 初始化全局连接参数（从云端拉取）
try:
    CREDS = get_secret_from_aws()
    RABBITMQ_HOST = CREDS.get('host', 'localhost')  # 这里会正确获取到你的公网IP或地址
    RABBITMQ_USER = CREDS.get('username')
    RABBITMQ_PASS = CREDS.get('password')
except Exception as e:
    print(f"❌ 初始化失败，请检查 AWS 凭证和网络: {e}")
    exit(1)

def get_connection():
    # 使用从 AWS 获取的真实账号、密码和远端 Host
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )
    return pika.BlockingConnection(parameters)

# ==================== 1. 生产者：模拟用户下单 ====================
def order_producer():
    try:
        connection = get_connection()
        channel = connection.channel()
    except Exception as e:
        print(f"❌ 生产者连接 RabbitMQ 失败: {e}")
        return

    exchange_name = 'ecommerce.order.exchange'
    queue_name = 'ecommerce.order.queue'
    
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='order.new')

    print("🛒 订单服务已启动，开始模拟用户下单...")

    for i in range(1, 4):
        order_data = {
            "order_id": f"ORD-2026-0820-{random.randint(10000, 99999)}",
            "user_id": 1000 + i,
            "total_amount": round(random.uniform(50.0, 500.0), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        channel.basic_publish(
            exchange=exchange_name,
            routing_key='order.new',
            body=json.dumps(order_data),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"📤 [用户下单成功] 订单号: {order_data['order_id']} 已写入云端 MQ (金额: ¥{order_data['total_amount']})")
        time.sleep(1)

    connection.close()

# ==================== 2. 消费者：模拟后端异步处理系统 ====================
def background_worker(worker_name):
    try:
        connection = get_connection()
        channel = connection.channel()
    except Exception as e:
        print(f"❌ [{worker_name}] 连接 RabbitMQ 失败: {e}")
        return
    
    queue_name = 'ecommerce.order.queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        order = json.loads(body.decode())
        print(f"\n⚙️ [{worker_name}] 收到新订单，开始异步处理...")
        print(f"   -> 订单详情: {order['order_id']} | 用户: {order['user_id']}")
        
        print(f"   -> 1/3 正在调用仓储服务扣减库存...")
        time.sleep(1.5)
        print(f"   -> 2/3 正在增加用户积分...")
        time.sleep(1.0)
        print(f"   -> 3/3 正在发送短信通知...")
        time.sleep(1.0)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"✅ [{worker_name}] 订单 {order['order_id']} 全套业务处理完毕！")

    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    print(f"👷 [{worker_name}] 后端工作线程已就绪，正在监听云端队列...")
    channel.start_consuming()

if __name__ == '__main__':
    # 启动后台消费线程
    t1 = threading.Thread(target=background_worker, args=("Worker-A",))
    t2 = threading.Thread(target=background_worker, args=("Worker-B",))
    
    t1.daemon = True
    t2.daemon = True
    
    t1.start()
    t2.start()

    # 等消费者准备好后，主线程开始发消息
    time.sleep(2)
    order_producer()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序退出。")