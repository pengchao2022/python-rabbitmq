import pika
import json
import time
import boto3
from botocore.exceptions import ClientError

# 1. 从 AWS Secrets Manager 获取连接凭证
def get_secret_from_aws(secret_name="secret-for-rabbitmq-int", region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def main():
    try:
        creds = get_secret_from_aws()
        host = creds.get('host', 'localhost')
        username = creds.get('username')
        password = creds.get('password')
    except Exception as e:
        print(f"❌ 无法加载配置: {e}")
        return

    # 2. 建立 RabbitMQ 连接
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=5672, credentials=credentials)
    )
    channel = connection.channel()

    queue_name = 'bulk_order_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    
    # 公平分发：一次只拿一条，处理完才拿下一条
    channel.basic_qos(prefetch_count=1)

    # 3. 定义消费回调函数
    def callback(ch, method, properties, body):
        order = json.loads(body.decode())
        print(f"⚙️ 正在处理订单 [{order['sequence']}/100] -> 单号: {order['order_id']} (用户: {order['user_id']})")
        
        # 模拟真实的业务耗时（比如写数据库、扣库存）
        time.sleep(0.5) 

        # 手动确认消息处理完成
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"✅ 订单 {order['order_id']} 处理成功并已 ACK。")

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(f"👷 消费者已启动，正在从远端 ({host}) 监听队列 [{queue_name}]，按 Ctrl+C 退出...")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n程序安全退出。")