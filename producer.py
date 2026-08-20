import pika
import json
import random
import time
import boto3
from botocore.exceptions import ClientError

# 1. 从 AWS Secrets Manager 获取连接凭证
def get_secret_from_aws(secret_name="secret-for-rabbitmq-int", region_name="us-east-1"):
    print(f"🔄 正在从 AWS Secrets Manager 获取密钥: {secret_name} ...")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"❌ 获取 AWS 密钥失败: {e}")
        raise e

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
    try:
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host, 
                port=5672, 
                credentials=credentials,
                heartbeat=3600  # 100万条逐条打印耗时较长，心跳延长至 1 小时
            )
        )
        channel = connection.channel()
    except Exception as e:
        print(f"❌ 连接 RabbitMQ 失败: {e}")
        return

    # 3. 声明队列（持久化）
    queue_name = 'bulk_order_queue'
    channel.queue_declare(queue=queue_name, durable=True)

    TOTAL_COUNT = 10000000  # 100万
    print(f"🚀 开始向远程 RabbitMQ ({host}) 逐条发送并打印 {TOTAL_COUNT:,} 个订单消息...\n")
    
    start_time = time.time()

    # 4. 循环发送 100 万条消息，每条都严格打印
    try:
        for i in range(1, TOTAL_COUNT + 1):
            order_data = {
                "sequence": i,
                "order_id": f"ORD-2026-{random.randint(1000000, 9999999)}",
                "user_id": random.randint(2000, 3000),
                "amount": round(random.uniform(20.0, 1000.0), 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(order_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化
                )
            )
            
            # 严格按照你要求的格式，每一条都实时打印
            print(f"📤 [{i}/{TOTAL_COUNT}] 已发送订单: {order_data['order_id']} (金额: ¥{order_data['amount']})")

    except KeyboardInterrupt:
        print("\n⚠️ 用户手动中断了发送过程。")
    except Exception as e:
        print(f"\n❌ 发送过程中发生异常: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()
            
    total_time = time.time() - start_time
    print(f"\n✨ 1000 万条订单逐条发送完毕！总耗时: {total_time:.2f} 秒")

if __name__ == '__main__':
    main()