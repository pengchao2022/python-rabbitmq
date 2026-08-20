import pika
import json
import random
import time
import boto3
from botocore.exceptions import ClientError

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

    try:
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host, 
                port=5672, 
                credentials=credentials,
                heartbeat=7200  # 2小时超长心跳
            )
        )
        channel = connection.channel()
    except Exception as e:
        print(f"❌ 连接 RabbitMQ 失败: {e}")
        return

    exchange_name = 'enterprise.extreme.exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    TOTAL_QUEUES = 300
    MSGS_PER_QUEUE = 100000  # 每个队列 10 万条
    TOTAL_MSGS = TOTAL_QUEUES * MSGS_PER_QUEUE  # 共计 3000 万条

    print(f"🚀 [极限模式] 开始初始化 {TOTAL_QUEUES} 个队列...")
    
    queue_configs = []
    for i in range(1, TOTAL_QUEUES + 1):
        q_name = f"queue_ext_{i:03d}"
        r_key = f"ext.routing.{i}"
        channel.queue_declare(queue=q_name, durable=True)
        channel.queue_bind(exchange=exchange_name, queue=q_name, routing_key=r_key)
        queue_configs.append({"queue": q_name, "routing_key": r_key})

    print(f"✅ 300 个队列准备完毕！开始向远程服务器灌入 3000 万条消息，并开启【逐条绝对实时打印】...\n")
    
    start_time = time.time()
    global_counter = 0

    try:
        for q_idx, q_cfg in enumerate(queue_configs, 1):
            q_name = q_cfg["queue"]
            r_key = q_cfg["routing_key"]

            for i in range(1, MSGS_PER_QUEUE + 1):
                global_counter += 1
                order_data = {
                    "queue_id": q_idx,
                    "sequence": i,
                    "order_id": f"ORD-2026-{random.randint(100000, 999999)}",
                    "amount": round(random.uniform(10.05, 999.99), 2),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }

                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=r_key,
                    body=json.dumps(order_data),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

                # 没有任何条件限制：绝对的每一条都打印！
                print(f"📤 [{q_name} ({q_idx}/{TOTAL_QUEUES})] [{i}/{MSGS_PER_QUEUE}] 已发送订单: {order_data['order_id']} (金额: ¥{order_data['amount']}) [全局累计: {global_counter:,}/{TOTAL_MSGS:,}]")

    except KeyboardInterrupt:
        print("\n⚠️ 用户手动中断了压测。")
    except Exception as e:
        print(f"\n❌ 压测过程中发生异常: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()

    total_time = time.time() - start_time
    print(f"\n🎉 压测结束！总耗时: {total_time:.2f} 秒")

if __name__ == '__main__':
    main()