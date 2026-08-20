import pika
import json
import threading
import time
import boto3
from botocore.exceptions import ClientError

def get_secret_from_aws(secret_name="secret-for-rabbitmq-int", region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"❌ 获取 AWS 密钥失败: {e}")
        raise e

# 单个 Worker 线程：负责监听分配给它的队列列表
def worker_consumer(worker_id, assigned_queues):
    try:
        creds = get_secret_from_aws()
        # 每个线程必须拥有独立的 connection 和 channel（pika 不是线程安全的）
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=creds.get('host'),
                port=5672,
                credentials=pika.PlainCredentials(creds.get('username'), creds.get('password')),
                heartbeat=600
            )
        )
        channel = connection.channel()
        # 提高预取计数，允许批量预取消息以加快消费吞吐
        channel.basic_qos(prefetch_count=50)

        # 定义回调函数
        def make_callback(q_name):
            def callback(ch, method, properties, body):
                try:
                    msg = json.loads(body.decode())
                    # 实时打印消费详情（你可以根据需要选择注释掉或保留）
                    print(f"📥 [Worker-{worker_id:02d}] 消费队列 [{q_name}] -> 订单号: {msg.get('order_id')} (金额: ¥{msg.get('amount')})")
                    
                    # 手动 ACK 确认消息处理完成
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as ex:
                    print(f"❌ 处理消息异常: {ex}")
                    # 拒绝并让消息重新入队，或者根据需要丢弃
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return callback

        # 循环为该线程负责的所有队列绑定监听
        for q_name in assigned_queues:
            channel.queue_declare(queue=q_name, durable=True)
            channel.basic_consume(
                queue=q_name, 
                on_message_callback=make_callback(q_name), 
                auto_ack=False
            )

        print(f"👷 [Worker-{worker_id:02d}] 已成功接管 {len(assigned_queues)} 个队列，开始全速消费...")
        channel.start_consuming()

    except Exception as e:
        print(f"❌ [Worker-{worker_id:02d}] 线程异常退出: {e}")

if __name__ == '__main__':
    TOTAL_QUEUES = 300
    # 生成 300 个队列的名称列表
    all_queues = [f"queue_ext_{i:03d}" for i in range(1, TOTAL_QUEUES + 1)]

    # 设定启动多少个并发消费线程（例如：启动 20 个 Worker 线程来分担 300 个队列）
    TOTAL_WORKERS = 20
    print(f"🚀 初始化消费集群：总计 {TOTAL_QUEUES} 个队列，将由 {TOTAL_WORKERS} 个并发 Worker 线程协同消费...\n")

    # 将 300 个队列平均分配给各个 Worker 线程
    queues_per_worker = TOTAL_QUEUES // TOTAL_WORKERS
    threads = []

    for i in range(TOTAL_WORKERS):
        start_idx = i * queues_per_worker
        # 最后一个线程负责收尾剩下的所有队列
        if i == TOTAL_WORKERS - 1:
            worker_queues = all_queues[start_idx:]
        else:
            worker_queues = all_queues[start_idx:start_idx + queues_per_worker]

        t = threading.Thread(target=worker_consumer, args=(i + 1, worker_queues))
        t.daemon = True
        threads.append(t)
        t.start()
        time.sleep(0.05) # 错开连接时间，防止瞬间连接风暴

    print(f"\n✨ 300队列消费集群已全部启动运行！按 Ctrl+C 可以随时安全退出。")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⚠️ 用户手动终止了消费集群。")