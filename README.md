# python-rabbitmq
devops demo

## Usage a python virtual env

```shell

sudo apt install python3.14-venv

python3 -m venv venv


source venv/bin/activate

# optional if you need
pip install --upgrade pip


pip install -r requirements.txt


python3 test_rabbitmq.py

```
为什么使用 rabbitmq ?

同步（传统调用）：比如用户在网页点击“下单”，前端发起请求给后端，后端必须当场去扣库存、发短信、写日志，全部搞完才给用户返回“下单成功”。如果其中一个环节卡住，用户就要一直转圈等待。

异步（MQ 解耦）：用户点击“下单”后，后端把订单数据往 RabbitMQ 一扔，立刻就返回给用户“下单成功”（毫秒级响应）。至于后面的扣库存、发短信、写日志，由后台的消费者进程异步地去队列里慢慢消费。

结论：RabbitMQ 解决的是系统与系统之间、模块与模块之间的异步解耦和削峰填谷。
