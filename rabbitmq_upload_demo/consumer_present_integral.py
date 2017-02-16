#!/usr/bin/env python
#-*- coding:utf-8 -*-

import pika
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class Consumer(object):
    """docstring for Consumer"""
    _connection = None
    _exchange_type = "fanout"

    def __init__(self, username='guest', password='guest', host='localhost',
                       port=5672, vhost='/', queue='', exchange='',
                       routing_key=''):
        super(Consumer, self).__init__()
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._channel = None
        self._closing = True

    def connect(self):
        """通过实例初始化的参数创建rabbit连接"""
        cred = pika.PlainCredentials(self._username, self._password) # 认证
        # 连接参数
        conn_params = pika.ConnectionParameters(host = self._host,
                                                virtual_host = self._vhost,
                                                credentials = cred)
        # 返回 Select连接
        return pika.SelectConnection(parameters = conn_params,
                                     on_open_callback = self.on_connected)

    def on_connected(self, connection):
        """当链接成功调用"""
       
        self._connection = connection
        self.on_close_callback()
        self.open_channel()

    def open_channel(self):
   
        self._connection.channel(on_open_callback=self.on_channel_opened)

    def on_channel_opened(self, channel):
        """当channel打开的时候调用定义交换器(exchange)"""
       
        self._channel = channel
        self.on_close_callback()
       
        # 公平分发。这样一次一个消费者处理一个任务，当准备分发任务时候，
        # 发现该消费者已经有任务，将会分配给另外一个消费者去处理任务
        self._channel.basic_qos(prefetch_count=1)
        self._channel.exchange_declare(callback = self.on_exchange_delareok,
                                       exchange = self._exchange,
                                       exchange_type = self._exchange_type,
                                       durable = True)  # 持久化

    def on_exchange_delareok(self, unused_frame):
        """定义队列"""
       
        self.on_close_callback()
        self._channel.queue_declare(callback = self.on_queue_declareok,
                                    queue = self._queue,
                                    durable = True, # 持久化
                                    passive = False, # 持久化
                                    auto_delete = False)  # 是否不确认

    def on_queue_declareok(self, method_frame):
        """调动绑定交换器(exchange)和队列(queue)"""
       
        self._channel.queue_bind(callback = self.on_queue_bindok,
                                 exchange = self._exchange,
                                 queue = self._queue,
                                 routing_key = self._routing_key)

    def on_queue_bindok(self, unused_frame):
        """调用消费"""
       
        self._channel.basic_consume(consumer_callback = self.handler_task)

    def handler_task(self, channel, method, properties, body):
        """获取信息并进行消费
        Args:
            channel: pika.Channel
            method: pika.spec.Basic.Deliver
            properties: pika.spec.BasicProperties
            body: 接收的消息
        """
       
        print '---------------------'
        print 'present intergral consumer ...'
        print properties
        print body
        print '---------------------'
        # 使用获得的消息做一些事
        # 事情做完了进行确认(告诉生产者收到消息了)
        self._channel.basic_ack(method.delivery_tag)

    def on_close_callback(self):
        """当发生异常，关闭 connection"""
                
        self._connection.add_on_close_callback(self.connection_close)

    def connection_close(self):
        """关闭连接"""
       
        self._connection.close()

    def reconnect(self):
        """重新连接"""
       
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def start(self):
        """开始消费"""
        print "starting consumer..."
        self._closing = False
       
        self._connection = self.connect()
        # 启动
        self._connection.ioloop.start()

    def restart(self):
        """重启消费者"""
       
        self.stop()
        self.start()

    def close(self):
        """关闭连接"""
       
        self.connection_close()
    def stop(self):
        """停止消费"""
       
        print "stopping consumer..."
        self._closing = True
        self._connection.ioloop.stop()

def run():
    consumer_conf = {
        'username': 'alert_user',
        'password': 'oracle',
        'host': '192.168.1.233',
        'port': 5672,
        'queue': 'present_intergral',
        'exchange': 'upload_exchange',
        'routing_key': '',
        'vhost': '/',
    }
    consumer = Consumer(**consumer_conf)
    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()
    except:
        consumer.restart()
if __name__ == '__main__':
    run()
