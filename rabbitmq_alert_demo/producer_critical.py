#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pika
import sys
import time
import itertools
from pika import spec

reload(sys)
sys.setdefaultencoding('utf-8')

class Producer(object):
    """docstring for Producer"""

    _connection = None
    _exchange_type = "topic"
    _published = 0
    _confirmed = 0
    _errors = 0

    def __init__(self, username='guest', password='guest', host='localhost',
                       port=5672, vhost='/', queue='', exchange='',
                       routing_key=''):
        super(Producer, self).__init__()
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
        return pika.BlockingConnection(parameters = conn_params)

    def loop_produce(self, cnt=0):
        """循环生产新消息"""
        # 添加confirm机制
        self._channel.confirm_delivery()
        # delivery_mode 为 2 是使消息持久化
        properties = pika.BasicProperties(content_type = 'application/plain',
                                          delivery_mode = 2)
        # 构建循环次数变量
        loop = xrange(cnt) if cnt else itertools.count(0, 1)
        for i in loop:
            self.produce(properties) # 产生消息
            self._published += 1
        self.stop()

    def produce(self, properties=None):
        """生产消息到消息队列中"""
        self.msg = self.create_msg() # 创建具体消息
        is_ok = self._channel.basic_publish(
                                    body = self.msg,
                                    exchange = self._exchange,
                                    routing_key = self._routing_key,
                                    properties = properties,
                                    mandatory = False)
        if is_ok:
            self._confirmed += 1
        else:
            self._errors += 1

    def create_msg(self):
        """生成消息的逻辑在这边
        Args: None
        Return:
            msg: 返回一个字符串的消息
        Raise: None
        """
        return 'critical message'

    def start(self):
        """启动生产者"""
        print "starting producer..."
        if (not self._connection
                or not self._connection.is_open):
            self._connection = self.connect()
            self._channel = self._connection.channel()
        self.loop_produce(cnt = 100) # 开始消费定义消费次数

    def restart(self):
        """重启生产者"""
       
        self.stop()
        self.start()

    def stop(self):
        """停止连接"""
        print 'stopping produce...'
        if self._channel.is_open:
            self._channel.close()
        if self._connection.is_open:
            self._connection.close()

def run():

    producer_conf = {
        'username': 'alert_user',
        'password': 'oracle',
        'host': '192.168.1.233',
        'port': 5672,
        'queue': 'critical',
        'exchange': 'alerts',
        'routing_key': 'critical.*',
        'vhost': '/',
    }

    producer = Producer(**producer_conf)

    try:
        producer.start()
    except KeyboardInterrupt:
        producer.stop()
    except:
        producer.restart()
if __name__ == '__main__':
    run()
