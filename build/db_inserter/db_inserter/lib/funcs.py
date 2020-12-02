#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 12:15:15 2017

@author: rory
""" 
import logging
import logging.handlers
import time
import os
import subprocess
import json
import datetime
import pytz
import psycopg2
 
from kombu import Connection, Exchange, Producer, Queue
from kombu.mixins import ConsumerMixin

from . import db_inserter

log = logging.getLogger('main.funcs') 

    # os.getenv('')
class Worker(ConsumerMixin):
    def __init__(self, conn, queues):
        log.info('Starting up Rabbit Consumer')
        self.connection = conn 
        self.queue = queues
        self.msg_proc = db_inserter.MessageProcessor()

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self.queue, callbacks=[self.on_message], accept=['json']),
        ]
        
    def unknown_to_dict_list(self,body):

        msg_dict_list = [{}]

        if type(body) is str:
            msg = json.loads(body)
            if type(msg) is list:
                msg_dict_list = msg
            elif type(msg) is dict:
                msg_dict_list = [msg]
            else:
                log.warning('Unconverted str in dict_list: %s',body)
        elif type(body) is dict:
            msg_dict_list = [body]
        elif type(body) is list:
            msg_dict_list = body
        elif body is None:
            log.warning('None-type in dict_list: %s',body)
        else:
            log.warning('Unconverted kak in dict_list: %s',body)

        return msg_dict_list
    def on_message(self, body, message):
        log.debug('Msg type %s received: %s',type(body),body)
        if message.delivery_info['redelivered']:
            message.reject()
            return
        else:
            message.ack()
            
        msg_dict_list = self.unknown_to_dict_list(body)
        try:
            for msg in msg_dict_list: 
                self.msg_proc.proc_message(msg)
                    
        except Exception as err:
                log.error('Error in message consumer: {0}'.format(err))

class Rabbit_Wrapper(object):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self):
        '''
        setup all the goodies
        '''
        log.debug('Setting up RabbitMQ interface...')

        user = os.getenv('RABBITMQ_DEFAULT_USER')
        password = os.getenv('RABBITMQ_DEFAULT_PASS')
        host = os.getenv('RABBIT_HOST')
        port = os.getenv('RABBIT_MSG_PORT')
        sink_topic_exchange_name= os.getenv('RABBIT_EXCHANGE')
 
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(user, password, host, port)
        log.debug('Rabbit is at {0}'.format(self.rabbit_url))
        self.sink_topic_exchange = Exchange(sink_topic_exchange_name, type="topic")
        self.conn = Connection(self.rabbit_url)
        self.sink = Producer(exchange=self.sink_topic_exchange,
                              channel=self.conn,
                              serializer ='json')
        log.info('Rabbit Wrapper initialised.')
        log.info('Creating Test Queue')
        self.create_test_queue()
        log.info('Done')

    def errback(self, exc, interval):
        log.warning('Produce error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)

    def produce_msg(self, msg_dict):
        '''
        send the info to all the right places
        '''
        payload = msg_dict
        payload_routing_key = msg_dict['routing_key']
        producer = self.conn.ensure(self.sink, self.sink.publish, errback=self.errback, interval_start = 1.0)
        producer(payload, routing_key=payload_routing_key)
        log.debug(' -Sent to Rabbit exchange: %s >> %s',self.sink_topic_exchange, payload_routing_key)

    def create_test_queue(self):
        queue = Queue(name="AAA-db-sink-test-queue", 
                      exchange=self.sink_topic_exchange,
                      max_length = 1000, 
                      routing_key=os.getenv('SOURCE_RKEY'))
        queue.maybe_bind(self.conn)
        queue.declare()
        return