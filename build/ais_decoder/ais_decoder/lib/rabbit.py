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
 
from kombu import Connection, Exchange, Producer, Queue, Consumer, binding


log = logging.getLogger('main.lib.rabbit') 
 
class Rabbit_Consumer(object):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self, CFG):
        '''
        setup all the goodies
        '''
        log.debug('Setting up RabbitMQ source interface...')
        self.cfg = CFG 

        user = CFG.get('src_rabbit_user')
        password = CFG.get('src_rabbit_pass')
        host = CFG.get('src_rabbit_host')
        port = CFG.get('src_rabbit_port')
        exchange_name= CFG.get('src_rabbit_exch')

        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(user, password, host, port)
        log.debug('Source Rabbit is at {0}'.format(self.rabbit_url))
        self.topic_exchange = Exchange(exchange_name, type="topic")
        self.conn = Connection(self.rabbit_url) 
        log.info('Creating Test Queue')
        self.create_test_queue()
        log.info('Done')

    def errback(self, exc, interval):
        log.warning('Produce error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)
 
    def create_test_queue(self):
        test_q_name = "AAA-{0}-test-consume".format(self.cfg['project_name'])
        queue = Queue(name=test_q_name, 
                        exchange=self.sink_topic_exchange,
                        max_length = 1000, 
                        routing_key=os.getenv('SOURCE_RKEY'))
        queue.maybe_bind(self.conn)
        queue.declare()
        return
 
class Rabbit_Producer(object):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self, CFG):
        '''
        setup all the goodies
        '''
        log.debug('Setting up RabbitMQ sink interface...')
        self.cfg = CFG 

        user = CFG.get('snk_rabbit_user')
        password = CFG.get('snk_rabbit_pass')
        host = CFG.get('snk_rabbit_host')
        port = CFG.get('snk_rabbit_port')
        sink_topic_exchange_name= CFG.get('snk_rabbit_exch')
 
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(user, password, host, port)
        log.debug('Sink Rabbit is at {0}'.format(self.rabbit_url))
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
        test_q_name = "AAA-{0}-test-produce".format(self.cfg['project_name'])
        queue = Queue(name=test_q_name, 
                      exchange=self.sink_topic_exchange,
                      max_length = 1000, 
                      routing_key=os.getenv('SOURCE_RKEY'))
        queue.maybe_bind(self.conn)
        queue.declare()
        return