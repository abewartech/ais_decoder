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
 
from kombu import Connection, Exchange, Producer, Queue


log = logging.getLogger('main.funcs') 

def read_env_vars():
    '''
    Read environ variables and return as dict. 

    This is to replace the config file function in preperation for rancher, where all config is handled by env vars.
    '''
    log.debug('Reading environment variables...')
    CFG = {}
    CFG['rabbit_port'] = os.getenv('RABBIT_MSG_PORT')
    CFG['rabbit_user'] = os.getenv('RABBITMQ_DEFAULT_USER')
    CFG['rabbit_pw'] = os.getenv('RABBITMQ_DEFAULT_PASS')
    CFG['rabbit_host'] = os.getenv('RABBIT_HOST')
    CFG['routing_key'] = os.getenv('SOURCE_RKEY')
    CFG['file_sleep'] = os.getenv('WAIT_BETWEEN_FILES')
    CFG['file_folder'] = os.getenv('CONTAINER_FILE_DIR')
    CFG['exchange'] = os.getenv('RABBIT_EXCHANGE')
    CFG['topic'] = os.getenv('RABBIT_TOPIC')
    # CFG[''] = os.getenv('')
    log.info('Config: {0}'.format(CFG))
    return CFG

class Rabbit_Wrapper(object):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self, cfg_object):
        '''
        setup all the goodies
        '''
        log.debug('Setting up RabbitMQ interface...')

        user = cfg_object.get('rabbit_user')
        password = cfg_object.get('rabbit_pw')
        host = cfg_object.get('rabbit_host')
        port = cfg_object.get('rabbit_port')
        sink_topic_exchange_name= cfg_object.get('exchange')
 
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
        queue = Queue(name="AAA-file-reader-test-queue", 
                      exchange=self.sink_topic_exchange,
                      max_length = 1000, 
                      routing_key=os.getenv('SOURCE_RKEY'))
        queue.maybe_bind(self.conn)
        queue.declare()
        return