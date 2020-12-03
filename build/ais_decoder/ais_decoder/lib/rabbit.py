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
from kombu.mixins import ConsumerMixin

log = logging.getLogger('main.lib.rabbit')  
class Rabbit_Consumer(ConsumerMixin):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self, message_handler):
        '''
        Initialise the connection to the RMQ server and use the message_handler to 
        process incoming messages. 

        In most cases the message_handler will be the decoder but it could also
        be a filter, neural net, anomoly detector or what not.

        The message handler must accept kombu message object
        '''
        log.info('Setting up RabbitMQ source interface...') 

        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(os.getenv('SRC_RABBITMQ_DEFAULT_USER'),
                                                            os.getenv('SRC_RABBITMQ_DEFAULT_PASS'),
                                                            os.getenv('SRC_RABBIT_HOST'),
                                                            os.getenv('SRC_RABBIT_MSG_PORT'))
        log.debug('Source Rabbit is at {0}'.format(self.rabbit_url))
        self.exchange = Exchange(os.getenv('SRC_RABBIT_EXCHANGE'), type="topic")
        self.bind_to_keys()
        self.create_test_queue()
        self.conn = Connection(self.rabbit_url) #This connection is only used for the dummy queue... 
        self.message_handler = message_handler
        log.debug('Consumer init complete')  
        
    def on_message(self, body, message):
        log.debug('Msg type %s received: %s',type(body),body)
        if message.delivery_info['redelivered']:
            message.reject()
            return
        else:
            message.ack()
        try:            
            self.message_handler(message)
        except Exception as err:
                log.error('Error in message consumer: {0}'.format(err))
        
        
    
    def bind_to_keys(self):
        # takes the list of routing keys in the config file
        # and create a queue bound to them.
        log.info('Creating queue and binding keys')
        topic_binds = []
        keys = json.loads(os.getenv('SRC_KEYS'))
        for key in keys:
            log.info('    -Key: %s',key)
            # NOTE: don't declare queue name. It'll get auto generated and expire after 600 seconds of inactivity
            topic_bind = binding(self.exchange, routing_key=key)
            topic_binds.append(topic_bind)
        queue_name = os.getenv('INSERT_QUEUE')
        self.queues = Queue(name=queue_name,
                        exchange=self.exchange,
                        bindings=topic_binds,
                        max_length = 10000000)

        log.info('Source queues: %s',self.queues)
        return

    def errback(self, exc, interval):
        log.warning('Consumer error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)
        return
 
    def create_test_queue(self):
        # Create a dummy queue on the rabbitmq server. Useful for debugging
        log.debug('Creating Source Test Queue')  
        test_q_name = "AAA-{0}-test-consume".format(os.getenv('PROJECT_NAME'))
        queue = Queue(name=test_q_name, 
                        exchange=self.exchange,
                        max_length = 1000, 
                        routing_key=os.getenv('PRODUCE_KEY'))
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
        log.info('Creating Sink Test Queue')
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