#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 15:05:19 2017

@author: rory
"""
import sys
import argparse
import logging 
import os
import json
import time

from kombu import Connection, Exchange, Queue, binding

import lib.funcs
import lib.db_inserter

log = logging.getLogger('main')
# log.setLevel('DEBUG')

def do_work():  
    '''
    Post processing from message broker.
    '''
    log.info('Getting ready to do work...')
    time.sleep(30)
    user = os.getenv('RABBITMQ_DEFAULT_USER')
    password = os.getenv('RABBITMQ_DEFAULT_PASS')
    host = os.getenv('RABBIT_HOST')
    port = os.getenv('RABBIT_MSG_PORT')
    rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(user, password, host, port)
    
    exchange = Exchange(os.getenv('RABBIT_EXCHANGE'), type="topic") 
    topic_binds = []
 
    keys = json.loads(os.getenv('BIND_TO_KEYS'))
    for key in keys:
        log.info('Building queue for topic: %s',key)
        # NOTE: don't declare queue name. It'll get auto generated and expire after 600 seconds of inactivity
        topic_bind = binding(exchange, routing_key=key)
        topic_binds.append(topic_bind)
    '''
    Here's the main input queue. It binds to all the routing keys specified
    in the config file. The name is also specified there.
    The max_length is set massively so that if the consumer goes down it
    will take a while to fill but won't become a massive pig eating all the
    servers resources.
    '''
    queue_name = os.getenv('INSERT_QUEUE')
    queues = Queue(name=queue_name,
                    exchange=exchange,
                    bindings=topic_binds,
                    max_length = 10000000)

    log.info('Queues: %s',queues)
    with Connection(rabbit_url, heartbeat=20) as conn:
        worker = lib.funcs.Worker(conn, queues)
        worker.run()

    log.info('Worker shutdown...')


def main(args):
    '''
    Setup logging, read config, fire up the Rabbit and Database wrappers and
    then process some messages. Messages come from text file or from web server
    '''
    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        #level= log.debug)
        level=getattr(logging, args.loglevel))

    log.setLevel(getattr(logging, args.loglevel)) 
    log.info('ARGS: {0}'.format(ARGS))

    try:
        do_work()
    except Exception as error:
        log.warning('Inserter died: {0}'.format(error)) 

if __name__ == "__main__":
    '''
    This takes the command line args and passes them to the 'main' function
    '''
    PARSER = argparse.ArgumentParser(
        description='Run the DB inserter')
    PARSER.add_argument(
        '-f', '--folder', help='This is the folder to read.',
        default = None, required=False)
    PARSER.add_argument(
        '-ll', '--loglevel', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set log level for service (%s)" % 'INFO')
    ARGS = PARSER.parse_args()
    try:
        main(ARGS)
    except KeyboardInterrupt:
        log.warning('Keyboard Interrupt. Exiting...')
        # os._exit(0)
    except Exception as error:
        log.error('Other exception. Exiting with code 1...')
        log.error(error)
        # os._exit(1)