#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 15:05:19 2017

@author: rory
"""

import sys
import time
import argparse
import logging 
import os

from kombu import Connection, Exchange, Queue, binding

import lib.funcs
import lib.rabbit
import lib.ais_decoder

log = logging.getLogger('main')
# log.setLevel('DEBUG')

def dummy_handler(message):
    print(message)
    return

def do_work():  
    '''
    Message worker: consume > decode > publish.
    '''
    log.info('Getting ready to do work...')

    decoder = dummy_handler
    consumer = lib.rabbit.Rabbit_Consumer(decoder)  # This pulls encoded messages from RMQ and hands then to the decoder
    # producer = lib.rabbit.Rabbit_Producer()  # This pushes decoded messages to RMQ
    # decoder  = lib.ais_decoder.AIS_Worker()  # This decodes em. 

    time.sleep(10)
    with Connection(consumer.rabbit_url, heartbeat=20) as conn:
        # consumer_worker = consumer(conn, consumer.queues)
        log.info('Waiting for incoming messges...')
        consumer_worker.run()
 
    log.info('Worker shutdown...')

def main(args):
    '''
    Receive encoded AIS messages from RabbitMQ, decode them, and 
    republish them with a different routing key
    '''
    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        #level= log.debug)
        level=getattr(logging, args.loglevel))

    log.setLevel(getattr(logging, args.loglevel))
    log.info('ARGS: {0}'.format(ARGS))
    cfg_object = lib.funcs.read_env_vars()
    do_work()
    log.warning('Script Ended...') 

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