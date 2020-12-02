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
 