#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
""" 
import time as timer
import logging
import os
import re
import time
import psycopg2
from psycopg2.extras import execute_values
from . import funcs
from . import sql_funcs


log = logging.getLogger('main.message_proc') 

class MessageProcessor():
    # Feed this thing decoded AIS messages and it'll handle the bulk inserts into the DB.

    def __init__(self):
        log.info('Initialising message processor...')
        self.type_123s = []
        self.type_5s = []
        self.type_9s = []
        self.type_18s = []
        self.type_19s = []
        self.type_21s = []
        self.proc_counter = 0
        self.proc_timer = timer.time()

    def proc_message(self, msg):
        # Group message types and do insert
        log.debug('Handling message: {0}'.format(msg))
        self.proc_counter += 1
        if msg.get('id') in [1,2,3]:
            self.type_123s.append(msg)
        elif msg.get('id') in [5]:
            self.type_5s.append(msg)
        elif msg.get('id') in [9]:
            self.type_9s.append(msg)
        elif msg.get('id') in [18]:
            self.type_18s.append(msg)            
        elif msg.get('id') in [19]:
            self.type_19s.append(msg)
        elif msg.get('id') in [21]:
            self.type_19s.append(msg)
        else:
            log.info('Message ID unknown: {0}'.format(msg.get('id')))       
        self.bulk_insert()
        return

    def bulk_insert(self):
        # Check if it's time to do a bulk insert, then do it if it is.
        log.debug("Checking if it's time to do bulk insert...")
        time_now = timer.time()
        if (time_now - self.proc_timer) > int(os.getenv('INSERT_PERIOD')):
            try:
                log.debug('Doing bulk insert...')       
                self.insert_123s()
                self.insert_5s()
                self.insert_9s()
                self.insert_18s()
                self.insert_19s()
                self.insert_21s()
            except Exception as not_catched:
                log.error('Uncatched exception in History Insert.')
                log.error(not_catched)
            finally:
                log.info('{2} Msg/Sec. Processed {0} messages in {1} seconds.'.format(self.proc_counter,
                                                                                      (timer.time() - self.proc_timer),
                                                                                      self.proc_counter/(timer.time() - self.proc_timer)))
                self.proc_counter = 0
                self.proc_timer = timer.time()
        else:
            log.debug('Not time to do insert yet...')
        return

    def execute_sql(self, sql, sql_template, list_of_dicts):
        log.debug('Starting SQL execution...')
        conn = psycopg2.connect(host=os.getenv('DB_HOST'),
                                    dbname=os.getenv('POSTGRES_DB'),
                                    port=os.getenv('DB_INT_PORT'),
                                    user=os.getenv('POSTGRES_USER'),
                                    password=os.getenv('POSTGRES_PASSWORD'))
        cursor = conn.cursor()
        execute_values(cursor,
                    sql,
                    list_of_dicts, 
                    sql_template, 
                    page_size = 1000)
        conn.commit()
        log.debug('Finished SQL execution. Commited.')
        conn.close()
        return

    def insert_123s(self):
        try:
            sql, sql_template = sql_funcs.sql_for_type123s()
            self.execute_sql(sql, sql_template, self.type_123s)
            self.type_123s = []
        except Exception as identifier:
            log.warning('Insert 123:') 
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 123 messages waiting to get inserted...')
                self.type_9s = [] 
        return

    def insert_5s(self):
        #Type 5's have both position and voyage info. Insert to both tables.
        try:
            sql, sql_template = sql_funcs.sql_for_type5s()
            self.execute_sql(sql, sql_template, self.type_5s)
            self.type_5s = []
        except Exception as identifier:
            log.warning('Insert 5:')
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 5 messages waiting to get inserted...')
                self.type_9s = [] 
        return

    def insert_9s(self):
        try:
            # sql, sql_template = sql.sql_for_type9s()
            # self.execute_sql(sql, sql_template, self.type_9s)
            if len(self.type_9s) > 0:
                log.info('{0} type 9 messages not handled...'.format(len(self.type_9s)))
            self.type_9s = []
        except Exception as identifier:
            log.warning('Insert 9:')
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 9 messages waiting to get inserted...')
                self.type_9s = [] 
        return

    def insert_18s(self):
        try:
            sql_A, sql_template_A, sql_B, sql_template_B = sql_funcs.sql_for_type18s()
            self.execute_sql(sql_A, sql_template_A, self.type_18s)
            self.execute_sql(sql_B, sql_template_B, self.type_18s)
            self.type_18s = []
        except Exception as identifier:
            log.warning('Insert 18:')
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 18 messages waiting to get inserted...')
                self.type_18s = []  
        return

    def insert_19s(self):
        # Type 19 messages are a combo of voyage and pos reports. So insert happens twice.
        try:
            sql_A, sql_template_A, sql_B, sql_template_B = sql_funcs.sql_for_type19s()
            self.execute_sql(sql_A, sql_template_A, self.type_19s)
            self.execute_sql(sql_B, sql_template_B, self.type_19s)
            self.type_19s = []
        except Exception as identifier:
            log.warning('Insert 19:')
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 19 messages waiting to get inserted...')
                self.type_19s = [] 
        return

    def insert_21s(self):
        try:
            sql, sql_template = sql_funcs.sql_for_type21s()
            self.execute_sql(sql, sql_template, self.type_21s)
            self.type_21s = []
        except Exception as identifier:
            log.warning('Insert 21:')
            log.warning(identifier)
            if os.getenv('ON_ERROR_DROP_MSGS') == 'True':
                log.warning('Dropping type 21 messages waiting to get inserted...')
                self.type_21s = []
        return
