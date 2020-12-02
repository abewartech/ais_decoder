#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created April 2020
@author: Rory

This hold the sql definitions for the AIS db inserter that match to the ouputs of the various AIS decoders.

""" 
import logging
log = logging.getLogger('main.sql') 

def sql_for_type123s(): 
        #This depends on the DB structure.
        # Type 3 libais output:
        # ['id', 'repeat_indicator', 'mmsi', 'nav_status', 'rot_over_range', 'rot',
        #    'sog', 'position_accuracy', 'x', 'y', 'cog', 'true_heading',
        #    'timestamp', 'special_manoeuvre', 'spare', 'raim', 'sync_state',
        #    'slot_increment', 'slots_to_allocate', 'keep_flag']
        # additional columns are 'event_time' and 'routing_key'
        #  
    sql = ''' INSERT INTO ais.pos_reports
        (
        mmsi,
        navigation_status,
        rot,
        sog,
        longitude,
        latitude,
        position,
        cog,
        hdg,
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''
         
         #This depends on the Decoder output structure
    sql_template = ''' 
        ( 
        %(mmsi)s,
        %(nav_status)s,
        %(rot)s,
        %(sog)s,
        %(x)s,
        %(y)s,
        ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326),
        %(cog)s,
        %(true_heading)s,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )
         '''
    return sql, sql_template

def sql_for_type5s():
    # # ['id', 'repeat_indicator', 'mmsi', 'ais_version', 'imo_num', 'callsign',
    #    'name', 'type_and_cargo', 'dim_a', 'dim_b', 'dim_c', 'dim_d',
    #    'fix_type', 'eta_month', 'eta_day', 'eta_hour', 'eta_minute', 'draught',
    #    'destination', 'dte', 'spare']
    sql = ''' INSERT INTO ais.voy_reports
        (
        mmsi,
        imo,
        callsign,
        name,
        type_and_cargo,
        to_bow,
        to_stern,
        to_port,
        to_starboard,
        fix_type,
        eta_month,
        eta_day,
        eta_hour,
        eta_minute,
        eta,
        draught,
        destination, 
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''
         
         #This depends on the Decoder output structure
    sql_template = ''' 
        ( 
        %(mmsi)s,
        %(imo_num)s,
        %(callsign)s,
        %(name)s,
        %(type_and_cargo)s,
        %(dim_a)s,
        %(dim_b)s,
        %(dim_c)s,
        %(dim_d)s,
        %(fix_type)s,
        %(eta_month)s,
        %(eta_day)s,
        %(eta_hour)s,
        %(eta_minute)s,
        %(eta)s,
        %(draught)s,
        %(destination)s,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )
         ''' 
    return sql, sql_template

def sql_for_type9s():
    #   ehhhhh   
    sql = ''
    sql_template = '' 
    return sql, sql_template

def sql_for_type18s():

    # ['id', 'repeat_indicator', 'mmsi', 'sog', 'position_accuracy', 'x', 'y',
    #     'cog', 'true_heading', 'timestamp', 'spare', 'raim', 'spare2',
    #     'unit_flag', 'display_flag', 'dsc_flag', 'band_flag', 'm22_flag',
    #     'mode_flag', 'commstate_flag']

    sql_voy = ''' INSERT INTO ais.pos_reports
        (
        mmsi,
        navigation_status,
        rot,
        sog,
        longitude,
        latitude,
        position,
        cog,
        hdg,
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''

    sql_template_voy = '''
    ( 
        %(mmsi)s,
        Null,
        Null,
        %(sog)s,
        %(x)s,
        %(y)s,
        ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326),
        %(cog)s,
        %(true_heading)s,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )'''

    
    sql_pos = ''' INSERT INTO ais.pos_reports
        (
        mmsi,
        navigation_status,
        rot,
        sog,
        longitude,
        latitude,
        position,
        cog,
        hdg,
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''

    sql_template_B_pos = '''
    ( 
        %(mmsi)s,
        Null,
        Null,
        %(sog)s,
        %(x)s,
        %(y)s,
        ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326),
        %(cog)s,
        %(true_heading)s,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )''' 

    return sql_voy, sql_template_voy, sql_pos, sql_template_B_pos

def sql_for_type19s():
    # THis has both pos and voy info, so generate two set of SQL to use
    # ['id', 'repeat_indicator', 'mmsi', 'sog', 'position_accuracy', 'x', 'y',
    #        'cog', 'true_heading', 'timestamp', 'spare', 'raim', 'fix_type',
    #        'spare2', 'name', 'type_and_cargo', 'dim_a', 'dim_b', 'dim_c', 'dim_d',
    #        'dte', 'assigned_mode', 'spare3']

    sql_voy = ''' INSERT INTO ais.voy_reports
        (
        mmsi,
        imo,
        callsign,
        name,
        type_and_cargo,
        to_bow,
        to_stern,
        to_port,
        to_starboard,
        fix_type,
        eta_month,
        eta_day,
        eta_hour,
        eta_minute,
        eta,
        draught,
        destination, 
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''

    sql_template_voy = ''' 
        ( 
        %(mmsi)s,
        Null,
        Null,
        %(name)s,
        %(type_and_cargo)s,
        %(dim_a)s,
        %(dim_b)s,
        %(dim_c)s,
        %(dim_d)s,
        %(fix_type)s,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )
         ''' 

    sql_pos = ''' INSERT INTO ais.pos_reports
        (
        mmsi,
        navigation_status,
        rot,
        sog,
        longitude,
        latitude,
        position,
        cog,
        hdg,
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''

    sql_template_B_pos = '''
    ( 
        %(mmsi)s,
        Null,
        Null,
        %(sog)s,
        %(x)s,
        %(y)s,
        ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326),
        %(cog)s,
        %(true_heading)s,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )''' 

    return sql_voy, sql_template_voy, sql_pos, sql_template_B_pos


def sql_for_type21s():

    # ['id', 'repeat_indicator', 'mmsi', 'nav_status', 'rot_over_range', 'rot',
    #        'sog', 'position_accuracy', 'x', 'y', 'cog', 'true_heading',
    #        'timestamp', 'special_manoeuvre', 'spare', 'raim', 'sync_state',
    #        'slot_timeout', 'slot_number', 'received_stations', 'slot_increment',
    #        'slots_to_allocate', 'keep_flag', 'utc_hour', 'utc_min', 'utc_spare',
    #        'slot_offset', 'year', 'month', 'day', 'hour', 'minute', 'second',
    #        'fix_type', 'transmission_ctl', 'spare2', 'unit_flag', 'display_flag',
    #        'dsc_flag', 'band_flag', 'm22_flag', 'mode_flag', 'commstate_flag',
    #        'commstate_cs_fill', 'name', 'type_and_cargo', 'dim_a', 'dim_b',
    #        'dim_c', 'dim_d', 'dte', 'assigned_mode', 'spare3']

    # TODO: Got to add columns specific to this somewhere.
    sql = ''' INSERT INTO ais.voy_reports
        (
        mmsi,
        imo,
        callsign,
        name,
        type_and_cargo,
        to_bow,
        to_stern,
        to_port,
        to_starboard,
        fix_type,
        eta_month,
        eta_day,
        eta_hour,
        eta_minute,
        eta,
        draught,
        destination, 
        event_time,
        server_time,
        msg_type, 
        routing_key
        ) VALUES %s'''

    sql_template = ''' 
        ( 
        %(mmsi)s,
        Null,
        Null,
        %(name)s,
        %(type_and_cargo)s,
        %(dim_a)s,
        %(dim_b)s,
        %(dim_c)s,
        %(dim_d)s,
        %(fix_type)s,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        %(event_time)s,
        now(),
        %(id)s,
        %(routing_key)s
        )
         ''' 

    return sql, sql_template