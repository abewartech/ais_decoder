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
import lib.funcs
import lib.file_streamer

log = logging.getLogger('main')
# log.setLevel('DEBUG')

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
    cfg_object = lib.funcs.read_env_vars()
    # log.warning(os.listdir(cfg_object['file_folder']))
    time.sleep(30)
    lib.file_streamer.file_streamer(cfg_object)
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