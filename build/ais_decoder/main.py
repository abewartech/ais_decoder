#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 15:05:19 2017

@author: rory
"""

import argparse
import logging
import os
import sys
import time
import traceback

import lib.ais_decoder
import lib.rabbit
from kombu import Connection

log = logging.getLogger("main")


# log.setLevel('DEBUG')
def do_work():
    """
    Message worker: consume > decode > publish.
    """
    log.info("Getting ready to do work...")

    # This pulls/pushes messages from RMQ
    # rabbit_interface = lib.rabbit.Rabbit_ConsumerProducer()
    rabbit_interface = lib.rabbit.DockerRabbit_CP()

    ais_message_format = lib.ais_decoder.Basic_AIS()
    ais_worker = lib.ais_decoder.AIS_Decoder(ais_message_format)
    rabbit_interface.message_processor = ais_worker.message_processor

    # This decodes messages and is used to overload the default function in the
    #   Consumer/Producer

    time.sleep(2)
    # ================
    # https://www.fugue.co/blog/diagnosing-and-fixing-memory-leaks-in-python.html
    import tracemalloc

    tracemalloc.start(10)
    try:
        with Connection(rabbit_interface.rabbit_url, heartbeat=20) as conn:
            rabbit_interface.connection = conn
            log.info("Waiting for incoming messages...")
            rabbit_interface.run()  # calls dummy_handler.on_message(message)
    except Exception as e:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        log.error(e)
        log.error(" ==== [ Top 10 ] ====")
        for stat in top_stats[:10]:
            log.error(stat)

    log.info("Worker shutdown...")


def main(args):
    """
    Setup logging, and args, then "do_work"
    """
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        level=getattr(logging, args.loglevel),
    )

    log.setLevel(getattr(logging, args.loglevel))
    log.info("ARGS: {0}".format(ARGS))
    do_work()
    log.warning("Script Ended...")


if __name__ == "__main__":
    """
    This takes the command line args and passes them to the 'main' function
    """
    PARSER = argparse.ArgumentParser(description="Run the DB inserter")
    PARSER.add_argument(
        "-f",
        "--folder",
        help="This is the folder to read.",
        default=None,
        required=False,
    )
    PARSER.add_argument(
        "-ll",
        "--loglevel",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set log level for service (%s)" % "INFO",
    )
    ARGS = PARSER.parse_args()
    try:
        main(ARGS)
    except KeyboardInterrupt:
        log.warning("Keyboard Interrupt. Exiting...")
        os._exit(0)
    except Exception as error:
        log.error("Other exception. Exiting with code 1...")
        log.error(traceback.format_exc())
        log.error(error)
        os._exit(1)
