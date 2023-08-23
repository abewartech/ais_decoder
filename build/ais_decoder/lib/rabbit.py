#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 12:15:15 2017

@author: rory
"""
import logging
import logging.handlers
import os
import time

from rab_the_bit import RabbitConsumerProducer

log = logging.getLogger("main.lib.rabbit")


def errback(exc, interval):
    log.warning("Consumer error: %r", exc)
    log.warning("Retry in %s +1  seconds.", interval)
    time.sleep(float(interval) + 1)
    return


class DockerRabbit_CP(RabbitConsumerProducer):
    def __init__(self):
        log.info("Setting up RabbitMQ source/sink interface...")
        # Key to consume from:
        RABBITMQ_USER = os.getenv("SRC_RABBITMQ_DEFAULT_USER")
        RABBITMQ_PASS = os.getenv("SRC_RABBITMQ_DEFAULT_PASS")
        RABBIT_HOST = os.getenv("SRC_RABBIT_HOST")
        RABBIT_MSG_PORT = os.getenv("SRC_RABBIT_MSG_PORT")

        self.rabbit_url = (
            f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}"
            f"@{RABBIT_HOST}:{RABBIT_MSG_PORT}/"
        )
        log.debug("Source/Sink Rabbit is at {0}".format(self.rabbit_url))
        self.queue_to_consume = os.getenv("AISIMOV_RABBIT_QUEUE")
        self.exchange_to_consume = os.getenv("AISIMOV_DECODER_RABBIT_EXCHANGE")

        self.exchange_to_deliver = os.getenv("AISDECODER_RABBIT_EXCHANGE")
        self.queue_to_deliver = os.getenv("AISDECODER_RABBIT_QUEUE")

        super().__init__(
            self.rabbit_url,
            exchange_to_consume=self.exchange_to_consume,
            queue_to_consume=self.queue_to_consume,
            exchange_to_deliver=self.exchange_to_deliver,
            queue_to_deliver=self.queue_to_deliver,
            log=log,
            errback=errback,
        )
