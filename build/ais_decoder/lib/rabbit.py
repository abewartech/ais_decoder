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

from kombu import Connection, Consumer, Exchange, Queue
from kombu.mixins import ConsumerProducerMixin

log = logging.getLogger("main.lib.rabbit")


def errback(exc, interval):
    log.warning("Consumer error: %r", exc)
    log.warning("Retry in %s +1  seconds.", interval)
    time.sleep(float(interval) + 1)
    return


class Rabbit_Producer:
    def __init__(
        self,
        amqp_url,
        exchange_name,
        queue_name,
        connection_args={},
        exchange_args={"type": "topic"},
        queue_args={},
    ):
        connection_args.update({"hostname": amqp_url})
        exchange_args.update({"name": exchange_name})

        self.connection = Connection(**connection_args)
        self.exchange = Exchange(**exchange_args)
        queue_args.update(
            {
                "name": queue_name,
                "exchange": self.exchange,
                "channel": self.connection,
            }
        )

        self.queue = Queue(**queue_args)

    def send_message(self, message, routing_key):
        self.queue.routing_key = routing_key
        self.queue.declare()
        with self.connection.Producer() as producer:
            producer.publish(
                message,
                exchange=self.exchange,
                routing_key=routing_key,
                declare=[self.queue],
            )

    def produce(self, message, routing_key, errback_func=errback):
        producer = self.connection.ensure(
            self, self.send_message, errback=errback_func, interval_start=1.0
        )
        producer(message, routing_key=routing_key)


class Rabbit_CP(ConsumerProducerMixin):
    def __init__(
        self,
        amqp_url,
        exchange_to_consume,
        queue_to_consume,
        exchange_to_deliver,
        queue_to_deliver,
        connection_args={},
        exchange_args={"type": "topic"},
        queue_args={},
    ):
        # Connection configuration
        connection_args.update({"hostname": amqp_url})
        self.connection = Connection(**connection_args)

        # We need to connect to a exchange to be able to get/deliver
        #   from a Queue.
        # Think of exchange as the only way to interact with queue.
        # The exchange is just a stream, no memory
        # The queue drinks from the exchange, keeps in memory until read upon.

        # Consumer configuration, where to get messages from
        self.exchange_to_consume = exchange_to_consume
        queue_args.update(
            {"name": queue_to_consume, "channel": self.connection}
        )
        self.consumer_queue = Queue(**queue_args)

        # Producer Configuration, where to deliver messages
        self.exchange_to_deliver = exchange_to_deliver
        self.rabbit_producer = Rabbit_Producer(
            amqp_url,
            exchange_name=self.exchange_to_deliver,
            queue_name=queue_to_deliver,
            connection_args=connection_args,
            exchange_args=exchange_args,
        )

    def get_consumers(self, Consumer=Consumer, channel=None):
        return [
            Consumer(
                queues=self.consumer_queue,
                on_message=self.on_message,
                accept={"application/json"},
                prefetch_count=1,
            )
        ]

    def message_processor(self, message):
        pass

    def on_message(self, message):
        if message.delivery_info["redelivered"]:
            message.reject()
            return
        else:
            message.ack()
        proc_msg = self.message_processor(message)

        self.rabbit_producer.produce(
            proc_msg, routing_key=proc_msg["routing_key"]
        )

        return


class DockerRabbit_CP(Rabbit_CP):
    def __init__(self):
        log.info("Setting up RabbitMQ source/sink interface...")
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(
            os.getenv("SRC_RABBITMQ_DEFAULT_USER"),
            os.getenv("SRC_RABBITMQ_DEFAULT_PASS"),
            os.getenv("SRC_RABBIT_HOST"),
            os.getenv("SRC_RABBIT_MSG_PORT"),
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
        )
