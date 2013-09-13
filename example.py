# -*- coding: utf-8 -*-

import sys
import time

from twisted.python import log

from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import task

from txamqphlpr import RabbitMQFactory
from txamqphlpr import Content

def publisher_example():
    msg = Content(body="TEST", properties={"delivery mode":2})
    exchange={"exchange":"RDB.Experts", "type":"topic", "durable":True}
    publish={"exchange": "RDB.Experts", "routing_key":"rdb.expert.request.isaran"}
    amqp.publisher(exchange=exchange, publish=publish, content=msg)

@defer.inlineCallbacks
def consumer_callback(ch, msg):
    log.msg("Message: %s" % msg)
    yield ch.basic_ack(delivery_tag=msg.delivery_tag)

#@defer.inlineCallbacks
def consumer_errback(failure):
    log.err(failure)

def consumer_example(callback, errback):
    exchange_declare_args={"exchange":"RDB.Experts", "type":"topic", "durable": True}
    queue_declare_args={"queue":"RDB.Experts.mq_request", "durable":True, "exclusive": False}
    queue_bind_args={"queue":"RDB.Experts.mq_request", "exchange": "RDB.Experts", "routing_key":"rdb.expert.request.isaran"}
    basic_qos_args={"prefetch_count":1}
    basic_consume_args={"queue":"RDB.Experts.mq_request", "no_ack":False}
    
    amqp.consumer(
        exchange_declare=exchange_declare_args, 
        queue_declare=queue_declare_args, 
        queue_bind=queue_bind_args, 
        basic_qos=basic_qos_args, 
        basic_consume=basic_consume_args,
        callback = callback, 
        errback = errback
    )

if __name__ == '__main__':
    """
    """
    
    log.startLogging(sys.stdout)
    
    amqp = RabbitMQFactory(vhost="topview", host="localhost", port=5672, user="guest", password="guest")

    # run periodic publisher
    task.LoopingCall(publisher_example).start(1)
    
    # run consumer with callback and errback
    consumer_example(consumer_callback, consumer_errback)

    reactor.run()
