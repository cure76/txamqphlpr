# -*- coding: utf-8 -*-

import sys
import os 

from twisted.python import log

from twisted.internet import reactor
from twisted.internet import defer 
from twisted.internet import protocol

from twisted.internet.defer import inlineCallbacks, Deferred

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp


class AmqpProtocol(AMQClient):
    """The protocol is created and destroyed each time a connection is created and lost."""
    
    ch = None
    
    def connectionMade(self):
        """Called when a connection has been made."""
        AMQClient.connectionMade(self)

        # Flag that this protocol is not connected yet.
        self.connected = False

        # Authenticate.
        deferred = self.start({"LOGIN": self.factory.user, "PASSWORD": self.factory.password})
        deferred.addCallback(self._authenticated)
        deferred.addErrback(self._authentication_failed)

    def _authenticated(self, ignore):
        """Called when the connection has been authenticated."""

        # Get a channel.
        d = self.channel(1)
        d.addCallback(self._got_channel)
        d.addErrback(self._got_channel_failed)

    def _got_channel(self, ch):
        self.ch = ch

        d = self.ch.channel_open()
        d.addCallback(self._channel_open)
        d.addErrback(self._channel_open_failed)

    def _channel_open(self, arg):
        """Called when the channel is open."""

        # Flag that the connection is open.
        self.connected = True

        # Now that the channel is open add any readers the user has specified.
        for consumer_kwargs in self.factory.read_list:
            consumer_kwargs and self.consumer(**consumer_kwargs)

        # Send any messages waiting to be sent.
        self.publish()

        # Fire the factory's 'initial connect' deferred if it hasn't already
        if not self.factory.initial_deferred_fired:
            self.factory.deferred.callback(self)
            self.factory.initial_deferred_fired = True

    def consume(self, **kwargs):
        """ """
        self.connected and self.consumer(**kwargs) or log.err("Unable connection for consuming")

    # Send all messages that are queued in the factory.
    def publish(self):
        """If connected, send all waiting messages."""
        if self.connected:
            while len(self.factory.queued_messages) > 0:
                m = self.factory.queued_messages.pop(0)
                self.publisher(**m)

    @inlineCallbacks
    def consumer(self, **kwargs):
        """
        """
        # exchange declare
        if "exchange_declare" in kwargs and "exchange" in kwargs["exchange_declare"]:
            """ yield self.ch.exchange_declare(exchange="RDB.Experts", type="topic", durable=True) """
            yield self.ch.exchange_declare(**kwargs["exchange_declare"])
            log.msg(
                "consumer exchange declared: %s" % kwargs["exchange_declare"], 
                level=log.logging.DEBUG
            )

        # queue declare
        if "queue_declare" in kwargs and "queue" in kwargs["queue_declare"]:
            """ yield self.ch.queue_declare(queue="RDB.Experts.mq_request", durable=True, exclusive=False) """
            yield self.ch.queue_declare(**kwargs["queue_declare"])
            log.msg(
                "consumer queue declared: %s" % kwargs["queue_declare"], 
                level=log.logging.DEBUG
            )

        # queue bind
        if "queue_bind" in kwargs and "queue" in kwargs["queue_bind"]:
            """ yield self.ch.queue_bind(queue="RDB.Experts.mq_request", exchange="RDB.Experts", routing_key="rdb.expert.request.isaran") """
            yield self.ch.queue_bind(**kwargs["queue_bind"])
            log.msg(
                "consumer queue bind: %s" % kwargs["queue_bind"], 
                level=log.logging.DEBUG
            )

        # basic qos
        if "basic_qos" in kwargs and "prefetch_count" in kwargs["basic_qos"]:
            """ yield self.ch.basic_qos(prefetch_count=1) """
            yield self.ch.basic_qos(**kwargs["basic_qos"])
            log.msg(
                "consumer set basic_qos: %s" % kwargs["basic_qos"], 
                level=log.logging.DEBUG
            )
            
        # basic consume
        if "basic_consume" in kwargs:
            """ """
            _default_callback = lambda ch, msg:( 
                log.msg("message: %s" % msg, level=log.logging.DEBUG) 
                    or log.msg(self.ch.basic_ack(delivery_tag=msg.delivery_tag), level=log.logging.DEBUG)
            )
            _default_errback = lambda failure: log.err(failure)
            
            """ yield self.ch.basic_consume(queue="RDB.Experts.mq_request", no_ack=False) """
            req = yield self.ch.basic_consume(**kwargs["basic_consume"])
            log.msg(
                "consumer set basic_consume: %s" % kwargs["basic_consume"], 
                level=log.logging.DEBUG
            )
            queue = yield self.queue(req.consumer_tag)
            d = queue.get()
            d.addCallback(self._read_item, queue, self.ch, kwargs.get("callback", _default_callback), kwargs.get("errback", _default_errback))
            d.addErrback(kwargs.get("errback", _default_errback))

    def _channel_open_failed(self, failure):
        log.err("Channel open failed.")
        log.err(failure)

    def _got_channel_failed(self, failure):
        log.err("Error getting channel.")
        log.err(failure)

    def _authentication_failed(self, failure):
        log.err("AMQP authentication failed.")
        log.err(failure)

    @inlineCallbacks
    def publisher(self, **kwargs):
        """ """
        yield
        # declare exchange
        if "exchange" in kwargs and isinstance(kwargs["exchange"], dict) and "exchange" in kwargs["exchange"]:
            yield self.ch.exchange_declare(**kwargs["exchange"])
            log.msg(
                "Exchange declared: %s" % kwargs["exchange"], 
                level=log.logging.DEBUG
            )
        # publish
        if "publish" in kwargs and "content" in kwargs and isinstance(kwargs["content"], Content):
            d = self.ch.basic_publish(content=kwargs["content"], **kwargs["publish"])
            d.addCallback(kwargs["publish"].get(
                "callback", lambda _: log.msg("Success published: %s" % kwargs["publish"], level=log.logging.DEBUG))
            )
            d.addErrback(kwargs["publish"].get("errback", lambda failure: log.err()))

    def _read_item(self, item, queue, ch, callback, errback):
        """Callback function which is called when an item is read."""
        # Setup another read of this queue.
        d = queue.get()
        d.addCallback(self._read_item, queue, ch, callback, errback)
        d.addErrback(errback)
#        d.addErrback(self._read_item_err)
        # Process the read item by running the callback.
        callback(ch, item)

    def _read_item_err(self, failure):
        log.err("Error reading item") 
        txamqp.queue.Closed is failure.trap(txamqp.queue.Closed)
        log.err(failure)

class AmqpFactory(protocol.ReconnectingClientFactory):
    
    protocol = AmqpProtocol
    
    spec = txamqp.spec.load("%s/amqp0-8.stripped.rabbitmq.xml" % os.path.dirname(os.path.abspath(__file__)))

    def __init__(self, vhost=None, host=None, port=None, user=None, password=None):
        self.user = user or "guest"
        self.password = password or "guest"
        self.vhost = vhost or "/"
        self.host = host or "localhost"
        self.port = port or 5672
        self.delegate = TwistedDelegate()
        self.deferred = Deferred()
        self.initial_deferred_fired = False

        self.p = None # The protocol instance.
        self.client = None # Alias for protocol instance

        self.queued_messages = [] # List of messages waiting to be sent.
        self.read_list = [] # List of queues to listen on.

        # Make the TCP connection.
        reactor.connectTCP(self.host, self.port, self)

    def buildProtocol(self, addr):
        p = self.protocol(self.delegate, self.vhost, self.spec)
        p.factory = self # Tell the protocol about this factory.

        self.p = p # Store the protocol.
        self.client = p

        # Reset the reconnection delay since we're connected now.
        self.resetDelay()

        return p

    def clientConnectionFailed(self, connector, reason):
        log.err("Connection failed")
        log.err(reason)
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        log.err("Client connection lost") 
        log.err(reason)
        self.p = None
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def publisher(self, **kwargs):
        """ queued publisher """
        # Add the new message arguments to the queue.
        self.queued_messages.append(kwargs)
        # This tells the protocol to send all queued messages.
        self.p is not None and self.p.publish()

    def consumer(self, **kwargs):
        """ queued consumer """
        # Add this to the read list so that we have it to re-add if we lose the connection.
        self.read_list.append(kwargs)
        # Tell the protocol to read this if it is already connected.
        self.p is not None and self.p.consume(**kwargs)

