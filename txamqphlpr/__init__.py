# -*- coding: utf-8 -*-

import os

import txamqp
import txamqp.queue
import amqp

Content = txamqp.content.Content

class RabbitMQFactory(amqp.AmqpFactory):
    specfile = "%s/amqp0-8.stripped.rabbitmq.xml" % os.path.dirname(os.path.abspath(__file__))
    spec = txamqp.spec.load(specfile)

#####

VERSION = (1, 0)

__version__ = '.'.join([str(i) for i in VERSION])
