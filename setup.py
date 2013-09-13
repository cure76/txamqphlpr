try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
    
from txamqphlpr import __version__

setup(
    name= "txamqphlpr",
    version= __version__,
    description= "txamqp helper",
    author_email= "cure76@gmail.com",
    packages= ["txamqphlpr"],
    include_package_data=True,
    install_requires=["Twisted>=10.0", "txAMQP>=0.4"], 
    platforms='any', 
    package_data={'txamqphlpr': ['amqp0-8.stripped.rabbitmq.xml']}
)
