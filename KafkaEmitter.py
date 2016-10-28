"""
Simple custom emitter for DataDog agent to produce messages to Kafka.
Enable it by adding the following lines to datadog.conf and restarting
the agent:
kafka_hosts: kafka-hostname:9092
custom_emitters: /path/to/KafkaEmitter.py
See the emitter documentation for additional optional settings.
Version: 0.0.1
"""

# stdlib
import logging
import simplejson as json

# 3p
from kafka import KafkaProducer

class emitter(object):
  """
  The kafkaemitter class is called from the collector with a dict message payload
  """

  def __init__(self):
    """
    Setup kafka connection. Configs:
      'kafka_topic': topic where messages are sent
      'kafka_bootstrap_servers': Comma separated list of bootstrap servers
    """
#    self.topic = agentConfig.get('kafka_topic', 'datadog')
    self.topic = 'datadog'
    self.dry_run = True
    self._producer = None

    logger = logging.getLogger('kafka')
    logger.setLevel(logging.DEBUG)


  def __call__(self, message, log, agent_config):
    """
      Send payload:
        message is a dictionary as collected by the agent
        logger is a logging.Logger object
        agentConfig is a dictionary of all the configuration values
    """
    if not self._producer:
        try:
            bootstrap= [x.strip() for x in agent_config['kafka_hosts'].split(',')]
            self._producer = KafkaProducer(bootstrap_servers=bootstrap,
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          compression_type='gzip')
        except:
            raise

    log.info('kafka_emitter: msg payload %s', len(json.dumps(message).encode('utf-8')))
    try:
      self._producer.send(self.topic, message)
    except:
      raise
