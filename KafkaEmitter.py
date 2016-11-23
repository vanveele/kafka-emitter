"""
Simple custom emitter for DataDog agent to produce messages to Kafka.
Enable it by adding the following lines to datadog.conf and restarting
the agent:
kafka_hosts: kafka-hostname:9092
kafka_topic: datadog
custom_emitters: /path/to/KafkaEmitter.py
See the emitter documentation for additional optional settings.
Version: 0.0.1
"""

# stdlib
import sys
import time

import simplejson as json
# 3p
from kafka import KafkaProducer
from kafka.errors import LeaderNotAvailableError, \
    GroupCoordinatorNotAvailableError


class emitter(object):
    """
    The emitter class is called from the collector with a dict message payload
    """

    def __init__(self):
        """
        Setup kafka connection. Configs:
          'kafka_topic_prefix': topic where messages are sent
          'kafka_hosts': Comma separated list of bootstrap servers
        """
        self._topic_prefix = None
        self._dry_run = False
        self._producer = None
        self._bootstrap = None
        self._log = None

    def __call__(self, message, log, agent_config):
        """
            __call__ is called for each collector payload:
              message is a dictionary as collected by the agent
              log is a logging.Logger object
              agent_config is a dictionary of all the configuration values
        """

        # Connect to cluster on first call.
        if not self._producer:
            # Check configuration values
            if 'kafka_hosts' not in agent_config:
                log.error('Agent config missing kafka_hosts')
                return
            self._bootstrap = [x.strip() for x in
                               agent_config['kafka_hosts'].split(',')]

            self.dry_run = ('kafka_dry_run' in agent_config and
                            (agent_config['kafka_dry_run'] == 'yes' or
                             agent_config['kafka_dry_run'] == 'true'))
            self._topic_prefix = agent_config.get('kafka_topic_prefix', 'datadog')
            self._log = log

            if log:
                log.debug('Starting Kafka Emitter %s', ','.join(
                    self._bootstrap))

            if not self.dry_run:
                try:
                    bootstrap = self._bootstrap
                    self._producer = emitter.connect_cluster(
                        bootstrap_servers=bootstrap)
                # pylint: disable=bare-except
                except:
                    exc = sys.exc_info()
                    log.error('Unable to connect to Kafka cluster: %s',
                            str(exc[1]))
            else:
                self._producer = None
        try:
            if u'cpuSystem' in message:
                log.info('collector')
                self.parse_collector(message)
            elif u'series' in message:
                log.info('series')
                self.parse_dogstatsd(message)
            elif u'events' in message:
                log.info('event')
                self.parse_event(message)
            else:
                log.info('unknown')
                self.parse_dogstatsd(message)
        # pylint: disable=bare-except
        except:
            exc = sys.exc_info()
            log.error('Unable to parse message: %s\n%s',
                    str(exc[1]), str(message))
            raise

    def parse_dogstatsd(self, message):
        """
        Parses the JSON that was sent by dogstatsd
        Arguments:
        message - a JSON object representing the message sent to datadoghq
        """

        metrics = message['series']
        self.send_message(message, type='dogstatsd')

    def parse_event(self, message):
        self.send_message(message, type='event')

    def send_message(self, message, type):
        topic = '.'.join([self._topic_prefix, type])
        try:
            if self._producer:
                self._producer.send(topic, message)
        except LeaderNotAvailableError as kafka_error:
            _self.log.error('Kafka Emitter: Unable to send message %s: %s', message, str(kafka_error))
            return
        except GroupCoordinatorNotAvailableError as kafka_error:
            # attempt reconnect
            try:
                bootstrap = self._bootstrap
                self._producer = emitter.connect_cluster(
                    bootstrap_servers=bootstrap)
            # pylint: disable=bare-except
            except:
                exc = sys.exc_info()
            return
        except AttributeError as kafka_error:
            _self.log.error('Kafka Emitter: No connection to Kafka Cluster: %s',str(kafka_error))
            return

    # pylint: disable=too-many-arguments
    def send_bare_metric(self, name, value, tstamp, host_name, tags):
        """
        Sends a metric to the proxy
        """
        topic = '.'.join([self._topic_prefix, 'bare'])

        if value is None:
            return
        skip_tag_key = None
        if tags and host_name[0] == '=' and host_name[1:] in tags:
            skip_tag_key = host_name[1:]
            host_name = tags[skip_tag_key]

        line = ('%s key=%s value=%d source=%s tags=%s' %
                (long(tstamp), name, value, host_name, tags))
        if self._dry_run:
            self._log.info(line)
        else:
            try:
                self._producer.send(topic, line)
            except LeaderNotAvailableError as kafka_error:
                self._log.error('Kafka Emitter: Unable to send message %s: %s',line, str(kafka_error))
                return
            except GroupCoordinatorNotAvailableError as kafka_error:
                # attempt reconnect
                try:
                    bootstrap = self._bootstrap
                    self._producer = emitter.connect_cluster(
                        bootstrap_servers=bootstrap)
                # pylint: disable=bare-except
                except:
                    exc = sys.exc_info()


    @staticmethod
    def connect_cluster(bootstrap_servers):
        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             compression_type='gzip')

    @staticmethod
    def convert_key_to_dotted_name(key):
        """
        Convert a key that is camel-case notation to a dotted equivalent.
        This is best described with an example: key = "memPhysFree"
        returns "mem.phys.free"
        Arguments:
        key - a camel-case string value
        Returns:
        dotted notation with each uppercase containing a dot before
        """
        buf = []
        for char in key:
            if char.isupper():
                buf.append('.')
                buf.append(char.lower())
            else:
                buf.append(char)
        return ''.join(buf)

    # pylint: disable=too-many-locals
    def parse_collector(self, message):

        tstamp = long(message['collection_timestamp'])
        #tstamp = int(time.time())
        host_name = message['internalHostname']

        # cpu* mem*
        for key, value in message.iteritems():
            if key[0:3] == 'cpu' or key[0:3] == 'mem':
                dotted = 'system.' + emitter.convert_key_to_dotted_name(key)
                self.send_bare_metric(dotted, value, tstamp, host_name, [''])

        # metrics
        if False:
            metrics = message['metrics']
            for metric in metrics:
                self.send_bare_metric(
                    metric[0], metric[2], long(metric[1]), '=hostname', metric[3])

        # iostats
        if False:
            iostats = message['ioStats']
            for disk_name, stats in iostats.iteritems():
                for name, value in stats.iteritems():
                    name = (name.replace('%', '')
                            .replace('/', '_'))

                    metric_name = ('system.io.%s' % (name,))
                    tags = {'disk': disk_name}
                    self.send_bare_metric(metric_name, value, tstamp, host_name,
                                          tags)

        # count processes
        processes = message['processes']
        # don't use this name since it differs from internalHostname on ec2
        host_name = processes['host']
        metric_name = 'system.processes.count'
        value = len(processes['processes'])
        self.send_bare_metric(metric_name, value, tstamp, host_name, None)

        # system.load.*
        load_metric_names = ['system.load.1', 'system.load.15', 'system.load.5',
                             'system.load.norm.1', 'system.load.norm.15',
                             'system.load.norm.5']
        for metric_name in load_metric_names:
            if metric_name not in message:
                continue
            value = message[metric_name]
            self.send_bare_metric(metric_name, value, tstamp, host_name, None)

        self.send_message(message,'system')


    @staticmethod
    def sanitize(s):
        """
        Removes any `[ ] "' characters from the input screen
        """
        replace_map = {
            '[': '',
            ']': '',
            '"': ''
        }
        for search, replace in replace_map.iteritems():
            s = s.replace(search, replace)
        return s
