
from datetime import datetime, timedelta
import json
import logging
import sys

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.util.date import format_time


# globals
# TODO: better schema management? Schema registry? 
COREDB_POS_SCHEMA = {
	"name": "coredb_position",
	"type": "struct",
	"fields": [
		{
			"field": "apx_PositionID",
			"type": "int32"
		},
		{
			"field": "apx_PortfolioID",
			"type": "int32"
		},
		{
			"field": "apx_SecurityID",
			"type": "int32"
		},
		{
			"field": "is_short",
			"type": "boolean",
			"optional": True
		},
		{
			"field": "quantity",
			"type": "float",
			"optional": True
		},
		{
			"field": "asofdate",
			"type": "string",
			"optional": True
		},
		{
			"field": "deleted",
			"type": "boolean",
			"optional": True
		},
	]
}
APXDB_POS_TOPIC = "apxdb.APXFirm.dbo.AdvPosition"
COREDB_POS_TOPIC = "confluent-kafka.devlwdb.coredb.dbo.position-v13"



class Command(BaseCommand):
	help = 'Run kafka Consumers and Producers'

	def add_arguments(self, parser):
		parser.add_argument('-cf', '--config_file', type=FileType('r'))
		parser.add_argument('-r', '--reset_offset', action='store_true', help='Reset consumer offset upon startup', default=False)
		
		

	def handle(self, *args, **kwargs):
		"""
		Run kafka Consumers and Producers:
		"""

		logging.info('Starting kafka Consumer(s) & Producer(s)...')

		# Parse the configuration.
		# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
		config_parser = ConfigParser()
		config_parser.read_file(kwargs['config_file'])
		config = dict(config_parser['default'])
		logging.info(config)

		# Create Producer & Consumer instances
		producer = Producer(config)
		config.update(config_parser['consumer'])
		consumer = Consumer(config)

		# Set up a callback to handle the '--reset' flag.
		def reset_offset(consumer, partitions):
			if kwargs['reset_offset']:
				for p in partitions:
					p.offset = OFFSET_BEGINNING
				consumer.assign(partitions)

		# Optional per-message delivery callback (triggered by poll() or flush())
		# when a message has been successfully delivered or permanently
		# failed delivery (after retries).
		def delivery_callback(err, msg):
			if err:
				logging.error('ERROR: Message failed delivery: {}'.format(err))
			else:
				logging.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
					topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

		# Subscribe to topic
		consumer.subscribe([APXDB_POS_TOPIC], on_assign=reset_offset)
		
		# Poll for new messages from Kafka and print them.
		try:
			while True:
				msg = consumer.poll(1.0)
				if msg is None:
					# Initial message consumption may take up to
					# `session.timeout.ms` for the consumer group to
					# rebalance and start consuming
					logging.debug("Waiting...")
				elif msg.error():
					logging.error("ERROR: %s".format(msg.error()))
				elif msg.value() is not None:
					# Extract the (optional) key and value, transform, and produce to coredb topic.
					mv = json.loads(msg.value().decode('utf-8'))
					if mv['payload']['op'] == 'c' or mv['payload']['op'] == 'u' or mv['payload']['op'] == 'r':
						vals = mv['payload']['after']
						coredb_pos_vals = {
							'apx_PositionID'	: vals['PositionID'],
							'apx_PortfolioID'   : vals['PortfolioID'],
							'apx_SecurityID'	: vals['SecurityID'],
							'is_short'			: vals['IsShortPosition'],
							'quantity'			: vals['Quantity'],
							'asofdate'			: format_time(datetime.now()),
							'deleted'			: False,
						}
						res_msg = {
							"schema": COREDB_POS_SCHEMA,
							"payload": coredb_pos_vals
						}
						producer.produce(COREDB_POS_TOPIC, value=json.dumps(res_msg).encode('utf-8'), key=str(coredb_pos_vals['apx_PositionID']), callback=delivery_callback)
						producer.flush()
					elif mv['payload']['op'] == 'd':
						vals = mv['payload']['before']
						coredb_pos_vals = {
							'apx_PositionID'	: vals['PositionID'],
							'apx_PortfolioID'   : vals['PortfolioID'],
							'apx_SecurityID'	: vals['SecurityID'],
							'is_short'			: vals['IsShortPosition'],
							'quantity'			: vals['Quantity'],
							'asofdate'			: format_time(datetime.now()),
							'deleted'			: True,
						}
						res_msg = {
							"schema": COREDB_POS_SCHEMA,
							"payload": coredb_pos_vals
						}
						producer.produce(COREDB_POS_TOPIC, value=json.dumps(res_msg).encode('utf-8'), key=str(coredb_pos_vals['apx_PositionID']), callback=delivery_callback)
						producer.flush()

		except KeyboardInterrupt:
			pass
		finally:
			# Leave group and commit final offsets
			consumer.close()

		return EXIT_SUCCESS
	
		



if __name__ == '__main__':
	sys.exit(Command().run_from_argv())
