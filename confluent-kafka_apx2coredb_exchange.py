
from datetime import datetime, timedelta
import json
import logging
import pandas as pd
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
COREDB_EXCHANGE_SCHEMA = {
	"name": "coredb_exchange",
	"type": "struct",
	"fields": [
		{
			"field": "apx_ExchangeID",
			"type": "int32"
		},
		{
			"field": "exchange_name",
			"type": "string",
			"optional": True
		},
		{
			"field": "apx_HolidayScheduleID",
			"type": "int32",
			"optional": True
		},
		{
			"field": "is_system",
			"type": "boolean",
			"optional": True
		},
		{
			"field": "last_business_day",
			"type": "string",
			"optional": True
		},
		{
			"field": "MIC",
			"type": "string",
			"optional": True
		},
		{
			"field": "asofdate",
			"type": "string",
			"optional": True
		},
	]
}
APXDB_EXCHANGE_TOPIC = "devapxdb.APXFirm.dbo.AdvExchange"
COREDB_EXCHANGE_TOPIC = "confluent-kafka.devlwdb.coredb.dbo.exchange-v13"



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

		# Subscribe to topics
		consumer.subscribe([APXDB_EXCHANGE_TOPIC], on_assign=reset_offset)
		
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
					logging.error(f"ERROR: {msg.error()}")
				elif msg.value() is not None:
					# Extract the (optional) key and value, transform, and produce to coredb topic.
					mv = json.loads(msg.value().decode('utf-8'))
					if mv['payload']['op'] == 'c' or mv['payload']['op'] == 'u' or mv['payload']['op'] == 'r':
						vals = mv['payload']['after']
						coredb_exchange_vals = {
							'apx_ExchangeID'        : vals['ExchangeID'],
							'exchange_name'	        : vals['ExchangeName'],
							'apx_HolidayScheduleID'	: vals['HolidayScheduleID'],
							'is_system'	            : vals['IsSystem'],
							'last_business_day'	    : str(datetime.strptime('1/1/1970', '%m/%d/%Y') + timedelta(days=vals['LastBusinessDay']))[:-3],
							# TODO: better way to transform date from "days since epoch" to a string?
							'MIC'	                : vals['MIC'],
							'asofdate'			    : format_time(datetime.now()),
						}
						res_msg = {
							"schema": COREDB_EXCHANGE_SCHEMA,
							"payload": coredb_exchange_vals
						}
						producer.produce(COREDB_EXCHANGE_TOPIC, value=json.dumps(res_msg).encode('utf-8'), key=str(coredb_exchange_vals['apx_ExchangeID']), callback=delivery_callback)
						producer.flush()
					# TODO: handle deletes? Not sure if needed

		except KeyboardInterrupt:
			pass
		finally:
			# Leave group and commit final offsets
			consumer.close()

		return EXIT_SUCCESS
	
		



if __name__ == '__main__':
	sys.exit(Command().run_from_argv())
