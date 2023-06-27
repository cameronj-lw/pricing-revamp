
from datetime import datetime, timedelta
import json
import logging
import os
import pandas as pd
import sys

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.util.date import format_time, get_next_bday

import flaskrp_helper


# globals
COREDB_SECURITY_TOPIC = "jdbc.lwdb.coredb.dbo.vw_security"



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

        # Create Producer & Consumer instances
        producer = Producer(config)
        config.update(config_parser['consumer'])
        logging.info(f'config: {config}')
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
        consumer.subscribe([COREDB_SECURITY_TOPIC], on_assign=reset_offset)
        
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
                    consumer.commit(message=msg, asynchronous=False)
                elif msg.value() is not None:
                    mv = json.loads(msg.value().decode('utf-8'))
                    # The message represents a security in secmaster.
                    # We'll process it and add to the security's JSON file.
                    lw_id = mv['lw_id']
                    if '/' not in lw_id:  # TODO: should actually handle this? 
                        # Rename pms_ to apx_
                        mv = flaskrp_helper.rename_keys_pms2apx(mv)
                        if 'modified_at' in mv:
                            if isinstance(mv['modified_at'], int):
                                mv['modified_at'] = datetime.fromtimestamp(mv['modified_at'] / 1000.0)
                            mv['modified_at'] = mv['modified_at'].isoformat()
                        flaskrp_helper.set_read_model_content('security', lw_id, mv)
                        dates_to_update = [datetime(year=2023, month=5, day=19).date()]
                        for d in dates_to_update:
                            flaskrp_helper.refresh_sec_prices_read_model(lw_id, d)
                    # mv = json.loads(msg.value().decode('utf-8'))
                    # logging.info(f'msg value: {mv}')
                    # Finally, commit
                    consumer.commit(message=msg, asynchronous=False)

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            consumer.close()

        return EXIT_SUCCESS
    
        



if __name__ == '__main__':
    sys.exit(Command().run_from_argv())
