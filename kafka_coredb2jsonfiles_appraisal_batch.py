
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
COREDB_APPRAISAL_BATCH_TOPIC = "jdbc.lwdb.coredb.pricing.vw_appraisal_batch"



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
        consumer.subscribe([COREDB_APPRAISAL_BATCH_TOPIC], on_assign=reset_offset)
        
        # Poll for new messages from Kafka and print them.
        try:
            while True:
                msg = consumer.poll(10.0)
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
                    # The message represents a batch of appraisal results.
                    # We'll find the lw_id's included in that batch, then update the held_securities_with_prices read model.
                    # 1. Get data_date:
                    if mv['data_date'] < 99999:
                        data_date = (datetime.strptime('1/1/1970', '%m/%d/%Y') + timedelta(days=mv['data_date'])).date()
                        # TODO: better way to transform date from "days since epoch" to a string?
                    else:
                        consumer.commit(message=msg, asynchronous=False)
                        continue  # Old messages which are invalid, from when data_date was datetime type -> ignore
                        data_date = datetime.fromtimestamp(mv['data_date'] / 1000.0).date()
                    logging.info(f"Found appr batch for {data_date}")
                    if data_date < datetime(year=2023, month=4, day=20).date():
                        consumer.commit(message=msg, asynchronous=False)
                        continue
                    # 2. Get the list of held lw_id's. Then append to our results list as we go.
                    results = []
                    held = flaskrp_helper.get_held_securities(curr_bday=data_date)
                    held = flaskrp_helper.rename_cols_apx2pms(held)
                    for id_col in ('lw_id', 'pms_security_id'):
                        ids = held[id_col].values.tolist()
                        # Update held securities read models for data_date:
                        flaskrp_helper.set_read_model_content('held_securities', id_col, ids, data_date)
                        # Create next bday held securities read model, if it DNE:
                        next_bday = get_next_bday(data_date)
                        next_bday_rm_file = flaskrp_helper.get_read_model_file('held_securities', id_col, data_date=next_bday)
                        next_bday_rm_file_exists = os.path.exists(next_bday_rm_file)
                        if not next_bday_rm_file_exists:
                            flaskrp_helper.set_read_model_content('held_securities', id_col, ids, next_bday)
                        if id_col == 'lw_id':
                            # Loop thru. Build the results list for all held secs with prices
                            for lw_id in ids:
                                security_with_prices = flaskrp_helper.get_read_model_content(
                                    'security_with_prices', file_name=lw_id, data_date=data_date)
                                results.append(security_with_prices)
                                if not next_bday_rm_file_exists:
                                    # The file should not yet exist for next day -> need to create it:
                                    flaskrp_helper.refresh_sec_prices_read_model(lw_id, next_bday)
                            # Now update the "master" read model for next bday, if it DNE (there will be no prices yet).
                            next_bday_rm_file = flaskrp_helper.get_read_model_file('held_securities_with_prices', 'held', data_date=next_bday)
                            next_bday_rm_file_exists = os.path.exists(next_bday_rm_file)
                            if not next_bday_rm_file_exists:
                                flaskrp_helper.refresh_held_sec_prices_read_model(data_date=next_bday, lw_ids=ids)
                    # Finally, update the "master" read models:
                    flaskrp_helper.set_read_model_content('held_securities_with_prices'
                        , file_name='held', content=results, data_date=data_date)
                    # And create next bday master file, if it DNE:
                    next_bday_rm_file = flaskrp_helper.get_read_model_file('held_securities_with_prices', 'held', data_date=next_bday)
                    next_bday_rm_file_exists = os.path.exists(next_bday_rm_file)
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
