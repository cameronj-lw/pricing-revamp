
from datetime import datetime, timedelta
import json
import logging
import multiprocessing
import os
import pandas as pd
import sys

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.db.coredb.vw_price import vwPriceTable
from lw.util.date import format_time, get_next_bday

import flaskrp_helper


# globals
DATE_OR_TIME_FIELDS = {
    'data_date'     : '%Y-%m-%d'
    , 'modified_at' : '%Y-%m-%d %H:%M:%S.%f'
}
COREDB_PRICE_BATCH_TOPIC = "jdbc.lwdb.coredb.pricing.vw_price_batch"


def worker_func(data_date, new_prices, day_prices=None, held_lw_ids=None, mode='curr'):
    """
    Set the read model contents.

    Args:
    - data_date (str in YYYYMMDD, or date or datetime): Date to retrieve read model for.
    - new_prices (DataFrame): A chunk of prices to process for.
    - day_prices (DataFrame): All prices for the day. Used in 'curr' mode to update chosen price.
    - mode: 'curr' if the prices are from current day, 'prev' if the prices are from prev day.
        This is relative to the data_date.

    Returns: Number of rows processed, when succeeded.
    """
    total_cnt = len(new_prices.index)
    if held_lw_ids is not None:
        new_prices = new_prices[new_prices['lw_id'].isin(held_lw_ids)]
    held_cnt = len(new_prices.index)
    for i, px in new_prices.iterrows():
        px = px.to_dict()
        lw_id = px['lw_id']
        if mode == 'prev':
            flaskrp_helper.set_read_model_content('prev_bday_price', lw_id, px, data_date)
        elif mode == 'curr':
            curr = flaskrp_helper.get_read_model_content('curr_bday_prices', file_name=lw_id, data_date=data_date)
            if curr is None:
                curr = []
            # Remove the entry matching source, if applicable:
            curr = [x for x in curr if x['source'] != px['source']]
            # And now add the new price to it:
            curr.append(px)
            # We now have the updated read model contents. Write it:
            flaskrp_helper.set_read_model_content('curr_bday_prices', file_name=lw_id, content=curr, data_date=data_date)
            # Now, update the "chosen price":
            sec_prices = day_prices[day_prices['lw_id'] == lw_id].copy()
            chosen_price = flaskrp_helper.get_chosen_price(sec_prices)
            for col in DATE_OR_TIME_FIELDS:
                chosen_price[col] = pd.to_datetime(chosen_price[col])
                chosen_price[col] = chosen_price[col].apply(lambda x: x.isoformat())
            chosen_price = flaskrp_helper.NaN_NaT_to_none(chosen_price)
            chosen_price = chosen_price.to_dict('records')[0]
            flaskrp_helper.set_read_model_content('chosen_price', file_name=lw_id, content=chosen_price, data_date=data_date)
        # Now cascade the changes upwards:
        flaskrp_helper.refresh_sec_prices_read_model(lw_id, data_date)
    return held_cnt


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
        # config['enable.auto.commit'] = False

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
        consumer.subscribe([COREDB_PRICE_BATCH_TOPIC], on_assign=reset_offset)
        
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
                    # The message represents a batch of prices.
                    # We'll process that batch and add the prices to each security's "prices" JSON file.
                    # 1. Get data_date, source:
                    if mv['data_date'] < 99999:
                        data_date = (datetime.strptime('1/1/1970', '%m/%d/%Y') + timedelta(days=mv['data_date'])).date()
                        # TODO: better way to transform date from "days since epoch" to a string?
                    else:
                        data_date = datetime.fromtimestamp(mv['data_date'] / 1000.0).date()
                    if data_date < datetime(year=2023, month=5, day=13).date():
                        consumer.commit(message=msg, asynchronous=False)
                        continue
                    source = mv['source']
                    modified_at = datetime.fromtimestamp(mv['modified_at'] / 1000.0) + timedelta(hours=7)
                    # 2. Query vw_price for date. 
                    # If source is PXAPX, we only need PXAPX since we will update the "prev bday price" for the following day.
                    day_prices = None
                    if source == 'PXAPX':
                        new_prices = vwPriceTable().read(data_date=data_date, source=source)
                        new_prices['source'] = 'APX'
                        new_prices = new_prices[new_prices['modified_at'] == modified_at]
                        new_prices = flaskrp_helper.NaN_NaT_to_none(new_prices)
                        data_date = get_next_bday(data_date)
                        mode = 'prev'
                        # folder_name = flaskrp_helper.get_read_model_folder('prev_bday_price', data_date)
                    else:
                        day_prices = vwPriceTable().read(data_date=data_date)
                        new_prices = day_prices[day_prices['source'] == source]
                        new_prices = new_prices[new_prices['modified_at'] == modified_at]
                        new_prices = flaskrp_helper.trim_px_sources(new_prices)
                        new_prices = flaskrp_helper.NaN_NaT_to_none(new_prices)
                        day_prices = flaskrp_helper.trim_px_sources(day_prices)
                        day_prices = flaskrp_helper.NaN_NaT_to_none(day_prices)
                        mode = 'curr'
                        # folder_name = flaskrp_helper.get_read_model_folder('curr_bday_prices', data_date)
                        # folder_name_chosen = flaskrp_helper.get_read_model_folder('chosen_price', data_date)
                    if not len(new_prices.index):
                        continue
                    
                    logging.info(f"Found {len(new_prices.index)} new prices for {data_date}, {source} as of {modified_at}.")
                    
                    # 3. Loop thru lw_id's. if JSON file... DNE -> create it; exists -> add/replace this price source in its dict.
                    # ... But first format date & time cols here:
                    for col in DATE_OR_TIME_FIELDS:
                        new_prices[col] = pd.to_datetime(new_prices[col])
                        # new_prices[col] = new_prices[col].apply(lambda x: x.isoformat() if not isinstance(x, str)
                        #     else datetime.strptime(x, DATE_OR_TIME_FIELDS[col]).isoformat())
                        new_prices[col] = new_prices[col].apply(lambda x: x.isoformat())
                    
                    # Using Pool: # TODO_CLEANUP: remove when not needed
                    # # create a pool of worker processes
                    # pool = multiprocessing.Pool()

                    # # divide the batch into smaller chunks
                    # if len(new_prices.index) < multiprocessing.cpu_count() * 2:
                    #     logging.info(f'Not splitting {len(new_prices.index)} into chunks...')
                    #     chunks = [new_prices]
                    # else:
                    #     logging.info(f'Splitting {len(new_prices.index)} into chunks...')
                    #     chunk_size = len(new_prices.index) // multiprocessing.cpu_count()
                    #     chunks = [new_prices.loc[i:i+chunk_size] for i in range(0, len(new_prices.index), chunk_size)]
                    # # submit the chunks to the worker pool
                    # results = []
                    # for chunk in chunks:
                    #     logging.info(f'Chunk with {len(chunk)} records')
                    #     result = pool.apply_async(worker, args=(data_date, chunk, day_prices, mode))
                    #     logging.info(f'Got result: {result}')
                    #     results.append(result)

                    # # wait for all the workers to finish
                    # for result in results:
                    #     result.wait()

                    # Using Process:
                    # create a pool of worker processes
                    processes = []

                    # divide the batch into smaller chunks
                    if len(new_prices.index) < multiprocessing.cpu_count() * 2:
                        logging.info(f'Not splitting {len(new_prices.index)} into chunks...')
                        chunks = [new_prices]
                    else:
                        logging.info(f'Splitting {len(new_prices.index)} into chunks...')
                        chunk_size = len(new_prices.index) // multiprocessing.cpu_count()
                        chunks = [new_prices.loc[i:i+chunk_size] for i in range(0, len(new_prices.index), chunk_size)]
                    # submit the chunks to the worker pool
                    results = []
                    held_lw_ids = flaskrp_helper.get_read_model_content('held_securities', file_name='held', data_date=data_date)
                    for chunk in chunks:
                        logging.info(f'Chunk with {len(chunk)} records')
                        process = multiprocessing.Process(target=worker_func, args=(data_date, chunk, day_prices, held_lw_ids, mode))
                        processes.append(process)

                    # start all the worker processes
                    for process in processes:
                        process.start()

                    # wait for all the worker processes to finish
                    for process in processes:
                        process.join()

                    # update the master file with all the contents
                    logging.info('Updating master...')
                    lw_ids = new_prices['lw_id'].tolist()
                    flaskrp_helper.refresh_held_sec_prices_read_model(data_date, lw_ids)
                    logging.info('Done updating master.')
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                    # TODO_CLEANUP: remove when not needed? I think above code is sufficient...
                    for i, row in new_prices.iterrows():
                        row_dict = row.to_dict()
                        lw_id = row_dict['lw_id']
                        if lw_id is None:
                            continue  # Need an lw_id in order to do any useful processing
                        # Put dates and times into ISO fmt 
                        file_name = f'{lw_id}.json'
                        json_file = os.path.join(folder_name, file_name)
                        if source == 'PXAPX':
                            # No need to read the existing file, since we will fully replace it anyway
                            content = row_dict  
                            # Now write to the file:
                            flaskrp_helper.set_read_model_content('prev_bday_price', lw_id, content, data_date)
                            # ... and cascade this upwards, to the security with prices:
                            flaskrp_helper.refresh_sec_prices_read_model(lw_id, data_date)
                            # json_content = json.dumps(curr, default=str)
                            # with open(json_file, 'w') as f:
                            #     logging.debug(f'writing to {json_file}...')
                            #     f.write(json_content)
                        else:
                            curr = flaskrp_helper.get_read_model_content('curr_bday_prices', file_name=lw_id, data_date=data_date)
                            if curr is None:
                                curr = []
                            # Remove the entry matching source, if applicable:
                            curr = [x for x in curr if x['source'] != row['source']]
                            # And now add the new price to it:
                            curr.append(row_dict)
                            # We now have the updated read model contents. Write it:
                            flaskrp_helper.set_read_model_content('curr_bday_prices', file_name=lw_id, content=curr, data_date=data_date)
                            # Now, update the "chosen price":
                            sec_prices = prices[prices['lw_id'] == lw_id]
                            chosen_price = flaskrp_helper.get_chosen_price(sec_prices)
                            for col in DATE_OR_TIME_FIELDS:
                                chosen_price[col] = pd.to_datetime(chosen_price[col])
                                chosen_price[col] = chosen_price[col].apply(lambda x: x.isoformat())
                            chosen_price = flaskrp_helper.NaN_NaT_to_none(chosen_price)
                            chosen_price = chosen_price.to_dict('records')[0]
                            flaskrp_helper.set_read_model_content('chosen_price', file_name=lw_id, content=chosen_price, data_date=data_date)
                            # ... and cascade this upwards, to the security with prices:
                            flaskrp_helper.refresh_sec_prices_read_model(lw_id, data_date)
                    # Now that we've updated all secs' read models, update the shared ones:
                    # flaskrp_helper.refresh_held_sec_prices_read_model(data_date)
                    # mv = json.loads(msg.value().decode('utf-8'))
                    # logging.info(f'msg value: {mv}')

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            consumer.close()

        return EXIT_SUCCESS
    
        



if __name__ == '__main__':
    sys.exit(Command().run_from_argv())
