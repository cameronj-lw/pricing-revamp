
import json
import logging
import numpy as np
import os
import pandas as pd
import socket
import sys
from datetime import date, datetime, timedelta
from email.utils import parseaddr
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from flask_restx import Api, Resource, fields
from marshmallow import Schema, fields, ValidationError, pre_load
import pymsteams
from sqlalchemy import exc, text, update
from sqlalchemy.orm import Session

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.db.apxdb.temppricehistory import TempPriceHistoryTable
from lw.db.coredb.pricingauditreason import PricingAuditReasonTable
from lw.db.coredb.pricingaudittrail import PricingAuditTrailTable
from lw.db.coredb.pricingcolumnconfig import PricingColumnConfigTable
from lw.db.coredb.pricingmanualpricingsecurity import PricingManualPricingSecurityTable
from lw.db.coredb.pricingnotificationsubscription import PricingNotificationSubscriptionTable
from lw.db.coredb.vprice import vPriceTable
from lw.db.coredb.vheldsecurity import vHeldSecurityTable
from lw.db.coredb.vportfolio import vPortfolioTable
from lw.db.coredb.vposition import vPositionTable
from lw.db.coredb.vsecurity import vSecurityTable
from lw.db.coredb.vtransaction import vTransactionTable
from lw.db.mgmtdb.monitor import MonitorTable
from lw.db.lwdb.apx_appraisal import ApxAppraisalTable
from lw.util.date import format_time, get_current_bday, get_previous_bday, get_next_bday
from lw.util.file import prepare_dated_file_path


# globals
app = Flask(__name__)
ma = Marshmallow(app)
api = Api(app)
CORS(app)


PRICING_FEEDS = {
    'FTSE': {
        'FTP_DOWNLOAD': [
            # run_group, [run_names]
            ('FTP-FTSETMX_PX', ['FQCOUPON','FQFRN','FQMBS','FQMBSF','SCMHPDOM','SMIQUOTE'])
        ],
        'LOAD2LW': [
            ('FTSETMX_PX', ['FQCOUPON','FQFRN','FQMBS','FQMBSF','SCMHPDOM','SMIQUOTE'])
        ],
        'LOAD2PRICING': [
            ('FTSETMX_PX', ['PostProcess'])
        ],
        'normal_eta': datetime.today().replace(hour=14, minute=15)
    },
    'MARKIT': {
        'FTP_UPLOAD': [
            ('MARKIT_PRICE', ['ISINS_SEND'])
        ],
        'FTP_DOWNLOAD': [
            ('FTP-MARKIT', ['LeithWheeler_Nxxxx_Standard'])
        ],
        'LOAD2LW': [
            ('MARKIT_PRICE', ['MARKIT_PRICE'])
        ],
        'LOAD2PRICING': [
            ('MARKIT_PRICE', ['MARKIT_PRICE'])
        ],
        'normal_eta': datetime.today().replace(hour=13, minute=30)
    },
    'FUNDRUN': {
        'FTP_UPLOAD': [
            ('FUNDRUN', ['EQUITY_UPLOAD'])
        ],
        'FTP_DOWNLOAD': [
            ('FTP-FUNDRUN_PRICE_EQ', ['FUNDRUN_PRICE_EQ'])
        ],
        'LOAD2LW': [
            ('FUNDRUN', ['EQUITY_PRICE_MAIN'])
        ],
        'LOAD2PRICING': [
            ('FUNDRUN', ['EQUITY_PRICE_MAIN'])
        ],
        'normal_eta': datetime.today().replace(hour=13, minute=45)
    },
    'FUNDRUN_LATAM': {
        'FTP_UPLOAD': [
            ('FUNDRUN', ['EQUITY_UPLOAD'])
        ],
        'FTP_DOWNLOAD': [
            ('FTP-FUNDRUN_PRICE_EQ_LATAM', ['FUNDRUN_PRICE_EQ_LATAM'])
        ],
        'LOAD2LW': [
            ('FUNDRUN', ['EQUITY_PRICE_LATAM'])
        ],
        'LOAD2PRICING': [
            ('FUNDRUN', ['EQUITY_PRICE_LATAM'])
        ],
        'normal_eta': datetime.today().replace(hour=14, minute=00)
    },
    'BLOOMBERG': { 
        'BB_SNAP': [
            ('BB-SNAP', ['BOND_PRICE'])
        ],
        'LOAD2PRICING': [
            ('LOADPRICE_FI', ['BOND_PRICE'])
        ],
        'normal_eta': datetime.today().replace(hour=14, minute=30)
    },
}

# TODO_PROD: change all references to APX to PMS where possible
APX_SEC_TYPES = {
    # TODO_TO: confirm if desired
    'bond': ['cb', 'cf', 'cm', 'cv', 'fr', 'lb', 'sf', 'tb'],
    'equity': ['cc', 'ce', 'cg', 'ch', 'ci', 'cj', 'ck', 'cn', 'cr', 'cs', 'ct', 'cu', 'ps']
}

APX_PRICE_TYPES = {
    'price_value': ''  # Standard (price)
    , 'duration_value': '_LWBondDur'
    , 'yield_value': '_LWBondYield'
}

APX_PX_SOURCES = {
    # 'LWDB source': APX source ID   # APX source name
    'DEFAULT'               : 3000,  # LW Not Classified
    'FTSE'                  : 3006,  # LW FTSE TMX
    'FTSETMX_PX'            : 3006,  # LW FTSE TMX
    'BLOOMBERG'             : 3004,  # LW Bloomberg
    'MARKIT'                : 3005,  # LW Markit
    'MARKIT_LOAN'           : 3011,  # LW Markit - Loan
    'FUNDRUN'               : 3019,  # LW FundRun Equity
    'FIDESK_MANUALPRICE'    : 3007,  # LW FI Desk - Manual Price
    'MANUAL'                : 3007,  # LW FI Desk - Manual Price
    'FIDESK_MISSINGPRICE'   : 3008,  # LW FI Desk - Missing Price
    'MISSING'               : 3008,  # LW FI Desk - Missing Price
}

PRICE_HIERARCHY = RELEVANT_PRICES = [
    # highest to lowest in the hierarchy, i.e. MANUAL is highest
    # TODO_IDEA: should add more pricing sources to "hierarchy"? 
    # Only adding ones relevant to pricing revamp for now...
    'MANUAL','MISSING','FUNDRUN','FTSE','MARKIT','BLOOMBERG','RBC'
]


def NaN_NaT_to_none(df):
    return df.replace({np.nan: None, pd.NaT: None})

def clean(df):
    if len(df.index):
        df = NaN_NaT_to_none(df)
        data_dict = df.to_dict('records')
        res_dict = {
            'status': 'success',
            'data': data_dict,
            'message': None
        }
    else:
        res_dict = {
            'status': 'warning',
            'data': None,
            'message': 'No data was found.'
        }
    res_str = jsonify(res_dict)
    return res_str

def trim_px_sources(raw_prices):
    prices = raw_prices
    prices['source'] = prices['source'].apply(lambda x: 'BLOOMBERG' if (x[:3] == 'BB_' and '_DERIVED' not in x) else x)
    prices['source'] = prices['source'].apply(lambda x: 'FTSE' if x == 'FTSETMX_PX' else x)
    prices['source'] = prices['source'].apply(lambda x: 'FUNDRUN' if x == 'FUNDRUN_EQUITY' else x)
    prices['source'] = prices['source'].apply(lambda x: 'MANUAL' if x == 'FIDESK_MANUALPRICE' else x)
    prices['source'] = prices['source'].apply(lambda x: 'MISSING' if x == 'FIDESK_MISSINGPRICE' else x)
    prices = prices[prices['source'].isin(RELEVANT_PRICES)]
    return prices

@api.route('/api/pricing/price/<string:price_date>')
class PriceByDate(Resource):
    def get(self, price_date):
        prices = vPriceTable().read_for_date(data_date=price_date)
        prices = trim_px_sources(prices)
        return clean(prices)

def get_chosen_price(prices):
    chosen_loc = chosen_px = None
    for i, px in prices.iterrows():
        src = px['source']
        # logging.info(f"{src} for {px['lw_id']}")
        if src not in PRICE_HIERARCHY:
            continue
        elif chosen_loc is None:
            chosen_loc = PRICE_HIERARCHY.index(src)
            chosen_px = prices.loc[[i]]
        elif PRICE_HIERARCHY.index(src) < chosen_loc:
            chosen_loc = PRICE_HIERARCHY.index(src)
            chosen_px = prices.loc[[i]]
    if chosen_px is None:
        return chosen_px
    return chosen_px#.to_frame()

def is_manual(prices):
    return (len(prices[prices['source']=='MANUAL']) > 0)

def is_missing(prices):
    relevant_external_sources = [x for x in RELEVANT_PRICES if x not in ['MANUAL','MISSING']]
    if len(prices[prices['source'].isin(relevant_external_sources)]):
        return False
    else:
        return True

def is_override(prices):
    return (~is_missing(prices) and is_manual(prices))

def first2chars(s):
    if s is None:
        return s
    else:
        return s[:2]

def add_valid_dates(payload):
    payload['valid_from'] = date.today()
    payload['valid_to'] = None
    return payload

def add_asof(payload):
    if 'asofuser' not in payload:
        payload['asofuser'] = f"{os.getlogin()}_{socket.gethostname()}"
    payload['asofdate'] = format_time(datetime.now())
    return payload

def get_securities_by_sec_type(secs, sec_type=None, sec_type_col='apx_sec_type', sec_type_func=first2chars):
    if sec_type is None:
        return secs
    if isinstance(sec_type, str):  # convert to list
        if sec_type in APX_SEC_TYPES:
            sec_type = APX_SEC_TYPES[sec_type]  # should be a list now
        else:
            sec_type = [sec_type]
    if isinstance(sec_type, list):
        if sec_type_func is None:
            return secs[secs[sec_type_col].isin(sec_type)]
        else:
            processed_sec_types = secs[sec_type_col].apply(sec_type_func)
            return secs[processed_sec_types.isin(sec_type)]

def get_held_securities(curr_bday, sec_type=None):
    curr_bday_appr = ApxAppraisalTable().read_for_date(curr_bday)
    if len(curr_bday_appr):  # if there are Appraisal results for curr day, use it
        secs = vSecurityTable().read()
        secs = get_securities_by_sec_type(secs, sec_type)
        held_secs = list(curr_bday_appr['ProprietarySymbol'])
        held = secs[secs['lw_id'].isin(held_secs)]
    else:  # if not, use live positions
        held = vHeldSecurityTable().read()
        held = get_securities_by_sec_type(held, sec_type)
    return held

def add_prices(held, i, lw_id, curr_prices, prev_prices):
    sec_curr_prices = curr_prices.loc[curr_prices['lw_id'] == lw_id]
    sec_prev_prices = prev_prices.loc[prev_prices['lw_id'] == lw_id]
    # add prices:
    prices = pd.concat([sec_curr_prices, sec_prev_prices], ignore_index=True)
    if len(prices):
        prices = NaN_NaT_to_none(prices)
        held.at[i, 'prices'] = prices.to_dict('records')
        chosen_px = get_chosen_price(prices)
        if chosen_px is not None:
            held.at[i, 'chosen_price'] = chosen_px.to_dict('records')
    return held, prices

def add_audit_trail(held, i, lw_id, curr_audit_trail):
    sec_curr_audit = curr_audit_trail.loc[curr_audit_trail['lw_id'] == lw_id]
    if len(sec_curr_audit):
        sec_curr_audit = NaN_NaT_to_none(sec_curr_audit)
        held.at[i, 'audit_trail'] = sec_curr_audit.to_dict('records')
        if not isinstance(held.at[i, 'audit_trail'], list):  
            # If only one row, it will be a dict, but we want it as a list:
            held.at[i, 'audit_trail'] = [held.at[i, 'audit_trail']]
    return held

def should_exclude_sec(lw_id, prices, price_type, manually_priced_secs):
    if price_type is None:
        return False
    if price_type == 'manual':  
        # securities which have been priced by a user
        if not is_manual(prices):
            return True
    elif price_type == 'missing':  
        # securities which do not have an external price,
        # OR are on the "sticky bond" list
        # TODO_TO: confirm whether desired to include the "sticky bond list" here
        if not is_missing(prices):
            if lw_id not in manually_priced_secs['lw_id']:
                return True
    elif price_type == 'override':  
        # securities which have an external price, but were also priced by a user
        if not is_override(prices):
            return True
    # If we made it here, the sec should not be excluded
    return False


def get_held_security_prices(curr_bday, prev_bday, sec_type=None, price_type=None):
    held = get_held_securities(curr_bday, sec_type)
    held['prices'] = None
    curr_prices = vPriceTable().read(data_date=curr_bday)
    curr_prices = trim_px_sources(curr_prices)
    # want to include only 1 prev day px: the one from APX
    prev_prices = vPriceTable().read(data_date=prev_bday, source='PXAPX')
    prev_prices['source'] = 'APX'
    curr_audit_trail = PricingAuditTrailTable().read(data_date=curr_bday)
    manually_priced_secs = get_manual_pricing_securities()  # in case we need to add these below
    for i, row in held.iterrows():
        lw_id = row['lw_id']
        held, prices = add_prices(held, i, lw_id, curr_prices, prev_prices)
        # remove this security according to type, if requested
        if should_exclude_sec(lw_id, prices, price_type, manually_priced_secs):
            held = held.drop(i)
            continue
        # add audit trail:
        held = add_audit_trail(held, i, lw_id, curr_audit_trail)
    # TODO_WAVE2: make good thru date based on sec type / applicable holiday calendar
    held['good_thru_date'] = get_next_bday(curr_bday)
    return clean(held)


@api.route('/api/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        if 'sec_type' in payload:
            sec_type = payload['sec_type']
        else:
            sec_type = None
        if 'price_date' in payload:
            price_date = payload['price_date']
        else:
            price_date = date.today()
        if 'price_type' in payload:
            price_type = payload['price_type']
        else:
            price_type = None
        curr_bday, prev_bday = get_current_bday(price_date), get_previous_bday(price_date)
        # logging.info(request.json)
        # payload = api.payload
        # if 'price_type' in payload:
        #     return get_held_security_prices(curr_bday, prev_bday, payload['price_type'])
        return get_held_security_prices(curr_bday, prev_bday, sec_type, price_type)

def get_pricing_attachment_folder(data_date):
        base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing_audit')
        full_path = prepare_dated_file_path(folder_name=base_path, date=datetime.strptime(data_date, '%Y%m%d'), file_name='', rotate=False)
        return full_path

def save_binary_files(data_date, files):
    """
    Save binary files to folder corresponding to data_date.

    Args:
        folder_path (str): The path of the folder where the files will be saved.
        files (list): A list of dictionaries where each dictionary contains
                      the filename and its binary contents.

    Returns:
        None
    """

    # Create the folder if it doesn't exist
    folder_path = get_pricing_attachment_folder(data_date)
    if os.path.exists(folder_path):
        delete_res = delete_dir_contents(folder_path)
        if delete_res[1] != 200:
            return delete_res
    else:
        os.makedirs(folder_path)

    # Save each file in the list
    for f in files:
        file_name = f['name']
        file_content = f['binary_content']

        # Create the file path
        file_path = os.path.join(folder_path, file_name)

        # Save the binary content to the file
        with open(file_path, "wb") as fp:
            fp.write(file_content.encode('utf-8'))
    if len(files):
        return {
            'status': 'success',
            'data': None,
            'message': f'Saved {len(files)} files to {folder_path}'
        }, 201
    else:
        return {
            'status': 'warning',
            'data': None,
            'message': f'No files were provided, so there is nothing to save.'
        }, 200

def delete_dir_contents(path):
    try:
        files = os.listdir(path)
        for f in files:
            os.remove(os.path.join(path, f))
        return {
            'status': 'success',
            'data': None,
            'message': f'deleted {len(files)} files from {path}'
        }, 200
    except Exception as e:
        msg = f'Failed to delete files at {path}: {e}'
        logging.exception(msg)
        return {
            'status': 'error',
            'data': None,
            'message': msg
        }, 500

@api.route('/api/pricing/attachment/<string:price_date>')
class PricingAttachmentByDate(Resource):
    def post(self, price_date):
        payload = api.payload
        if 'files' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain files"
            }, 422
        else:
            return save_binary_files(price_date, payload['files'])
    
    def get(self, price_date):
        full_path = get_pricing_attachment_folder(price_date)
        files = []
        with os.listdir(full_path) as entries:
            for entry in entries:
                files.append(os.path.join(full_path, entry))
        return clean(pd.DataFrame(files, columns=['full_path']))

    def delete(self, price_date):
        full_path = get_pricing_attachment_folder(price_date)
        return delete_dir_contents(full_path)

def valid(df, data_date=date.today()):  
# TODO_REFACTOR: does this belong in a library?
    valid_from = [
        pd.isnull(df['valid_from']), df['valid_from'] <= np.datetime64(data_date)
    ]
    valid_to = [
        pd.isnull(df['valid_to']), df['valid_to'] >= np.datetime64(data_date)
    ]
    return df.loc[valid_from[0] | valid_from[1]].loc[valid_to[0] | valid_to[1]]

@api.route('/api/pricing/audit-reason')
class PricingAuditReason(Resource):
    def get(self):
        reasons = PricingAuditReasonTable().read()
        valid_reasons = valid(reasons)[['reason']]
        return clean(valid_reasons)

def save_df_to_table(df, table):
    try:
        res = table.bulk_insert(df)
        if res.rowcount == len(df.index):
            return {
                'status': 'success',
                'data': None,
                'message': f"Saved {res.rowcount} rows to " \
                f"{config.CONN_INFO[table.database_key][config.ENV]['hostname']}." \
                f"{config.CONN_INFO[table.database_key][config.ENV]['database']}." \
                f"{table.schema}.{table.table_name}"
            }, 201
        else:
            msg = f'Expected {len(df.index)} rows to be saved, but there were {res.rowcount}!'
            logging.error(msg)
            return {
                'status': 'error',
                'data': None,
                'message': msg
            }, 500
    except exc.SQLAlchemyError as e:
        logging.exception(e)
        return {
            'status': 'error',
            'data': None,
            'message': f'SQLAlchemy error: {e}'
        }, 500

@api.route('/api/pricing/notification-subscription')
class PricingNotificationSubscription(Resource):
    def post(self):
        payload = api.payload
        # TODO_CLEANUP: remove below when not needed
        # below using marshmallow ... may continue trying this if flask_marshmallow isn't sufficient
        # try:
        #     data = PricingNotificationSubscriptionSchema().load(payload)
        # except ValidationError as err:
        #     return err.messages, 422
        if 'email' not in payload or 'feed_name' not in payload:
            return {
                'status': 'error',
                'data': None,
                'message': f'required field(s) missing: email, feed_name'
            }, 422
        email = parseaddr(payload['email'])[1]
        if email == '':
            return {
                'status': 'error',
                'data': None,
                'message': f"invalid email: {payload['email']}"
            }, 422
        old_rows = self.set_valid_to_yesterday(email, payload['feed_name'])
        payload = add_valid_dates(payload)
        payload = add_asof(payload)
        return save_df_to_table(pd.DataFrame([payload]), PricingNotificationSubscriptionTable())
    
    def get(self):
        subs = PricingNotificationSubscriptionTable().read()
        valid_subs = valid(subs)[['email','feed_name','email_on_pending','email_on_in_progress','email_on_complete','email_on_error','email_on_delayed']]
        return clean(valid_subs)

    def delete(self):
        payload = api.payload
        if 'email' not in payload or 'feed_name' not in payload:
            return {
                'status': 'error',
                'data': None,
                'message': f'required field(s) missing: email, feed_name'
            }, 422
        email = parseaddr(payload['email'])[1]
        if email == '':
            return {
                'status': 'error',
                'data': None,
                'message': f"invalid email: {payload['email']}"
            }, 422
        updated_rows = self.set_valid_to_yesterday(email, payload['feed_name'])
        if updated_rows:
            return {
                'status': 'success',
                'message': f"deleted {updated_rows} subscription(s) for {email} {payload['feed_name']}"
            }, 200
        else:
            return {
                'status': 'warning',
                'data': None,
                'message': f"Found nothing to delete for {email} {payload['feed_name']}"
            }, 200

    def set_valid_to_yesterday(self, email, feed_name):
        pns_table = PricingNotificationSubscriptionTable()
        new_vals = {'valid_to': date.today() + timedelta(days=-1)}
        new_vals = add_asof(new_vals)
        stmt = update(pns_table.table_def).\
						where(pns_table.table_def.c.email == email).\
						where(pns_table.table_def.c.feed_name == feed_name).\
						where(pns_table.table_def.c.valid_to == None).\
						values(new_vals)
        updated_rows = pns_table._database.execute_write(stmt).rowcount
        logging.info(
            f"{pns_table.table_name}: Updated valid_to for {updated_rows} rows for {email} {feed_name}."
        )
        return updated_rows
    
@api.route('/api/transaction/<string:trade_date>')
class TransactionByDate(Resource):
    def get(self, trade_date):
        txns = vTransactionTable().read_for_trade_date(trade_date=trade_date)
        return clean(txns)

def is_error(pf, data_date):
    res, max_ts = False, datetime.fromordinal(1)  # beginning of time
    for task in PRICING_FEEDS[pf]:
        if not isinstance(PRICING_FEEDS[pf][task], list):
            continue
        for (rg, rns) in PRICING_FEEDS[pf][task]:
            for rn in rns:                    
                mon = MonitorTable().read(scenario=MonitorTable().base_scenario, data_date=data_date, run_group=rg, run_name=rn, run_type='RUN')
                error = mon[mon['run_status'] == 1]
                if len(error.index):
                    res = True
                    max_ts = max(max_ts, error['asofdate'].max())
    return res, (max_ts if res else datetime.now())

def is_priced(pf, data_date):
    max_ts = datetime.fromordinal(1)  # beginning of time
    if 'LOAD2PRICING' in PRICING_FEEDS[pf]:
        for (rg, rns) in PRICING_FEEDS[pf]['LOAD2PRICING']:
            for rn in rns:
                mon = MonitorTable().read(scenario=MonitorTable().base_scenario, data_date=data_date, run_group=rg, run_name=rn, run_type='RUN')
                complete = mon[mon['run_status'] == 0]
                if not len(complete.index):
                    return False, datetime.now()
                max_ts = max(max_ts, complete['asofdate'].max())
        return True, max_ts
    return False, datetime.now()

def is_in_progress(pf, data_date):
    res, max_ts = False, datetime.fromordinal(1)  # beginning of time
    for task in ['BB_SNAP', 'FTP_DOWNLOAD', 'LOAD2LW', 'LOAD2PRICING']:
        if task in PRICING_FEEDS[pf]:
            for (rg, rns) in PRICING_FEEDS[pf][task]:
                for rn in rns:                    
                    mon = MonitorTable().read(scenario=MonitorTable().base_scenario, data_date=data_date, run_group=rg, run_name=rn, run_type='RUN')
                    in_progress = mon[mon['run_status'] == (-1 | 0)]  # include "success" here in case some are success and others not started
                    if len(in_progress.index):
                        res = True
                        max_ts = max(max_ts, in_progress['asofdate'].max())
    return res, (max_ts if res else datetime.now())

def is_delayed(pf, data_date):
    if is_priced(pf, data_date)[0]:
        return False, datetime.now()
    if 'normal_eta' in PRICING_FEEDS[pf]:
        if PRICING_FEEDS[pf]['normal_eta'] < datetime.now():
            return True, datetime.now()
    return False, datetime.now()

def is_pending(pf, data_date):
    max_ts = datetime.fromordinal(1)  # beginning of time
    if 'FTP_UPLOAD' in PRICING_FEEDS[pf]:
        for (rg, rns) in PRICING_FEEDS[pf]['FTP_UPLOAD']:
            for rn in rns:
                mon = MonitorTable().read(scenario=MonitorTable().base_scenario, data_date=data_date, run_group=rg, run_name=rn, run_type='RUN')
                complete = mon[mon['run_status'] == 0]
                if not len(complete.index):
                    return False, datetime.now()
                max_ts = max(max_ts, complete['asofdate'].max())
        return True, max_ts
    return False, datetime.now()

# class PricingFeed(Object):
#     def __init__(self, data_date=date.today()):
#         self.data_date = data_date
#         self.status = None

def get_pricing_feed_status(price_date=date.today()):
    statuses = {}
    pd = datetime.strptime(price_date, '%Y%m%d') if isinstance(price_date, str) else price_date
    try:
        for pf in PRICING_FEEDS:
            error = is_error(pf, pd)
            if error[0]:
                statuses[pf] = {'status': 'ERROR', 'asofdate': error[1].isoformat()}
                continue
            priced = is_priced(pf, pd)
            if priced[0]:
                statuses[pf] = {'status': 'PRICED', 'asofdate': priced[1].isoformat()}
                continue
            in_progress = is_in_progress(pf, pd)
            if in_progress[0]:
                statuses[pf] = {'status': 'IN PROGRESS', 'asofdate': in_progress[1].isoformat()}
                continue
            delayed = is_delayed(pf, pd)
            if delayed[0]:
                statuses[pf] = {'status': 'DELAYED', 'asofdate': delayed[1].isoformat()}
                continue
            pending = is_pending(pf, pd)
            if pending[0]:
                statuses[pf] = {'status': 'PENDING', 'asofdate': pending[1].isoformat()}
                continue
        for s in statuses:            
            eta = PRICING_FEEDS[s]['normal_eta'].replace(year=pd.year, month=pd.month, day=pd.day)
            statuses[s]['normal_eta'] = eta.isoformat()
        return {
            'status': 'success',
            'data': statuses,
            'message': None
        }, 200
    except exc.SQLAlchemyError as e:
        logging.exception(e)
        return {
            'status': 'error',
            'data': None,
            'message': f'SQLAlchemy error: {e}'
        }, 500

@api.route('/api/pricing/feed-status')
class PricingFeedStatus(Resource):
    def get(self):
        return get_pricing_feed_status()
            
@api.route('/api/pricing/feed-status/<string:price_date>')
class PricingFeedStatusByDate(Resource):
    def get(self, price_date):
        return get_pricing_feed_status(price_date)           
        # BEGIN old code ... TODO_CLEANUP: remove when not needed
        # for col in mon:
        #     if pd.api.types.is_datetime64_any_dtype(mon[col]):
        #         # when date value is null, it comes out as NaT which Flask cannot encode into JSON
        #         mon[col] = mon[col].replace([pd.NaT], None)
        # return mon.to_dict('records')
        # END old code

def get_apx_SourceID(source):
    if source in APX_PX_SOURCES:
        return APX_PX_SOURCES[source]
    return APX_PX_SOURCES['DEFAULT']

def price_file_name(from_date, price_type=''):
    name = datetime.strptime(from_date, "%Y-%m-%d").strftime("%m%d%y")
    return f"{name}{price_type}.pri"

def prices_to_tab_delim_files(prices):
    # TODO_OPTIMIZE: consider making this query APX prices, and only include prices which are changes
    base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing')
    today_folder = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name='', rotate=False)
    files = []
    for from_date in prices:
        for pt in prices[from_date]:
            full_path = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name=price_file_name(from_date, price_type=APX_PRICE_TYPES[pt]), rotate=True)
            pxs = pd.DataFrame(columns=['apx_sec_type','apx_symbol',pt,'message','source'])
            for px in prices[from_date][pt]:
                # logging.info(px)
                px['source'] = get_apx_SourceID(px['source'])
                pxs = pd.concat([pxs, pd.DataFrame([px])], ignore_index=True)
            pxs.to_csv(path_or_buf=full_path, sep='\t', header=False, index=False)
            files.append(full_path)
    return today_folder, files

def add_price(res_prices, px):
    from_date = px.pop('from_date')
    if from_date not in res_prices:
        res_prices[from_date] = {}
    for pt in APX_PRICE_TYPES:
        if pt in px:
            if pt in res_prices[from_date]:
                res_prices[from_date][pt].append(px)
            else:
                res_prices[from_date][pt] = [px]
    return res_prices

@api.route('/api/pricing/price')
class PriceByIMEX(Resource):
    def post(self):
        payload = api.payload
        req_missing = []
        res_prices = {}
        if 'prices' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain prices"
            }, 422
        for px in payload['prices']:
            for rf in ['apx_sec_type','apx_symbol','source','price_value','from_date']:
                if rf not in px:
                    req_missing.append(rf)  # pd.concat([req_missing, rf])
            if len(req_missing):
                return {
                    'status': 'error',
                    'data': {
                        'price': px,
                        'missing fields': req_missing
                    },
                    'message': f"required field(s) missing"
                }, 422
            # passed all QA for this row! Add it to the dict
            res_prices = add_price(res_prices, px)
        folder, files = prices_to_tab_delim_files(res_prices)
        for f in files:
            # TODO_PROD: dynamically generate IMEX cmd based on env?
            imex_cmd = f"\\\\devapx-app01.leithwheeler.com\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -Ama \"-f{f}\" -ttab4 -u"
            logging.info('Triggering cmd: %s', imex_cmd)
            os.system(imex_cmd) # TODO_PROD: logging? error handling? 
            logging.info('IMEX complete.')
        return {
            'status': 'success',
            'data': res_prices,
            'message': f"prices were successfully saved to APX."
        }, 200

def get_manual_pricing_securities(data_date=date.today()):
    secs = PricingManualPricingSecurityTable().read()
    valid_secs = valid(secs, data_date)
    return valid_secs

@api.route('/api/pricing/manual-pricing-security')
class ManualPricingSecurity(Resource):
    def post(self):
        payload = api.payload
        if 'lw_id' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain lw_id"
            }, 422
        if not isinstance(payload['lw_id'], list):
            return {
                'status': 'error',
                'data': payload,
                'message': f"lw_id must be an array"
            }, 422
        old_rows = self.set_valid_to_yesterday(payload['lw_id'])
        payload = add_valid_dates(payload)
        payload = add_asof(payload)
        secs = pd.DataFrame.from_dict(payload)
        return save_df_to_table(secs, PricingManualPricingSecurityTable())
    
    # def post_old(self):
    #     payload = api.payload
    #     payload['valid_from'] = date.today()
    #     payload['valid_to'] = None
    #     if 'asofuser' not in payload:
    #         payload['asofuser'] = 'CJ'
    #     payload['asofdate'] = format_time(datetime.now())
    #     PricingManualPricingSecurityTable().bulk_insert(pd.DataFrame([payload]))
    
    def get(self):
        valid_secs = get_manual_pricing_securities()
        return clean(valid_secs)

    def delete(self):
        payload = api.payload
        if 'lw_id' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain lw_id"
            }, 422
        if not isinstance(payload['lw_id'], list):
            return {
                'status': 'error',
                'data': payload,
                'message': f"lw_id must be an array"
            }, 422
        updated_rows = self.set_valid_to_yesterday(lw_ids)
        if updated_rows:
            return {
                'status': 'success',
                'data': None,
                'message': f"deleted {updated_rows} manually priced securities for {lw_ids.join(',')}"
            }, 200
        else:
            return {
                'status': 'warning',
                'data': None,
                'message': f"found nothing to delete for {lw_ids.join(',')}"
            }, 200

    def set_valid_to_yesterday(self, lw_ids):
        mps_table = PricingManualPricingSecurityTable()
        new_vals = {'valid_to': date.today() + timedelta(days=-1)}
        new_vals = add_asof(new_vals)
        stmt = update(mps_table.table_def)\
                        .filter(mps_table.table_def.c.valid_to.in_(lw_ids))\
						.where(mps_table.table_def.c.valid_to == None)\
						.values(new_vals)
        updated_rows = mps_table._database.execute_write(stmt).rowcount
        logging.info(
            f"{mps_table.table_name}: Updated valid_to for {updated_rows} rows for {lw_ids.join(',')}."
        )
        return updated_rows
    
@api.route('/api/pricing/audit-trail/<string:price_date>')
class PricingAuditTrail(Resource):
    def post(self, price_date):
        payload = api.payload
        req_missing = []
        res_audit_trail = None
        if 'audit_trail' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain audit_trail"
            }, 422
        if not isinstance(payload['audit_trail'], list):
            return {
                'status': 'error',
                'data': payload,
                'message': f"audit_trail must be an array"
            }, 422
        for at in payload['audit_trail']:
            for rf in ['lw_id','source','reason','comment']:
                if rf not in at:
                    req_missing.append(rf)
            if len(req_missing):
                return {
                    'status': 'error',
                    'data': {
                        'audit_trail': at,
                        'missing fields': req_missing
                    },
                    'message': f"required field(s) missing"
                }, 422
            # passed all QA for this row! Add it to the df
            at = add_asof(at)
            at['data_dt'] = price_date
            if res_audit_trail is None:
                res_audit_trail = pd.DataFrame([at])
            else:
                res_audit_trail = pd.concat([res_audit_trail, pd.DataFrame([at])], ignore_index=True)
        return save_df_to_table(res_audit_trail, PricingAuditTrailTable())

    def get(self, price_date):
        audit_trail = PricingAuditTrailTable().read(data_date=price_date)
        return clean(audit_trail)

@api.route('/api/pricing/column-config/<string:user_id>')
class PricingColumnConfig(Resource):
    def post(self, user_id):
        payload = api.payload
        req_missing = []
        res_col_config = None
        if 'columns' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain columns"
            }, 422
        if not isinstance(payload['columns'], list):
            return {
                'status': 'error',
                'data': payload,
                'message': f"columns must be an array"
            }, 422
        for c in payload['columns']:
            for rf in ['name','hidden']:
                if rf not in c:
                    req_missing.append(rf)
            if len(req_missing):
                return {
                    'status': 'error',
                    'data': {
                        'column': c,
                        'missing fields': req_missing
                    },
                    'message': f"required field(s) missing"
                }, 422
            # passed all QA for this row! Add it to the df
            c['user_id'] = user_id
            if res_col_config is None:
                res_col_config = pd.DataFrame([c])
            else:
                res_col_config = pd.concat([res_col_config, pd.DataFrame([c])], ignore_index=True)
        old_rows = self.set_valid_to_yesterday(user_id)
        res_col_config = add_valid_dates(res_col_config)
        res_col_config = add_asof(res_col_config)
        return save_df_to_table(res_col_config, PricingColumnConfigTable())

    def get(self, user_id):
        column_config = PricingColumnConfigTable().read(user_id=user_id)
        valid_config = valid(column_config)
        return clean(valid_config)

    def delete(self, user_id):
        updated_rows = self.set_valid_to_yesterday(user_id)
        if updated_rows:
            return {
                'status': 'success',
                'data': None,
                'message': f"deleted {updated_rows} column configurations for {user_id}"
            }, 200
        else:
            return {
                'status': 'warning',
                'data': None,
                'message': f"found nothing to delete for {user_id}"
            }, 200

    def set_valid_to_yesterday(self, user_id):
        pcc_table = PricingColumnConfigTable()
        new_vals = {'valid_to': date.today() + timedelta(days=-1)}
        new_vals = add_asof(new_vals)
        stmt = update(pcc_table.table_def).\
						where(pcc_table.table_def.c.user_id == user_id).\
						where(pcc_table.table_def.c.valid_to == None).\
						values(new_vals)
        updated_rows = pcc_table._database.execute_write(stmt).rowcount
        logging.info(
            f"{pcc_table.table_name}: Updated valid_to for {updated_rows} rows for {user_id}."
        )
        return updated_rows
    
@api.route('/api/zIN-PROGRESS/pricing/count-by-source/<string:price_date>')
class PriceCountBySource(Resource):  # TODO_WAVE4: implement
    def get(self, price_date):
        return "{\"noteyet\": \"implemented\"}", 200
        prices = vPriceTable().read_for_date(data_date=price_date)
        price_source_counts = prices.groupby(['source'])['source'].count()
        logging.info(type(price_source_counts))
        logging.info(price_source_counts)
        # price_source_counts = price_source_counts.drop(['PXAPX','LWCOMPOSITE'])
        return clean(price_source_counts.to_frame().reset_index())

@api.route('/api/zIN-PROGRESS/holiday/<string:data_date>')
class HolidayByDate(Resource):  # TODO_WAVE2: actually implement
    def get(self, data_date):
        return "{\"noteyet\": \"implemented\"}", 200

@api.route('/api/zIN-PROGRESS/pricing/table-capture/<string:price_date>/<string:email>')
class PricingTableCaptureByDate(Resource):  # TODO_WAVE3: actually implement
    def post(self, price_date, email):        
        return "{\"noteyet\": \"implemented\"}", 200

@api.route('/api/zTEST/msteams/<string:msg>')  # TODO_CLEANUP: remove when not needed
class MSTeamsMessage(Resource):
    def get(self, msg):        
        teams_msg = pymsteams.connectorcard("https://leithwheeler.webhook.office.com/webhookb2/4e8ff835-529a-4e47-b0c1-50a4daa5ccc4@6c6ac5c1-edbd-4cb7-b2fc-3b1721ce9fef/IncomingWebhook/03520fc26aab48058544ba7dd5ca9056/60afe48d-2282-4374-a5dc-77776c36c1fd")
        teams_msg.text(msg)
        teams_msg.send()

@api.route('/api/zTEST/held-security')  # TODO_CLEANUP: remove when not needed
class HeldSecurity(Resource):
    def get(self):
        held = vHeldSecurityTable().read()
        return clean(held)

@api.route('/api/zTEST/portfolio')  # TODO_CLEANUP: remove when not needed
class Portfolio(Resource):
    def get(self):
        portfs = vPortfolioTable().read()
        return clean(portfs)

@api.route('/api/zTEST/pricing-attachment-file-path/<string:lw_id>')
class PricingAttachmentFilePathForLWID(Resource):
    def get(self, lw_id):
        base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing')
        full_path = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name='', rotate=False)
        return os.path.join(full_path, lw_id)

@api.route('/api/zTEST/portfolio/<string:portfolio_code>')  # TODO_CLEANUP: remove when not needed
class PortfolioByCode(Resource):
    def get(self, portfolio_code): 
        portf = vPortfolioTable().read(portfolio_code=portfolio_code)
        return clean(portf)

@api.route('/api/zTEST/position')  # TODO_CLEANUP: remove when not needed
class Position(Resource):
    def get(self):
        posns = vPositionTable().read()
        return clean(posns)

@api.route('/api/zTEST/price-storedproc')
class PriceByStoredProc(Resource):
    def post(self):
        logging.info('in post')
        payload = api.payload
        temp_prices = pd.DataFrame(columns=['_SPID','Date','Type','Symbol','PriceValue','SourceID','PriceTypeID','SecurityID','ThruDate'])
        req_missing = []
        px_hist_table = TempPriceHistoryTable()
        with Session(px_hist_table._database.engine) as session:
            logging.info('in session')
            spid_res = session.execute(text("select @@SPID as spid"))
            for r in spid_res:
                logging.info(r)
                logging.info(type(r))
                spid = r[0]
            logging.info(f"got SPID {spid}")
            for px in payload['prices']:
                for rf in ['apx_sec_type','apx_symbol','apx_security_id','source','price_type','price_value','from_date','thru_date']:
                    if rf not in px:
                        req_missing = pd.concat([req_missing, rf])
                if len(req_missing):
                    return {
                        'status': 'error',
                        'data': {
                            'price': px,
                            'missing fields': req_missing,
                        },
                        'message': f"required field(s) missing"
                    }, 422
                if px['from_date'] > px['thru_date']:
                    return {
                        'status': 'error',
                        'data': {
                            'price': px,
                        },
                        'message': f"thru_date must not be before from_date"
                    }, 422
                # passed all QA for this row! Add it to the df
                temp_px = {
                    '_SPID': spid,
                    'Date': px['from_date'],
                    'Type': px['apx_sec_type'],
                    'Symbol': px['apx_symbol'],
                    'PriceValue': px['price_value'],
                    'SourceID': get_apx_SourceID(px['source']),
                    'PriceTypeID': 1,  # px['price_type'],  # TODO_REFACTOR: carve out into func?
                    'SecurityID': px['apx_security_id'],
                    'ThruDate': px['thru_date'],
                }
                temp_prices = pd.concat([temp_prices, pd.DataFrame([temp_px])], ignore_index=True)
        # TODO_PROD: further QA checks?
        # TODO_NEXT: figure out how to insert @@SPID as _SPID
        # TODO_NEXT: figure out if PricePrec will auto-populate
        # TODO_NEXT: map lw.dbo.pricing.source to APX SourceID?
        # TODO_NEXT: insert into APXFirm.Temp.PriceHistory
            logging.info(f"About to insert temp prices... {temp_prices}")
            # px_hist_table.bulk_insert(temp_prices)
            row_cnt = temp_prices.to_sql(name=px_hist_table.table_name
                , con=px_hist_table._database.engine, schema=px_hist_table.schema
                , if_exists='append', index=False)
            logging.info(f"Done insert temp prices. {row_cnt} rows.")
        # TODO_NEXT: call pAxPriceHistoryPutBulk to inject to APX
        # TODO_NEXT: capture any SQL errors
        # TODO_NEXT: delete from APXFirm.Temp.PriceHistory

@api.route('/api/zTEST/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def get(self):
        curr_bday, prev_bday = '2023-01-04', '2023-01-03'  # get_current_bday(date.today()), get_previous_bday(date.today())
        # logging.info(request.json)
        # payload = api.payload
        # if 'price_type' in payload:
        #     return get_held_security_prices(curr_bday, prev_bday, payload['price_type'])
        return get_held_security_prices(curr_bday, prev_bday)

@api.route('/api/zTEST/pricing/held-security-price/<string:price_type>')
class HeldSecurityWithPricesByType(Resource):
    def get(self, price_type):
        curr_bday, prev_bday = '2023-01-04', '2023-01-03'  # get_current_bday(date.today()), get_previous_bday(date.today())
        return get_held_security_prices(curr_bday, prev_bday, price_type)

@api.route('/api/zTEST/pricing/held-security-price/<string:price_date>')
class HeldSecurityWithPricesByDate(Resource):
    def get(self, price_date):
        curr_bday, prev_bday = get_current_bday(datetime.strptime(price_date, '%Y%m%d')), get_previous_bday(datetime.strptime(price_date, '%Y%m%d'))
        # logging.info(curr_bday, prev_bday)
        # payload = api.payload
        # if 'price_type' in payload:
        #     return get_held_security_prices(curr_bday, prev_bday, payload['price_type'])
        return get_held_security_prices(curr_bday, prev_bday)

@api.route('/api/zTEST/pricing/held-security-price/<string:price_date>/<string:price_type>')
class HeldSecurityWithPricesByDateAndType(Resource):
    def get(self, price_date, price_type):
        curr_bday, prev_bday = get_current_bday(datetime.strptime(price_date, '%Y%m%d')), get_previous_bday(datetime.strptime(price_date, '%Y%m%d'))
        return get_held_security_prices(curr_bday, prev_bday, price_type)

@api.route('/api/zTEST/pricing/attachment-folder/<string:price_date>')
class PricingAttachmentFilePath(Resource):  # TODO_CLEANUP: remove if not needed
    def get(self, price_date):
        return get_pricing_attachment_folder(price_date)

@api.route('/api/zzOLD/pricing/held-security-price/<string:price_type>')
class HeldSecurityWithPricesByType(Resource):
    def get(self, price_type):
        curr_bday, prev_bday = '2023-01-04', '2023-01-03'  # get_current_bday(date.today()), get_previous_bday(date.today())
        return get_held_security_prices(curr_bday, prev_bday, price_type)

@api.route('/api/zzOLD/pricing/held-security-price/<string:price_date>')
class HeldSecurityWithPricesByDate(Resource):
    def get(self, price_date):
        curr_bday, prev_bday = get_current_bday(datetime.strptime(price_date, '%Y%m%d')), get_previous_bday(datetime.strptime(price_date, '%Y%m%d'))
        # logging.info(curr_bday, prev_bday)
        # payload = api.payload
        # if 'price_type' in payload:
        #     return get_held_security_prices(curr_bday, prev_bday, payload['price_type'])
        return get_held_security_prices(curr_bday, prev_bday)

@api.route('/api/zzOLD/pricing/held-security-price/<string:price_date>/<string:price_type>')
class HeldSecurityWithPricesByDateAndType(Resource):
    def get(self, price_date, price_type):
        curr_bday, prev_bday = get_current_bday(datetime.strptime(price_date, '%Y%m%d')), get_previous_bday(datetime.strptime(price_date, '%Y%m%d'))
        return get_held_security_prices(curr_bday, prev_bday, price_type)

class PricingNotificationSubscriptionSchema(Schema):  # TODO_CLEANUP: remove when not needed
    email = fields.Email(required=True)
    feed_name = fields.Str(required=True)
    email_on_pending = fields.Bool(load_default=True, dump_default=True)
    email_on_in_progress = fields.Bool(load_default=True, dump_default=True)
    email_on_complete = fields.Bool(load_default=True, dump_default=True)
    email_on_error = fields.Bool(load_default=True, dump_default=True)
    email_on_delayed = fields.Bool(default=1, missing=1, load_default=1, dump_default=1)
    # Clean up data
    @pre_load
    def process_input(self, data, **kwargs):
        data["email"] = data["email"].lower().strip()
        return data


class Command(BaseCommand):
    help = 'Run local flask server'

    def handle(self, *args, **kwargs):
        """
        Run local flask server:
        """

        logging.info('Starting local flask server...')
        app.run(host='0.0.0.0', port=5000, debug=True)
        return EXIT_SUCCESS
    


if __name__ == '__main__':
    sys.exit(Command().run_from_argv())



