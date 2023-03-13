
import logging
import numpy as np
import os
import pandas as pd
import sys
from datetime import date, datetime, timedelta
from email.utils import parseaddr
from flask import Flask, json, jsonify
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from flask_restx import Api, Resource, fields
from marshmallow import Schema, fields, ValidationError, pre_load
import pymsteams
from sqlalchemy import exc, text
from sqlalchemy.orm import Session

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.db.apxdb.temppricehistory import TempPriceHistoryTable
from lw.db.coredb.pricingauditreason import PricingAuditReasonTable
from lw.db.coredb.pricingaudittrail import PricingAuditTrailTable
from lw.db.coredb.pricingnotificationsubscription import PricingNotificationSubscriptionTable
from lw.db.coredb.vprice import vPriceTable
from lw.db.coredb.vheldsecurity import vHeldSecurityTable
from lw.db.coredb.vportfolio import vPortfolioTable
from lw.db.coredb.vposition import vPositionTable
from lw.db.coredb.vtransaction import vTransactionTable
from lw.db.mgmtdb.monitor import MonitorTable
from lw.util.date import format_time, get_current_bday, get_previous_bday
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

APX_PX_SOURCES = {
    # 'LWDB source': 'APX source ID'  # APX source name
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


def clean(df):
    df.replace({np.nan: None, pd.NaT: None}, inplace = True)
    res_dict = df.to_dict('records')
    res_str = jsonify(res_dict)
    return res_str

@api.route('/api/zTEST/msteams/<string:msg>')  # TODO: remove when not needed
class MSTeamsMessage(Resource):
    def get(self, msg):        
        teams_msg = pymsteams.connectorcard("https://leithwheeler.webhook.office.com/webhookb2/4e8ff835-529a-4e47-b0c1-50a4daa5ccc4@6c6ac5c1-edbd-4cb7-b2fc-3b1721ce9fef/IncomingWebhook/03520fc26aab48058544ba7dd5ca9056/60afe48d-2282-4374-a5dc-77776c36c1fd")
        teams_msg.text(msg)
        teams_msg.send()

def trim_px_sources(raw_prices):
    prices = raw_prices
    prices['source'] = prices['source'].apply(lambda x: 'BLOOMBERG' if (x[:3] == 'BB_' and '_PX' not in x) else x)
    prices['source'] = prices['source'].apply(lambda x: 'FTSE' if x == 'FTSETMX_PX' else x)
    prices['source'] = prices['source'].apply(lambda x: 'FUNDRUN' if x == 'FUNDRUN_EQUITY' else x)
    prices['source'] = prices['source'].apply(lambda x: 'MANUAL' if x == 'FIDESK_MANUALPRICE' else x)
    prices['source'] = prices['source'].apply(lambda x: 'MISSING' if x == 'FIDESK_MISSINGPRICE' else x)
    prices = prices[prices['source'].isin(
        ['BLOOMBERG','FTSE','FUNDRUN','MARKIT','MANUAL','MISSING','RBC']
    )]
    return prices

@api.route('/api/pricing/price/<string:price_date>')
class PriceByDate(Resource):
    def get(self, price_date):
        prices = vPriceTable().read_for_date(data_date=price_date)
        prices = trim_px_sources(prices)
        return clean(prices)

@api.route('/api/zIN-PROGRESS/pricing/count-by-source/<string:price_date>')
class PriceCountBySource(Resource):
    def get(self, price_date):
        return "{\"noteyet\": \"implemented\"}", 200
        prices = vPriceTable().read_for_date(data_date=price_date)
        price_source_counts = prices.groupby(['source'])['source'].count()
        logging.info(type(price_source_counts))
        logging.info(price_source_counts)
        # price_source_counts = price_source_counts.drop(['PXAPX','LWCOMPOSITE'])
        return clean(price_source_counts.to_frame().reset_index())

@api.route('/api/zTEST/held-security')  # TODO: remove when not needed
class HeldSecurity(Resource):
    def get(self):
        held = vHeldSecurityTable().read()
        return clean(held)

@api.route('/api/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def get(self):
        curr_bday, prev_bday = '2023-01-04', '2023-01-03'  # get_current_bday(date.today()), get_previous_bday(date.today())
        # logging.info(curr_bday, prev_bday)
        held = vHeldSecurityTable().read()
        held['prices'] = None
        curr_prices = vPriceTable().read(data_date=curr_bday)
        curr_prices = trim_px_sources(curr_prices)
        prev_prices = vPriceTable().read(data_date=prev_bday, source='PXAPX')
        prev_prices['source'] = 'APX'
        for i, row in held.iterrows():
            lw_id = row['lw_id']
            sec_curr_prices = curr_prices.loc[curr_prices['lw_id'] == lw_id]
            sec_prev_prices = prev_prices.loc[prev_prices['lw_id'] == lw_id]
            prices = pd.concat([sec_curr_prices, sec_prev_prices], ignore_index=True)
            prices.replace({np.nan: None, pd.NaT: None}, inplace = True)
            held.at[i, 'prices'] = prices.to_dict('records')
            audit_trail = PricingAuditTrailTable().read(data_date=curr_bday, lw_id=lw_id)
        # TODO: add audit reason & comment
        # TODO: add "chosen" price - need to implement hierarchy logic
        # TODO: facilitate input JSON for "type": manual/missing/override (if not provided, return all)
        # TODO: add "good_thru_date" in top level output (incl holiday logic for wave 2)
        # TODO: refactor - share logic with below endpoint
        return clean(held)

@api.route('/api/pricing/held-security-price/<string:price_date>')
class HeldSecurityWithPricesByDate(Resource):
    def get(self, price_date):
        curr_bday, prev_bday = get_current_bday(datetime.strptime(price_date, '%Y%m%d')), get_previous_bday(datetime.strptime(price_date, '%Y%m%d'))
        # logging.info(curr_bday, prev_bday)
        held = vHeldSecurityTable().read()
        held['prices'] = None
        curr_prices = vPriceTable().read(data_date=curr_bday)
        curr_prices = trim_px_sources(curr_prices)
        prev_prices = vPriceTable().read(data_date=prev_bday, source='PXAPX')
        prev_prices['source'] = 'APX'
        for i, row in held.iterrows():
            lw_id = row['lw_id']
            sec_curr_prices = curr_prices.loc[curr_prices['lw_id'] == lw_id]
            sec_prev_prices = prev_prices.loc[prev_prices['lw_id'] == lw_id]
            prices = pd.concat([sec_curr_prices, sec_prev_prices], ignore_index=True)
            held.at[i, 'prices'] = prices.to_dict('records')
        # TODO: if price_date is not today, read historical positions in order to produce correct "held" list
        return clean(held)

@api.route('/api/zTEST/portfolio')  # TODO: remove when not needed
class Portfolio(Resource):
    def get(self):
        portfs = vPortfolioTable().read()
        return clean(portfs)

def get_pricing_attachment_folder(data_date):
        base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing')
        full_path = prepare_dated_file_path(folder_name=base_path, date=datetime.strptime(data_date, '%Y%m%d'), file_name='', rotate=False)
        return full_path

@api.route('/api/pricing/attachment-folder/<string:data_date>')
class PricingAttachmentFilePath(Resource):
    def get(self, data_date):
        return get_pricing_attachment_folder(data_date)

@api.route('/api/pricing/attachment/<string:data_date>')
class PricingAttachmentByDate(Resource):
    # TODO: create POST to take array of binary file contents and file names and save in folder
    def get(self, data_date):
        full_path = get_pricing_attachment_folder(data_date)
        files = []
        with os.scandir(full_path) as entries:
            for entry in entries:
                files.append(os.path.join(full_path, entry))
        return files

@api.route('/api/zTEST/pricing-attachment-file-path/<string:lw_id>')
class PricingAttachmentFilePathForLWID(Resource):
    def get(self, lw_id):
        base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing')
        full_path = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name='', rotate=False)
        return os.path.join(full_path, lw_id)

def valid(df, data_date=date.today()):  
# TODO: put into library; put function header; write unit tests?
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

class PricingNotificationSubscriptionSchema(Schema):  # TODO: delete when confirmed not needed
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

@api.route('/api/pricing/notification-subscription')
class PricingNotificationSubscription(Resource):
    def get(self):
        subs = PricingNotificationSubscriptionTable().read()
        valid_subs = valid(subs)[['email','feed_name','email_on_pending','email_on_in_progress','email_on_complete','email_on_error','email_on_delayed']]
        return clean(valid_subs)
    
    def post(self):
        payload = api.payload
        # below using marshmallow ... may continue trying this if flask_marshmallow isn't sufficient
        # try:
        #     data = PricingNotificationSubscriptionSchema().load(payload)
        # except ValidationError as err:
        #     return err.messages, 422
        if 'email' not in payload or 'feed_name' not in payload:
            return 'required field(s) missing: email, feed_name', 422
        email = parseaddr(payload['email'])[1]
        if email == '':
            return 'invalid email: ', 422
        payload['valid_from'] = date.today()
        payload['valid_to'] = None
        payload['asofuser'] = 'CJ'  # TODO: replace with actual user and/or hostname
        payload['asofdate'] = format_time(datetime.now())
        logging.info(payload)
        PricingNotificationSubscriptionTable().bulk_insert(pd.DataFrame([payload]))
    
    def delete(self):
        payload = api.payload
        if 'email' not in payload or 'feed_name' not in payload:
            return 'required field(s) missing: email, feed_name', 422
        email = parseaddr(payload['email'])[1]
        if email == '':
            return 'invalid email: ', 422
        pns_table = PricingNotificationSubscriptionTable()
        stmt = sql.update(pns_table.table_def).\
						where(pns_table.table_def.c.email == email).\
						where(pns_table.table_def.c.feed_name == feed_name).\
						values(valid_to=date.today() + timedelta(days=-1))
        updated_rows = pns_table._database.execute_write(stmt)
        logging.info(
            f"{pns_table.table_name}: Updated valid_to for {updated_rows} rows for {email} {feed_name}."
        )

@api.route('/api/zTEST/portfolio/<string:portfolio_code>')  # TODO: remove when not needed
class PortfolioByCode(Resource):
    def get(self, portfolio_code): 
        portf = vPortfolioTable().read(portfolio_code=portfolio_code)
        return clean(portf)

@api.route('/api/zTEST/position')  # TODO: remove when not needed
class Position(Resource):
    def get(self):
        posns = vPositionTable().read()
        return clean(posns)

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

@api.route('/api/pricing/feed-status')
class PricingFeedStatus(Resource):
    def get(self):  
        # TODO: refactor? Share logic with below endpoint. Maybe create a PricingFeed class w subclasses for each feed
        data_date = date.today()
        statuses = {}
        try:
            for pf in PRICING_FEEDS:
                error = is_error(pf, data_date)
                if error[0]:
                    statuses[pf] = {'status': 'ERROR', 'asofdate': error[1].isoformat()}
                    continue
                priced = is_priced(pf, data_date)
                if priced[0]:
                    statuses[pf] = {'status': 'PRICED', 'asofdate': priced[1].isoformat()}
                    continue
                in_progress = is_in_progress(pf, data_date)
                if in_progress[0]:
                    statuses[pf] = {'status': 'IN PROGRESS', 'asofdate': in_progress[1].isoformat()}
                    continue
                delayed = is_delayed(pf, data_date)
                if delayed[0]:
                    statuses[pf] = {'status': 'DELAYED', 'asofdate': delayed[1].isoformat()}
                    continue
                pending = is_pending(pf, data_date)
                if pending[0]:
                    statuses[pf] = {'status': 'PENDING', 'asofdate': pending[1].isoformat()}
                    continue
            for s in statuses:
                statuses[s]['normal_eta'] = PRICING_FEEDS[s]['normal_eta'].isoformat()
            return statuses
        except exc.SQLAlchemyError as err:  # TODO: return code based on what the error is
            logging.info(err.code, err)
            return None
            
@api.route('/api/pricing/feed-status/<string:data_date>')
class PricingFeedStatusByDate(Resource):
    def get(self, data_date):
        statuses = {}
        try:
            for pf in PRICING_FEEDS:
                error = is_error(pf, data_date)
                if error[0]:
                    statuses[pf] = {'status': 'ERROR', 'asofdate': error[1].isoformat()}
                    continue
                priced = is_priced(pf, data_date)
                if priced[0]:
                    statuses[pf] = {'status': 'PRICED', 'asofdate': priced[1].isoformat()}
                    continue
                in_progress = is_in_progress(pf, data_date)
                if in_progress[0]:
                    statuses[pf] = {'status': 'IN PROGRESS', 'asofdate': in_progress[1].isoformat()}
                    continue
                delayed = is_delayed(pf, data_date)
                if delayed[0]:
                    statuses[pf] = {'status': 'DELAYED', 'asofdate': delayed[1].isoformat()}
                    continue
                pending = is_pending(pf, data_date)
                if pending[0]:
                    statuses[pf] = {'status': 'PENDING', 'asofdate': pending[1].isoformat()}
                    continue
            for s in statuses:
                statuses[s]['normal_eta'] = PRICING_FEEDS[s]['normal_eta'].isoformat()
            return statuses
        except exc.SQLAlchemyError as err:  # TODO: return code based on what the error is
            logging.info(err.code, err)
            return None
            
        # BEGIN old code ... TODO: remove this once confirmed not needed
        # for col in mon:
        #     if pd.api.types.is_datetime64_any_dtype(mon[col]):
        #         # when date value is null, it comes out as NaT which Flask cannot encode into JSON
        #         # TODO: better way of handling this... maybe run every resultset through a generic "clean" function
        #         mon[col] = mon[col].replace([pd.NaT], None)
        # # TODO: trim resultset to only rows/cols relevant for pricing feeds
        # return mon.to_dict('records')
        # END old code

def get_apx_SourceID(source):
    if source in APX_PX_SOURCES:
        return APX_PX_SOURCES[source]
    return APX_PX_SOURCES['DEFAULT']

def price_file_name(from_date):
    name = datetime.strptime(from_date, "%Y-%m-%d").strftime("%m%d%y")
    return f"{name}.pri"

def prices_to_tab_delim_files(prices):
    base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing')
    today_folder = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name='', rotate=False)
    files = []
    for from_date in prices:
        full_path = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name=price_file_name(from_date), rotate=True)
        pxs = pd.DataFrame(columns=['apx_sec_type','apx_symbol','price_value','message','source'])
        for px in prices[from_date]:
            px['source'] = get_apx_SourceID(px['source'])
            pxs = pd.concat([pxs, pd.DataFrame([px])], ignore_index=True)
        pxs.to_csv(path_or_buf=full_path, sep='\t', header=False, index=False)
        files.append(full_path)
    return today_folder, files

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
                        'missing fields': req_missing,
                    },
                    'message': f"required field(s) missing"
                }, 422
            # passed all QA for this row! Add it to the dict
            from_date = px.pop('from_date')
            if from_date not in res_prices:
                res_prices[from_date] = [px]
            else:
                res_prices[from_date].append(px)
        folder, files = prices_to_tab_delim_files(res_prices)
        for f in files:
            # TODO: dynamically generate IMEX cmd based on env?
            imex_cmd = f"\\\\devapx-app01.leithwheeler.com\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -Ama \"-f{f}\" -ttab4 -u"
            logging.info('Triggering cmd: %s', imex_cmd)
            os.system(imex_cmd) # TODO: logging? error handling? 
            logging.info('IMEX complete.')
        return {
            'status': 'success',
            'data': res_prices,
            'message': f"prices were successfully saved to APX."
        }, 200

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
                    'PriceTypeID': 1,  # px['price_type'],  # TODO: carve out into func?
                    'SecurityID': px['apx_security_id'],
                    'ThruDate': px['thru_date'],
                }
                temp_prices = pd.concat([temp_prices, pd.DataFrame([temp_px])], ignore_index=True)
        # TODO: further QA checks?
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

@api.route('/api/zIN-PROGRESS/pricing/table-capture/<string:data_date>/<string:email>')  # TODO: actually implement
class PricingTableCaptureByDate(Resource):
    def post(self, data_date, email):        
        return "{\"noteyet\": \"implemented\"}", 200

@api.route('/api/zIN-PROGRESS/holiday/<string:data_date>')  # TODO: actually implement
class HolidayByDate(Resource):
    def get(self, data_date):
        return "{\"noteyet\": \"implemented\"}", 200

@api.route('/api/zIN-PROGRESS/pricing/manual-pricing-security')  # TODO: actually implement
class ManualPricingSecurity(Resource):
    def get(self):
        return "{\"noteyet\": \"implemented\"}", 200

@api.route('/api/zIN-PROGRESS/pricing/audit-trail')  # TODO: actually implement
class PricingAuditTrail(Resource):
    def get(self):
        return "{\"noteyet\": \"implemented\"}", 200

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



