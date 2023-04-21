
# TODO_REFACTOR: clean up imports, remove ones not used
import aiohttp
import asyncio
import json
import logging
import numpy as np
import os
import pandas as pd
import socket
import sys
from datetime import date, datetime, timedelta
from email.utils import parseaddr
from email_validator import validate_email, EmailNotValidError
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from flask_restx import Api, Resource, fields
from marshmallow import Schema, fields, ValidationError, pre_load
import pymsteams
from sqlalchemy import exc, text, update, or_, and_
from sqlalchemy.orm import Session
from threading import Thread

# Import LW libs ( PF version )
# TODO_PROD: better way to do this - detect environment somehow? 
if os.environ.get('pythonpath') is None:
    pythonpath = '\\\\dev-data\\lws$\\cameron\\lws\\libpy\\lib'
    os.environ['pythonpath'] = pythonpath
    sys.path.append(pythonpath)

from lw import config
from lw.core.command import BaseCommand
from lw.core import EXIT_SUCCESS, EXIT_FAILURE
from lw.db.apxdb.temppricehistory import TempPriceHistoryTable
from lw.db.coredb.pricingauditreason import PricingAuditReasonTable
from lw.db.coredb.pricingaudittrail import PricingAuditTrailTable
from lw.db.coredb.pricingcolumnconfig import PricingColumnConfigTable
from lw.db.coredb.pricingmanualpricingsecurity import PricingManualPricingSecurityTable
from lw.db.coredb.pricingnotificationsubscription import PricingNotificationSubscriptionTable
from lw.db.coredb.vw_price import vwPriceTable
from lw.db.coredb.vw_held_security import vwHeldSecurityTable
from lw.db.coredb.vw_portfolio import vwPortfolioTable
from lw.db.coredb.vw_position import vwPositionTable
from lw.db.coredb.vw_security import vwSecurityTable
from lw.db.coredb.vw_transaction import vwTransactionTable
from lw.db.mgmtdb.monitor import MonitorTable
from lw.db.lwdb.apx_appraisal import ApxAppraisalTable
from lw.db.lwdb.pricing import PricingTable
# from lw.util.dataframe import NaN_NaT_to_none
from lw.util.date import format_time, get_current_bday, get_previous_bday, get_next_bday
from lw.util.file import prepare_dated_file_path

import flaskrp_helper


# globals
app = Flask(__name__)
ma = Marshmallow(app)
api = Api(app)
CORS(app)


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
                'message': f'Required field(s) missing: email, feed_name'
            }, 422
        # email = parseaddr(payload['email'])[1]
        try:
            email = validate_email(payload['email'])["email"]
        except EmailNotValidError as e:
            return {
                'status': 'error',
                'data': None,
                'message': str(e)
            }, 422
        old_rows = self.set_valid_to_yesterday(email, payload['feed_name'])
        payload = flaskrp_helper.add_valid_dates(payload)
        payload = flaskrp_helper.add_asof(payload)
        payload_df = pd.DataFrame([payload])
        payload_df = payload_df.rename(columns=lambda x: 'is_' + x if 'email_on_' in x else x)  # Add "is_" prefix to match DB column names
        return flaskrp_helper.save_df_to_table(payload_df, PricingNotificationSubscriptionTable())
    
    def get(self):
        subs = PricingNotificationSubscriptionTable().read()
        subs = subs.rename(columns=lambda x: x[3:] if 'is_email_on_' in x else x)  # Remove "is_" prefix from DB column names
        valid_subs = flaskrp_helper.valid(subs)[['email','feed_name','email_on_pending','email_on_in_progress','email_on_complete','email_on_error','email_on_delayed']]
        return flaskrp_helper.clean(valid_subs)

    def delete(self):
        payload = api.payload
        if 'email' not in payload or 'feed_name' not in payload:
            return {
                'status': 'error',
                'data': None,
                'message': f'Required field(s) missing: email, feed_name'
            }, 422
        # email = parseaddr(payload['email'])[1]
        try:
            email = validate_email(payload['email'])["email"]
        except EmailNotValidError as e:
            return {
                'status': 'error',
                'data': None,
                'message': str(e)
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
        new_vals = {'valid_to_date': date.today() + timedelta(days=-1)}
        new_vals = flaskrp_helper.add_asof(new_vals)
        stmt = update(pns_table.table_def).\
                        where(pns_table.table_def.c.email == email).\
                        where(pns_table.table_def.c.feed_name == feed_name).\
                        where(pns_table.table_def.c.valid_to_date == None).\
                        values(new_vals)
        updated_rows = pns_table._database.execute_write(stmt).rowcount
        logging.debug(
            f"{pns_table.table_name}: Updated valid_to_date for {updated_rows} rows for {email} {feed_name}."
        )
        return updated_rows
    
@api.route('/api/pricing/feed-status')
class PricingFeedStatus(Resource):
    def get(self):
        return flaskrp_helper.get_pricing_feed_status()
            
@api.route('/api/pricing/feed-status/<string:price_date>')
class PricingFeedStatusByDate(Resource):
    def get(self, price_date):
        return flaskrp_helper.get_pricing_feed_status(price_date)           
        # BEGIN old code ... TODO_CLEANUP: remove when not needed
        # for col in mon:
        #     if pd.api.types.is_datetime64_any_dtype(mon[col]):
        #         # when date value is null, it comes out as NaT which Flask cannot encode into JSON
        #         mon[col] = mon[col].replace([pd.NaT], None)
        # return mon.to_dict('records')
        # END old code

@api.route('/api/transaction/<string:trade_date>')
class TransactionByDate(Resource):
    def get(self, trade_date):
        txns = vwTransactionTable().read_for_trade_date(trade_date=trade_date)
        return flaskrp_helper.clean(txns)

@api.route('/api/pricing/audit-reason')
class PricingAuditReason(Resource):
    def get(self):
        reasons = PricingAuditReasonTable().read()
        valid_reasons = flaskrp_helper.valid(reasons)[['reason']]
        return flaskrp_helper.clean(valid_reasons)

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
            return flaskrp_helper.save_binary_files(price_date, payload['files'])
    
    def get(self, price_date):
        full_path = flaskrp_helper.get_pricing_attachment_folder(price_date)
        files = []
        for f in os.listdir(full_path):
            files.append(os.path.join(full_path, f))
        return flaskrp_helper.clean(pd.DataFrame(files, columns=['full_path']))

    def delete(self, price_date):
        full_path = flaskrp_helper.get_pricing_attachment_folder(price_date)
        return flaskrp_helper.delete_dir_contents(full_path)

@api.route('/api/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def post(self):
        # Note this is not really a standard "post" as it does not save data - but is created as such 
        # because we want to accept optional params in the payload rather than the URL, and 
        # some clients such as AngularJS cannot do so for a GET request.
        payload = api.payload
        logging.info(f'HeldSecurityWithPrices POST payload: {payload}')
        if 'sec_type' in payload:
            sec_type = payload['sec_type']
        else:
            sec_type = None
        if 'price_date' in payload:
            price_date = payload['price_date']
        else:
            price_date = date.today()
        if 'source' in payload:
            source = payload['source'].upper()
        else:
            source = None
        curr_bday, prev_bday = get_current_bday(price_date), get_previous_bday(price_date)
        if 'no_cache' in payload:
            res = flaskrp_helper.get_held_security_prices(curr_bday, prev_bday, sec_type, source)
            api_cache_res = flaskrp_helper.cache_result(endpointurl=f'{api.base_url}api/pricing/held-security-price'
                , request_type='POST', payload=json.dumps(payload), result_code=200, result_text=res.get_data(as_text=True)
            )
            return res
        else:
            last_cache = flaskrp_helper.get_last_cache(endpointurl=f'{api.base_url}api/pricing/held-security-price'
                , request_type='POST', payload=json.dumps(payload))
            last_update_ts = flaskrp_helper.get_last_update(securities=False, prices=True, positions=True, data_date=price_date)
            if len(last_cache.index):
                last_cache_ts = last_cache['modified_at'].iloc[0]
                logging.info(f'Cache at {last_cache_ts}, update at {last_update_ts}')
                if last_cache_ts > last_update_ts:
                    return jsonify(json.loads(last_cache['result_text'].iloc[0]))
        # logging.info(request.json)
        # payload = api.payload
        # if 'source' in payload:
        #     return get_held_security_prices_v1(curr_bday, prev_bday, payload['source'])

        res = flaskrp_helper.get_held_security_prices(curr_bday, prev_bday, sec_type, source)
        api_cache_res = flaskrp_helper.cache_result(endpointurl=f'{api.base_url}api/pricing/held-security-price'
            , request_type='POST', payload=json.dumps(payload), result_code=200, result_text=res.get_data(as_text=True)
        )
        return res

@api.route('/api/pricing/price/<string:price_date>')
class PriceByDate(Resource):
    def get(self, price_date):
        prices = vwPriceTable().read_for_date(data_date=price_date)
        prices = flaskrp_helper.trim_px_sources(prices)
        return flaskrp_helper.clean(prices)

@api.route('/api/pricing/price')
class PriceByIMEX(Resource):
    
    def post(self):
        payload = api.payload
        logging.info(f'PriceByIMEX POST payload: {payload}')
        # TODO: Replace "xxx_value" with "xxx"?
        # payload = {k[:-6]:payload[k] if k.endswith('_value') else k:payload[k] for k in payload}
        req_missing = []
        res_prices = {}
        secs = vwSecurityTable().read()
        secs = flaskrp_helper.rename_cols_pms2apx(secs)
        if 'prices' not in payload:
            return {
                'status': 'error',
                'data': payload,
                'message': f"payload must contain prices"
            }, 422
        for px in payload['prices']:
            for rf in ['apx_sec_type','apx_symbol','source','from_date']:
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
            # In addition to above required fields, at least one of below must be provided:
            valid_price_types = flaskrp_helper.apx_price_types_list()
            for rf in valid_price_types + ['not_found']:
                pt_str = ', '.join(valid_price_types)
                if rf == 'not_found':
                    return {
                        'status': 'error',
                        'data': {
                            'price': px,
                            'missing fields': f"{pt_str}"
                        },
                        'message': f"At least one of ({pt_str}) must be provided."
                    }, 422
                if rf in px:
                    break
            # passed all QA for this row!
            # If not manual/override, there is nothing to do since we do not save the price/yield/dur anywhere.
            # But if manual/override, we save to LWDB pricing table and APX:            
            if px['source'] in ('MANUAL', 'OVERRIDE'):
                res_prices = flaskrp_helper.add_price(res_prices, px)
                lw_source = 'LW_' + px['source']
                sec = secs[secs['apx_symbol'] == px['apx_symbol']]
                if len(sec.index):
                    sec = sec.to_dict(orient='records')[0]
                else:
                    return {
                        'status': 'error',
                        'data': px,
                        'message': f"Could not find security with apx_symbol: {px['apx_symbol']}!"
                    }, 500
                sec['data_dt'] = px['from_date']
                for pt in flaskrp_helper.apx_price_types_list():
                    if pt in px:
                        if pt == 'duration':
                            sec['moddur'] = px[pt]
                        else:
                            sec[pt] = px[pt]
                sec['source'] = lw_source
                sec = flaskrp_helper.add_asof(sec)
                sec['asofdate'] = sec['scenariodate'] = sec['modified_at']
                sec['asofuser'] = sec['modified_by']
                # Insert to pricing table
                pt = PricingTable()
                sec['scenario'] = pt.base_scenario = 'LW_SEC_PRICING'
                extra_where = and_(
                    pt.table_def.c.source == sec['source'],
                    pt.table_def.c.lw_id == sec['lw_id']
                )
                res = pt.rotate(data_date=datetime.strptime(sec['data_dt'], '%Y-%m-%d'), extra_where=extra_where)
                logging.debug(f'{res} rows rotated.')
                res = pt.bulk_insert(pd.DataFrame.from_dict([sec]))
                if 'yield' in sec:
                    # Since yield is a python keyword, it does not get included in the initial bulk insert.
                    # Solution above is to update it after the fact... 
                    # See https://stackoverflow.com/q/58765676
                    stmt = update(pt.table_def).\
                        where(pt.table_def.c.scenario == sec['scenario']).\
                        where(pt.table_def.c.data_dt == sec['data_dt']).\
                        where(pt.table_def.c.source == sec['source']).\
                        where(pt.table_def.c.lw_id == sec['lw_id']).\
                        values(**{'yield': sec['yield']})
                    updated_rows = pt._database.execute_write(stmt)
                    logging.debug(f'updated yield for {updated_rows} rows.')
        try:
            folder, files, changed_prices = flaskrp_helper.prices_to_tab_delim_files(res_prices)
        except flaskrp_helper.SecurityNotFoundException as e:
            return {
                'status': 'error',
                'data': px,
                'message': f"Could not find security with {e.missing_col_name} {e.missing_col_value}!"
            }, 500
        if not len(changed_prices):
            return {
                'status': 'warning',
                'data': res_prices,
                'message': f"All prices are the same as current APX, therefore no prices were loaded."
            }, 200
        for f in files:
            # TODO_PROD: dynamically generate IMEX cmd based on env?
            # TODO_CLEANUP: remove below when not needed
            # imex_cmd = f"\\\\devapx-app01.leithwheeler.com\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -Ama \"-f{f}\" -ttab4 -u"
            # logging.info('Triggering cmd: %s', imex_cmd)
            # os.system(imex_cmd) # TODO_PROD: logging? error handling? 
            # logging.info('IMEX complete.')

            # loop = asyncio.get_event_loop()
            # task = loop.create_task(flaskrp_helper.trigger_imex(folder, f))

            logging.info('Calling trigger_imex...')
            # task = asyncio.run(flaskrp_helper.trigger_imex(folder, f))
            # asyncio_thread = Thread(target=self.async_send_request, args=(folder, f))
            asyncio_thread = Thread(target=flaskrp_helper.trigger_imex, args=(folder, f))
            asyncio_thread.daemon = True
            asyncio_thread.start()
            logging.info('Done calling trigger_imex.')

            # output, error, return_code = flaskrp_helper.trigger_imex(folder, f) 
        logging.debug(f"Changed prices df: {changed_prices}")
        # changed_prices = changed_prices.to_dict('records')
        logging.debug(f"Changed prices dict: {changed_prices}")
        # Dict comprehension within list comprehension... not the most readable, but all that's happening here is
        # removing the 'message' from each dict element. It is expected to be NaN and meaningless / could cause errors.
        changed_prices = [
            {k:p[k] for k in p if k != 'message'}
            for p in changed_prices
        ]
        return {
            'status': 'success',
            'data': changed_prices,
            'message': f"Prices are being sent to APX."
        }, 200

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
        old_rows = self.set_valid_to_yesterday()
        payload = flaskrp_helper.add_valid_dates(payload)
        payload = flaskrp_helper.add_asof(payload)
        secs = pd.DataFrame.from_dict(payload)
        return flaskrp_helper.save_df_to_table(secs, PricingManualPricingSecurityTable())
    
    # def post_old(self):
    #     payload = api.payload
    #     payload['valid_from'] = date.today()
    #     payload['valid_to'] = None
    #     if 'asofuser' not in payload:
    #         payload['asofuser'] = 'CJ'
    #     payload['asofdate'] = format_time(datetime.now())
    #     PricingManualPricingSecurityTable().bulk_insert(pd.DataFrame([payload]))
    
    def get(self):
        valid_secs = flaskrp_helper.get_manual_pricing_securities()
        return flaskrp_helper.clean(valid_secs)

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
        updated_rows = self.set_valid_to_yesterday(payload['lw_id'])
        if updated_rows:
            return {
                'status': 'success',
                'data': None,
                'message': f"deleted {updated_rows} manually priced securities for {','.join(payload['lw_id'])}"
            }, 200
        else:
            return {
                'status': 'warning',
                'data': None,
                'message': f"found nothing to delete for {','.join(payload['lw_id'])}"
            }, 200

    def set_valid_to_yesterday(self, lw_ids=None):
        mps_table = PricingManualPricingSecurityTable()
        new_vals = {'valid_to_date': date.today() + timedelta(days=-1)}
        new_vals = flaskrp_helper.add_asof(new_vals)
        stmt = update(mps_table.table_def)\
                        .where(or_(
                            mps_table.table_def.c.valid_to_date == None,
                            mps_table.table_def.c.valid_to_date >= date.today()
                        )).values(new_vals)
        if lw_ids is not None:
            stmt = stmt.filter(mps_table.table_def.c.lw_id.in_(lw_ids))
        updated_rows = mps_table._database.execute_write(stmt).rowcount
        rows_updated_for = ('all' if lw_ids is None else ','.join(lw_ids))
        logging.debug(
            f"{mps_table.table_name}: Updated valid_to_date for {updated_rows} rows for {rows_updated_for}."
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
            at = flaskrp_helper.add_asof(at)
            at['data_date'] = price_date
            if res_audit_trail is None:
                res_audit_trail = pd.DataFrame([at])
            else:
                res_audit_trail = pd.concat([res_audit_trail, pd.DataFrame([at])], ignore_index=True)
            # Rename cols to match table definition:
            res_audit_trail = res_audit_trail.rename(columns=lambda x: x+'_bid' if x in ('price', 'yield') else x)
        return flaskrp_helper.save_df_to_table(res_audit_trail, PricingAuditTrailTable())

    def get(self, price_date):
        audit_trail = PricingAuditTrailTable().read(data_date=price_date)
        # Remove "_bid" suffix:
        audit_trail = audit_trail.rename(columns=lambda x: x[:-4] if x[-4:] == '_bid' else x)
        return flaskrp_helper.clean(audit_trail)

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
            for rf in ['column_name','is_hidden']:
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
        res_col_config = flaskrp_helper.add_valid_dates(res_col_config)
        res_col_config = flaskrp_helper.add_asof(res_col_config)
        return flaskrp_helper.save_df_to_table(res_col_config, PricingColumnConfigTable())

    def get(self, user_id):
        column_config = PricingColumnConfigTable().read(user_id=user_id)
        valid_config = flaskrp_helper.valid(column_config)
        return flaskrp_helper.clean(valid_config)

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
        new_vals = {'valid_to_date': date.today() + timedelta(days=-1)}
        new_vals = flaskrp_helper.add_asof(new_vals)
        stmt = update(pcc_table.table_def).\
                        where(pcc_table.table_def.c.user_id == user_id).\
                        where(pcc_table.table_def.c.valid_to_date == None).\
                        values(new_vals)
        updated_rows = pcc_table._database.execute_write(stmt).rowcount
        logging.debug(
            f"{pcc_table.table_name}: Updated valid_to_date for {updated_rows} rows for {user_id}."
        )
        return updated_rows
    
@api.route('/api/pricing/count-by-source')
class PriceCountBySource(Resource):  # TODO_WAVE4: implement
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
        if 'no_cache' in payload:
            res = flaskrp_helper.get_held_sec_price_counts_by_source(price_date, sec_type)
            full_res = {
                'status': 'success',
                'data': res,
                'message': None
            }
            api_cache_res = flaskrp_helper.cache_result(endpointurl=f'{api.base_url}api/pricing/count-by-source'
                , request_type='POST', payload=json.dumps(payload), result_code=200, result_text=json.dumps(full_res)
            )
            return full_res, 200
        else:
            last_cache = flaskrp_helper.get_last_cache(endpointurl=f'{api.base_url}api/pricing/count-by-source'
                , request_type='POST', payload=json.dumps(payload))
            last_update_ts = flaskrp_helper.get_last_update(securities=False, prices=True, positions=True, data_date=price_date)
            if len(last_cache.index):
                last_cache_ts = last_cache['modified_at'].iloc[0]
                logging.info(f'Cache at {last_cache_ts}, update at {last_update_ts}')
                if last_cache_ts > last_update_ts:
                    return jsonify(json.loads(last_cache['result_text'].iloc[0]))
        res = flaskrp_helper.get_held_sec_price_counts_by_source(price_date, sec_type)
        full_res = {
            'status': 'success',
            'data': res,
            'message': None
        }
        api_cache_res = flaskrp_helper.cache_result(endpointurl=f'{api.base_url}api/pricing/count-by-source'
            , request_type='POST', payload=json.dumps(payload), result_code=200, result_text=json.dumps(full_res)
        )
        return full_res, 200

    # def get_OLD(self, price_date):
    #     return "{\"noteyet\": \"implemented\"}", 200
    #     prices = vwPriceTable().read_for_date(data_date=price_date)
    #     price_source_counts = prices.groupby(['source'])['source'].count()
    #     logging.debug(type(price_source_counts))
    #     logging.debug(price_source_counts)
    #     # price_source_counts = price_source_counts.drop(['PXAPX','LWCOMPOSITE'])
    #     return clean(price_source_counts.to_frame().reset_index())

@api.route('/api/zTEST/pricing/count-by-source/<string:price_date>/<string:sec_type>')
class PriceCountBySource(Resource):  # TODO_WAVE4: implement
    def get(self, price_date, sec_type):
        price_source_counts = flaskrp_helper.get_held_sec_price_counts_by_source(price_date, sec_type)
        return price_source_counts

    # def get_OLD(self, price_date):
    #     return "{\"noteyet\": \"implemented\"}", 200
    #     prices = vwPriceTable().read_for_date(data_date=price_date)
    #     price_source_counts = prices.groupby(['source'])['source'].count()
    #     logging.debug(type(price_source_counts))
    #     logging.debug(price_source_counts)
    #     # price_source_counts = price_source_counts.drop(['PXAPX','LWCOMPOSITE'])
    #     return clean(price_source_counts.to_frame().reset_index())

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
        held = vwHeldSecurityTable().read()
        return clean(held)

@api.route('/api/zTEST/portfolio')  # TODO_CLEANUP: remove when not needed
class Portfolio(Resource):
    def get(self):
        portfs = vwPortfolioTable().read()
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
        portf = vwPortfolioTable().read(portfolio_code=portfolio_code)
        return clean(portf)

@api.route('/api/zTEST/position')  # TODO_CLEANUP: remove when not needed
class Position(Resource):
    def get(self):
        posns = vwPositionTable().read()
        return clean(posns)

@api.route('/api/zTEST/price-storedproc')
class PriceByStoredProc(Resource):
    def post(self):
        logging.debug('in post')
        payload = api.payload
        temp_prices = pd.DataFrame(columns=['_SPID','Date','Type','Symbol','PriceValue','SourceID','PriceTypeID','SecurityID','ThruDate'])
        req_missing = []
        px_hist_table = TempPriceHistoryTable()
        with Session(px_hist_table._database.engine) as session:
            logging.debug('in session')
            spid_res = session.execute(text("select @@SPID as spid"))
            for r in spid_res:
                logging.debug(r)
                logging.debug(type(r))
                spid = r[0]
            logging.debug(f"got SPID {spid}")
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
            logging.debug(f"About to insert temp prices... {temp_prices}")
            # px_hist_table.bulk_insert(temp_prices)
            row_cnt = temp_prices.to_sql(name=px_hist_table.table_name
                , con=px_hist_table._database.engine, schema=px_hist_table.schema
                , if_exists='append', index=False)
            logging.debug(f"Done insert temp prices. {row_cnt} rows.")
        # TODO_NEXT: call pAxPriceHistoryPutBulk to inject to APX
        # TODO_NEXT: capture any SQL errors
        # TODO_NEXT: delete from APXFirm.Temp.PriceHistory

@api.route('/api/zTEST/pricing/held-security-price')
class HeldSecurityWithPrices(Resource):
    def get(self):
        curr_bday, prev_bday = '2023-01-04', '2023-01-03'  # get_current_bday(date.today()), get_previous_bday(date.today())
        # logging.debug(request.json)
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
        # logging.debug(curr_bday, prev_bday)
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
        # logging.debug(curr_bday, prev_bday)
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

    def add_arguments(self, parser):
        parser.add_argument(
            '-ps', '--sqlalchemy_pool_size', type=int,
            help=(
                'SQLAlchemy pool size - optionally override default.'
            )
        )
        parser.add_argument(
            '-pt', '--sqlalchemy_pool_timeout', type=int,
            help=(
                'SQLAlchemy pool timeout - optionally override default.'
            )
        )

    def handle(self, *args, **kwargs):
        """
        Run local flask server:
        """
        # flaskrp_helper.create_app()
        # app = Flask(__name__)
        # app.app_context()
        logging.info(f'Starting local flask server on port 5000...')
        app.run(host='0.0.0.0', port=5000, debug=True)
        return EXIT_SUCCESS
    


if __name__ == '__main__':
    sys.exit(Command().run_from_argv())



