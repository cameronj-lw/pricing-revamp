
# TODO_CLEANUP: clean up imports, remove ones not used
import aiohttp
import asyncio
import json
import logging
import numpy as np
import os
import pandas as pd
import paramiko
# import requests_async as requests
import requests
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
from sqlalchemy import exc, text, update
from sqlalchemy.orm import Session

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
from lw.db.apxdb.vprice import vPriceTable
from lw.db.coredb.pricingapicache import PricingApiCacheTable
from lw.db.coredb.pricingauditreason import PricingAuditReasonTable
from lw.db.coredb.pricingaudittrail import PricingAuditTrailTable
from lw.db.coredb.pricingcolumnconfig import PricingColumnConfigTable
from lw.db.coredb.pricingmanualpricingsecurity import PricingManualPricingSecurityTable
from lw.db.coredb.pricingnotificationsubscription import PricingNotificationSubscriptionTable
from lw.db.coredb.vw_apx_appraisal import vwApxAppraisalTable
from lw.db.coredb.vw_price import vwPriceTable
from lw.db.coredb.vw_held_security import vwHeldSecurityTable
from lw.db.coredb.vw_portfolio import vwPortfolioTable
from lw.db.coredb.vw_position import vwPositionTable
from lw.db.coredb.vw_security import vwSecurityTable
from lw.db.coredb.vw_transaction import vwTransactionTable
from lw.db.mgmtdb.monitor import MonitorTable
from lw.db.lwdb.apx_appraisal import ApxAppraisalTable
# from lw.util.dataframe import NaN_NaT_to_none
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
        'security_type': 'Canadian Bonds',
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
        'security_type': 'American Bonds',
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
        'security_type': 'All Equities (except Latin America)',
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
        'security_type': 'Latin America Equities',
        'normal_eta': datetime.today().replace(hour=14, minute=00)
    },
    'BLOOMBERG': { 
        'BB_SNAP': [
            ('BB-SNAP', ['BOND_PRICE'])
        ],
        'LOAD2PRICING': [
            ('LOADPRICE_FI', ['BOND_PRICE'])
        ],
        'security_type': 'All Instruments',
        'normal_eta': datetime.today().replace(hour=14, minute=30)
    },
}

# TODO_PROD: consider changing references to APX to PMS
APX_SEC_TYPES = {
    # TODO_TO: confirm if desired
    'bond': ['cb', 'cf', 'cm', 'cv', 'fr', 'lb', 'ln', 'sf', 'tb', 'vm'],
    'equity': ['cc', 'ce', 'cg', 'ch', 'ci', 'cj', 'ck', 'cn', 'cr', 'cs', 'ct', 'cu', 'ps']
}

APX_PRICE_TYPES = {
    'price': {
        'price_type_id': 1,  # Standard Prices
        'imex_file_suffix': ''
    },
    'yield': {
        'price_type_id': 2,  # LW Bond Yield
        'imex_file_suffix': '_LWBondYield'
    },
    'duration': {
        'price_type_id': 3,  # LW Bond Duration
        'imex_file_suffix': '_LWBondDur'
    }
}

APX_PX_SOURCES = {
    # TODO: create new sources for manual/override; add to below
    # 'LWDB source': APX source ID   # APX source name
    'DEFAULT'               : 3000,  # LW Not Classified
    'FTSE'                  : 3006,  # LW FTSE TMX
    'FTSETMX_PX'            : 3006,  # LW FTSE TMX
    'BLOOMBERG'             : 3004,  # LW Bloomberg
    'MARKIT'                : 3005,  # LW Markit
    'MARKIT_LOAN'           : 3011,  # LW Markit - Loan
    'FUNDRUN'               : 3019,  # LW FundRun Equity
    'FIDESK_MANUALPRICE'    : 3007,  # LW FI Desk - Manual Price
    'FIDESK_MISSINGPRICE'   : 3008,  # LW FI Desk - Missing Price
    'MISSING'               : 3008,  # LW FI Desk - Missing Price
    'MANUAL'                : 3031,  # LW Security Pricing - Manual
    'OVERRIDE'              : 3032,  # LW Security Pricing - Override
}

PRICE_HIERARCHY = RELEVANT_PRICES = [
    # highest to lowest in the hierarchy, i.e. MANUAL is highest
    # TODO_IDEA: should add more pricing sources to "hierarchy"? 
    # Only adding ones relevant to pricing revamp for now...
    'OVERRIDE','MANUAL','FUNDRUN','FTSE','MARKIT','BLOOMBERG','RBC'
]

LOG_TIMESTAMPS = False

IMEX_BASE_URL = 'http://TEST-WS003:5000/api'


# Classes

class SecurityNotFoundException(Exception):
    def __init__(self, missing_col_name, missing_col_value):
        self.missing_col_name = missing_col_name
        self.missing_col_value = missing_col_value



def NaN_NaT_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN and NaT values in the provided DataFrame with None.
    Is affected by this bug though, it seems: https://github.com/pandas-dev/pandas/issues/44485
    """
    # df = df.where(pd.notnull(df), None)
    df = df.replace({np.nan: None, pd.NaT: None})
    # df = df.fillna(None)
    return df

def clean(df: pd.DataFrame) -> str:
    """
    Clean and format a DataFrame and returns the results as a JSON string.
    
    Args:
    - df (DataFrame): The DataFrame to clean and format.
    
    Returns:
    - str: A JSON string containing the cleaned and formatted DataFrame, along with a status and message.
    """
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
    return res_str  # res_str

def trim_px_sources(raw_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Trim the sources in a DataFrame of prices based on a set of predetermined rules and returns the result.
    
    Args:
    - raw_prices (DataFrame): The DataFrame of raw prices to process.
    
    Returns:
    - DataFrame: The processed DataFrame with sources trimmed according to the rules.
    """
    prices = raw_prices
    prices['source'] = prices['source'].apply(lambda x: 'BLOOMBERG' if (x[:3] == 'BB_' and '_DERIVED' not in x) else x)
    prices['source'] = prices['source'].apply(lambda x: 'FTSE' if x == 'FTSETMX_PX' else x)
    prices['source'] = prices['source'].apply(lambda x: 'MARKIT' if x == 'MARKIT_LOAN_CLEANPRICE' else x)
    prices['source'] = prices['source'].apply(lambda x: 'FUNDRUN' if x == 'FUNDRUN_EQUITY' else x)
    prices['source'] = prices['source'].apply(lambda x: 'OVERRIDE' if x in ('FIDESK_MANUALPRICE', 'LW_OVERRIDE') else x)
    prices['source'] = prices['source'].apply(lambda x: 'MANUAL' if x in ('FIDESK_MISSINGPRICE', 'LW_MANUAL') else x)
    prices = prices[prices['source'].isin(RELEVANT_PRICES)]
    prices = prices.reset_index(drop=True)
    return prices

def get_chosen_price(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the preferred price for a set of prices based on the predetermined hierarchy.
    
    Args:
    - prices (DataFrame): The DataFrame of prices to process.
    
    Returns:
    - DataFrame: The preferred price as a DataFrame with a single row.
    """
    chosen_loc = chosen_px = None
    for i, px in prices.iterrows():
        src = px['source']
        logging.debug(f"{src} for {px['lw_id']}")
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
    chosen_px = chosen_px.reset_index(drop=True)
    return chosen_px

def get_manual_pricing_securities(data_date=date.today()):
    """
    Gets securities which we want to always price manually,
        even when we have external prices available.
        
    Args:
    - data_date: Include securities which should be manually priced as of this date.
    
    Returns:
    - DataFrame: Securities which we always want to price manually.
    """
    secs = PricingManualPricingSecurityTable().read()
    valid_secs = valid(secs, data_date)
    valid_secs = valid_secs.reset_index(drop=True)
    return valid_secs

def is_manual(prices):
    """
    Determines if the security with the prices provided in the DataFrame was manually priced.
    A manual price is any price manually inputted by a user (as opposed to from an external source).
    
    Args:
    - prices: A DataFrame of prices for a single security on a single date.
    
    Returns:
    - A boolean value indicating whether the security with the prices provided in the DataFrame 
        was manually priced.
    """
    manual_prices = prices[prices['source'] == 'MANUAL']
    if len(manual_prices.index) > 0:
        return True
    else:
        return False

def is_missing(prices):
    """
    Determines if the security with the prices provided in the DataFrame is a security missing price.
    A security missing price is one for which we have not yet recieved an external price.
    
    Args:
    - prices: A DataFrame of prices for a single security on a single date.
    
    Returns:
    - A boolean value indicating whether the security with the prices provided in the DataFrame
        is a security missing price.
    """
    # relevant_external_sources = [x for x in RELEVANT_PRICES if x not in ['MANUAL','MISSING']]
    if len(prices[prices['source'].isin(RELEVANT_PRICES)].index):
        return False
    else:
        return True


def is_override(prices):
    """
    Determines if the security with the prices provided in the DataFrame has an overridden price.
    An overridden price is a manual price (see above) which occurred despite having an external price.
    
    Args:
    - prices: A DataFrame of prices for a single security on a single date.
    
    Returns:
    - A boolean value indicating whether the security with the prices provided in the DataFrame
        has an overridden price.
    """
    return (~is_missing(prices) and is_manual(prices))


def first2chars(s):
    """
    Returns the first two characters of the provided string.
    """
    if s is None:
        return s
    else:
        return s[:2]


def add_valid_dates(payload):
    """
    Adds valid_from and valid_to fields to a dictionary or DataFrame.
    
    Args:
    - payload: A dictionary or DataFrame.
    
    Returns:
    - The input dictionary or DataFrame with valid_from and valid_to fields added.
    """
    payload['valid_from_date'] = date.today()
    payload['valid_to_date'] = None
    return payload


def add_asof(payload):
    """
    Adds modified_at and modified_by fields to a dictionary or DataFrame.
    
    Args:
    - payload: A dictionary or DataFrame.
    
    Returns:
    - The input dictionary or DataFrame with modified_at and modified_by fields added.
    """
    if 'modified_by' not in payload:
        if 'asofuser' in payload:
            payload['modified_by'] = payload['asofuser']
        else:
            payload['modified_by'] = f"{os.getlogin()}_{socket.gethostname()}"
    payload['modified_at'] = format_time(datetime.now())
    return payload

def get_securities_by_sec_type(secs, sec_type=None, sec_type_col='pms_sec_type', sec_type_func=first2chars):
    """
    Filters securities DataFrame by security type.

    Args:
    - secs (DataFrame): DataFrame containing securities.
    - sec_type (str or list, optional): Security type(s) to filter by. If not provided, do not filter.
    - sec_type_col (str, optional): Column name in 'secs' DataFrame containing security type information.
    - sec_type_func (function, optional): Function used to transform security types for filtering.

    Returns:
    - DataFrame: Filtered 'secs' DataFrame.
    """
    if sec_type is None:
        return secs
    if isinstance(sec_type, str):  # convert to list
        if sec_type in APX_SEC_TYPES:
            sec_type = APX_SEC_TYPES[sec_type]  # should be a list now
        else:
            sec_type = [sec_type]
    if isinstance(sec_type, list):
        if sec_type_func is None:
            result = secs[secs[sec_type_col].isin(sec_type)]
            return result.reset_index(drop=True)
        else:
            processed_sec_types = secs[sec_type_col].apply(sec_type_func)
            result = secs[processed_sec_types.isin(sec_type)]
            return result.reset_index(drop=True)

def rename_cols_pms2apx(df):
    """
    Renames df columns with "pms_" prefix by replacing it with "apx_" prefix.
    
    Args:
    - df (DataFrame): df with columns to be renamed.
    
    Returns:
    - DataFrame: df with renamed columns.
    """
    res_df = df.copy()
    res_df = res_df.rename(columns=lambda x: 'apx_' + x[4:] if x[0:4] == 'pms_' else x)
    return res_df

def get_held_sec_price_counts_by_source(curr_bday, sec_type=None):
    """
    For a given date, retrieves the breakdown of how many held securities
        have their "chosen price" from each source, optionally filtering by security type.

    Args:
    - curr_bday (str): Date to retrieve held securities for (in YYYYMMDD format).
    - sec_type (str or list, optional): Security type(s) to filter by.

    Returns:
    - dict: Counts by source. Keys are sources, values are counts.
    """
    source_counts = {}
    held = get_held_securities(curr_bday, sec_type)
    prices = vwPriceTable().read(data_date=curr_bday)
    prices = trim_px_sources(prices)
    for i, row in held.iterrows():
        lw_id = row['lw_id']
        sec_prices = prices.loc[prices['lw_id'] == lw_id]
        if is_missing(sec_prices):
            chosen_source = 'MISSING'
            logging.debug(f'missing for {lw_id}')
        else:
            sec_prices = NaN_NaT_to_none(sec_prices)
            chosen_price = get_chosen_price(sec_prices)
            chosen_source = chosen_price['source'].iloc[0]
        if chosen_source in source_counts:
            source_counts[chosen_source] += 1
        else:
            source_counts[chosen_source] = 1
    return source_counts

def get_held_securities(curr_bday, sec_type=None):
    """
    Retrieves held securities for a given date, optionally filtering by security type.

    Args:
    - curr_bday (str): Date to retrieve held securities for (in YYYYMMDD format).
    - sec_type (str or list, optional): Security type(s) to filter by.

    Returns:
    - DataFrame: Held securities DataFrame.
    """
    curr_bday_appr = vwApxAppraisalTable().read_for_date(curr_bday)
    if len(curr_bday_appr.index):  # if there are Appraisal results for curr day, use it
        secs = vwSecurityTable().read()
        secs = get_securities_by_sec_type(secs, sec_type)
        held_secs = list(curr_bday_appr['ProprietarySymbol'])
        held = secs[secs['lw_id'].isin(held_secs)]
    else:  # if not, use live positions
        held = vwHeldSecurityTable().read()
        held = get_securities_by_sec_type(held, sec_type)
    held = rename_cols_pms2apx(held)
    held = held.reset_index(drop=True)
    return held

def add_prices(held, i, lw_id, curr_prices, prev_prices):
    """
    Add price information to a held security row.

    Args:
    - held (DataFrame): DataFrame containing held securities.
    - i (int): Index of the row to add prices to.
    - lw_id (str): lw_id of the security to add prices for.
    - curr_prices (DataFrame): DataFrame containing current bday prices.
    - prev_prices (DataFrame): DataFrame containing previous bday prices.

    Returns:
    - DataFrame: Modified 'held' DataFrame containing prices.
    - DataFrame: Combined provided prices for current bday and previous bday.
    """
    sec_curr_prices = curr_prices.loc[curr_prices['lw_id'] == lw_id]
    sec_prev_prices = prev_prices.loc[prev_prices['lw_id'] == lw_id]
    # add prices:
    prices = pd.concat([sec_curr_prices, sec_prev_prices], ignore_index=True)
    # Initialize
    for col in ['curr_bday_prices', 'prev_bday_price', 'chosen_price']:
        if col not in held:
            held[col] = None
            held[col] = held[col].astype(object)
        held.at[i, col] = None
    if len(sec_prev_prices):
        sec_prev_prices = NaN_NaT_to_none(sec_prev_prices)
        held.at[i, 'prev_bday_price'] = sec_prev_prices.to_dict('records')
        if isinstance(held.at[i, 'prev_bday_price'], list):  # we only want the first item in the list
            if len(held.at[i, 'prev_bday_price']):
                held.at[i, 'prev_bday_price'] = held.at[i, 'prev_bday_price'][0]
    if len(sec_curr_prices):
        sec_curr_prices = NaN_NaT_to_none(sec_curr_prices)
        # logging.info(sec_curr_prices)
        # Add cols if not in df
        held.at[i, 'curr_bday_prices'] = sec_curr_prices.to_dict('records')
        # if sec_curr_prices is not None:
        #     held.at[i, 'curr_bday_prices'] = sec_curr_prices.to_dict('records')
        # if isinstance(held.at[i, 'prev_bday_price'], list):  # we only want the first item in the list
        #     held.at[i, 'prev_bday_price'] = held.at[i, 'prev_bday_price'][0]
        chosen_px = get_chosen_price(sec_curr_prices)
        if chosen_px is not None:
            held.at[i, 'chosen_price'] = chosen_px.to_dict('records')
            if isinstance(held.at[i, 'chosen_price'], list):  # we only want the first item in the list
                held.at[i, 'chosen_price'] = held.at[i, 'chosen_price'][0]
    return held

def add_audit_trail(held, i, lw_id, curr_audit_trail):
    """
    Add audit trail information to a held security row.

    Args:
    - held (DataFrame): DataFrame containing held securities.
    - i (int): Index of the row to add audit trail information to.
    - lw_id (str): lw_id of the security to add audit trail information for.
    - curr_audit_trail (DataFrame): DataFrame containing audit trail information.

    Returns:
    - DataFrame: Modified 'held' DataFrame containing audit trail.
    """
    # Further below, we will need to insert a "list" value into the 'audit_trail' column.
    # It seems in order to do so, its type must be "object": 
    # https://stackoverflow.com/a/47548471
    if 'audit_trail' not in held.columns:
        held['audit_trail'] = None
    if held.dtypes['audit_trail'] != object:
        held['audit_trail'] = held['audit_trail'].astype(object)
    sec_curr_audit = curr_audit_trail.loc[curr_audit_trail['lw_id'] == lw_id]
    if len(sec_curr_audit):
        sec_curr_audit = NaN_NaT_to_none(sec_curr_audit)
        held.at[i, 'audit_trail'] = sec_curr_audit.to_dict('records')
        if not isinstance(held.at[i, 'audit_trail'], list):  
            # If only one row, it will be a dict, but we want it as a list:
            held.at[i, 'audit_trail'] = [held.at[i, 'audit_trail']]
    return held

# TODO_CLEANUP: remove when not needed
# def should_exclude_sec_v1(lw_id, prices, price_type, manually_priced_secs):
#     """
#     Determine whether a security should be excluded based on pricing information.

#     Args:
#     - lw_id (str): lw_id of the security to check.
#     - prices (DataFrame): DataFrame containing prices for the given security on a single date.
#     - price_type (str or None): Type of pricing information to check for (e.g. 'manual', 'missing', 'override').
#     - manually_priced_secs (DataFrame): DataFrame containing securities which should always be manually priced,
#         even when we have an external price.

#     Returns:
#     - bool: Whether the security should be excluded.
#     """
#     if price_type is None:
#         return False
#     if price_type == 'manual':  
#         # securities which have been priced by a user
#         if not is_manual(prices):
#             return True
#     elif price_type == 'missing':  
#         # securities which do not have an external price,
#         # OR are on the "sticky bond" list
#         # TODO_TO: confirm whether desired to include the "sticky bond list" here
#         if not is_missing(prices):
#             if lw_id not in list(manually_priced_secs['lw_id']):
#                 return True
#     elif price_type == 'override':  
#         # securities which have an external price, but were also priced by a user
#         if not is_override(prices):
#             return True
#     # If we made it here, the sec should not be excluded
#     return False

def should_exclude_sec(chosen_price_source, source=None):
    """
    Determine whether a security should be excluded based on pricing information.

    Args:
    - chosen_price_source (dict): dict containing the chosen price for the given security on a single date.
    - source (str or None): Type of price to check for.
        Meaningful options are: a vendor source, missing, manual, override.
        Assumed to already be uppercase.

    Returns:
    - bool: Whether the security should be excluded. If source is None, returns False.
    """
    if source is None or source == 'FAVOURITE':
        return False
    elif source == 'MISSING':  
        # securities which do not have any price (yet) for today.
        if chosen_price_source is not None:
            return True
        # if not is_missing(chosen_price_source):
        #     return True
    elif chosen_price_source is None:
        return True  # No chosen price -> should be excluded for everything besides "missing".
    elif chosen_price_source != source:
        return True  # Different source than requested source
    # If we made it here, the sec should not be excluded
    return False


# TODO_CLEANUP: delete when not needed
# def get_held_security_prices_v1(curr_bday, prev_bday, sec_type=None, price_type=None):
#     """
#     Retrieve held securities with price and audit trail information for the provided
#     current and previous business days, optionally filtered by security type and/or price type.

#     Args:
#     - curr_bday (str): Date to retrieve held securities for (in YYYYMMDD format).
#     - prev_bday (str): Previous date to retrieve price for (in YYYYMMDD format).
#     - sec_type (str or list, optional): Security type(s) to filter by.
#     - source (str, or None, optional): Only retrieve securities with prices corresponding to this source.
#         Meaningful options include vendor sources, missing, manual, override, and favourite.

#     Returns:
#     - JSON string of held securities with price and audit trail information, along with status and message if applicable.
#     """
#     if LOG_TIMESTAMPS:
#         logging.info(f"Start get_held_security_prices {datetime.now()}")
#     held = get_held_securities(curr_bday, sec_type)
#     if LOG_TIMESTAMPS:
#         logging.info(f"Got held securities {datetime.now()}")
#     held['prices'] = None
#     curr_prices = vwPriceTable().read(data_date=curr_bday)
#     curr_prices = trim_px_sources(curr_prices)
#     # want to include only 1 prev day px: the one from APX
#     prev_prices = vwPriceTable().read(data_date=prev_bday, source='PXAPX')
#     prev_prices['source'] = 'APX'
#     curr_audit_trail = PricingAuditTrailTable().read(data_date=curr_bday)
#     manually_priced_secs = get_manual_pricing_securities()  # in case we need to add these below
#     if LOG_TIMESTAMPS:
#         logging.info(f"Got prices, audit trail, and sticky bonds {datetime.now()}")
#     for i, row in held.iterrows():
#         lw_id = row['lw_id']
#         held, prices = add_prices(held, i, lw_id, curr_prices, prev_prices)
#         # remove this security according to type, if requested
#         if should_exclude_sec_v1(lw_id, prices, price_type, manually_priced_secs):
#             held = held.drop(i)
#             continue
#         # add audit trail:
#         held = add_audit_trail(held, i, lw_id, curr_audit_trail)
#     if LOG_TIMESTAMPS:
#         logging.info(f"Finish get_held_security_prices {datetime.now()}")
#     # TODO_WAVE2: make good thru date based on sec type / applicable holiday calendar
#     held['good_thru_date'] = get_next_bday(curr_bday)
#     return clean(held)


def get_held_security_prices(curr_bday, prev_bday, sec_type=None, source=None):
    """
    Retrieve held securities with price and audit trail information for the provided
    current and previous business days, optionally filtered by security type and/or price type.

    Args:
    - curr_bday (str): Date to retrieve held securities for (in YYYYMMDD format).
    - prev_bday (str): Previous date to retrieve price for (in YYYYMMDD format).
    - sec_type (str or list, optional): Security type(s) to filter by.
    - source (str, or None, optional): Only retrieve securities with prices corresponding to this source.
        Meaningful options include vendor sources, missing, manual, override, and favourite.
        Assumed to already be uppercase.

    Returns:
    - JSON string of held securities with price and audit trail information, along with status and message if applicable.
    """
    if LOG_TIMESTAMPS:
        logging.info(f"Start get_held_security_prices {datetime.now()}")
    held = get_held_securities(curr_bday, sec_type)
    if LOG_TIMESTAMPS:
        logging.info(f"Got held securities {datetime.now()}")
    held['prices'] = None
    curr_prices = vwPriceTable().read(data_date=curr_bday)
    curr_prices = trim_px_sources(curr_prices)
    # want to include only 1 prev day px: the one from APX
    prev_prices = vwPriceTable().read(data_date=prev_bday, source='PXAPX')
    prev_prices['source'] = 'APX'
    curr_audit_trail = PricingAuditTrailTable().read(data_date=curr_bday)
    if source == 'FAVOURITE':
        manually_priced_secs = get_manual_pricing_securities()
        logging.debug(manually_priced_secs)
    if LOG_TIMESTAMPS:
        logging.info(f"Got prices, audit trail, and sticky bonds {datetime.now()}")
    for i, row in held.iterrows():
        lw_id = row['lw_id']
        if source == 'FAVOURITE':  # sticky bonds
            if lw_id not in manually_priced_secs['lw_id'].tolist():
                held = held.drop(i)
                continue
            else:
                # logging.info(f'keeping sticky bond {lw_id}')
                pass
        held = add_prices(held, i, lw_id, curr_prices, prev_prices)
        # remove this security according to type, if requested
        chosen_price = held.at[i, 'chosen_price']
        chosen_price_source = None
        if chosen_price is not None:
            if 'source' in chosen_price:
                chosen_price_source = chosen_price['source']
        if should_exclude_sec(chosen_price_source, source):
            held = held.drop(i)
            continue
        # add audit trail:
        held = add_audit_trail(held, i, lw_id, curr_audit_trail)
    if LOG_TIMESTAMPS:
        logging.info(f"Finish get_held_security_prices {datetime.now()}")
    # TODO_WAVE2: make good thru date based on sec type / applicable holiday calendar
    held['good_thru_date'] = get_next_bday(curr_bday)
    return clean(held)


def get_counts_by_source(curr_bday, sec_type=None):
    """
    Retrieve how many held securities are currently priced 

    Args:
    - curr_bday (str): Date to retrieve held securities for (in YYYYMMDD format).
    - sec_type (str or list, optional): Security type(s) to filter by.
    
    Returns:
    - JSON string of held securities with price and audit trail information, along with status and message if applicable.
    """
    if LOG_TIMESTAMPS:
        logging.info(f"Start get_held_security_prices {datetime.now()}")
    held = get_held_securities(curr_bday, sec_type)
    if LOG_TIMESTAMPS:
        logging.info(f"Got held securities {datetime.now()}")
    held['prices'] = None
    curr_prices = vwPriceTable().read(data_date=curr_bday)
    curr_prices = trim_px_sources(curr_prices)
    # want to include only 1 prev day px: the one from APX
    prev_prices = vwPriceTable().read(data_date=prev_bday, source='PXAPX')
    prev_prices['source'] = 'APX'
    curr_audit_trail = PricingAuditTrailTable().read(data_date=curr_bday)
    if source == 'favourite':
        manually_priced_secs = get_manual_pricing_securities()
    if LOG_TIMESTAMPS:
        logging.info(f"Got prices, audit trail, and sticky bonds {datetime.now()}")
    for i, row in held.iterrows():
        lw_id = row['lw_id']
        if source == 'favourite':  # sticky bonds
            if lw_id not in manually_priced_secs['lw_id']:
                held = held.drop(i)
                continue
        held = add_prices(held, i, lw_id, curr_prices, prev_prices)
        # remove this security according to type, if requested
        chosen_price = held.at[i, 'chosen_price']
        chosen_price_source = None
        if chosen_price is not None:
            if 'source' in chosen_price:
                chosen_price_source = chosen_price['source']
        if should_exclude_sec(chosen_price_source, source):
            held = held.drop(i)
            continue
        # add audit trail:
        held = add_audit_trail(held, i, lw_id, curr_audit_trail)
    if LOG_TIMESTAMPS:
        logging.info(f"Finish get_held_security_prices {datetime.now()}")
    # TODO_WAVE2: make good thru date based on sec type / applicable holiday calendar
    held['good_thru_date'] = get_next_bday(curr_bday)
    return clean(held)

def get_pricing_attachment_folder(data_date):
    """
    Retrieve the path to the folder containing pricing attachments for a given date.

    Args:
    - data_date (str): Date to retrieve attachment folder for (in YYYYMMDD format).

    Returns:
    - str: The path to the folder containing pricing attachments for the given date.
    """
    base_path = os.path.join(config.DATA_DIR, 'lw', 'pricing_audit')
    full_path = prepare_dated_file_path(folder_name=base_path, date=datetime.strptime(data_date, '%Y%m%d'), file_name='', rotate=False)
    return full_path

def save_binary_files(data_date, files):
    """
    Save binary files to the folder corresponding to data_date.

    Args:
    - data_date (str): Date for folder where the files will be saved.
    - files (list): A list of dictionaries where each contains the filename and its binary contents.

    Returns:
    - Standard result (status/data/message) and HTTP return code
    """

    # Create the folder if it doesn't exist
    folder_path = get_pricing_attachment_folder(data_date)
    if not os.path.exists(folder_path):
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
    """
    Delete all files in a given directory.

    Args:
    - path (str): The path to the directory whose contents should be deleted.

    Returns:
    - dict: Standard response (status/data/message).
    - int: HTTP return code which would be appropriate given the result of the operation.
    """
    try:
        files = os.listdir(path)
        for f in files:
            os.remove(os.path.join(path, f))
        return {
            'status': 'success',
            'data': None,
            'message': f'Deleted {len(files)} files from {path}'
        }, 200
    except Exception as e:
        msg = f'Failed to delete files at {path}: {e}'
        logging.exception(msg)
        return {
            'status': 'error',
            'data': None,
            'message': msg
        }, 500

def valid(df, data_date=date.today()):  
    """
    Filter a pandas dataframe to include only rows whose 'valid_from' and 'valid_to' date ranges
    include the specified date, or are null.

    Args:
    - df (pandas.DataFrame): The dataframe to be filtered.
    - data_date (datetime.date): The date to be used for the filtering.

    Returns:
    - pandas.DataFrame: A new dataframe containing only rows whose 'valid_from' and 'valid_to' date ranges
                        include the specified date, or are null.
    """
    # TODO_REFACTOR: does this belong in a library?
    valid_from = [
        pd.isnull(df['valid_from_date']), df['valid_from_date'] <= np.datetime64(data_date)
    ]
    valid_to = [
        pd.isnull(df['valid_to_date']), df['valid_to_date'] >= np.datetime64(data_date)
    ]
    res = df.loc[valid_from[0] | valid_from[1]].loc[valid_to[0] | valid_to[1]]
    return res

def save_df_to_table(df, table):
    """
    Save a pandas dataframe to a database table using the specified table object's bulk_insert method.

    Args:
    - df (pandas.DataFrame): The dataframe to be saved.
    - table (subclass of table.BaseTable or table.ScenarioTable): The database table object to save the dataframe to.

    Returns:
    - dict: Standard response (status/data/message).
    - int: HTTP return code which would be appropriate given the result of the operation.
    """
    try:
        res = table.bulk_insert(df)
        if res.rowcount == len(df.index):
            return {
                'status': 'success',
                'data': None,
                # 'message': f"Saved {res.rowcount} rows to " \
                # f"{config.CONN_INFO[table.database_key][config.ENV]['hostname']}." \
                # f"{config.CONN_INFO[table.database_key][config.ENV]['database']}." \
                # f"{table.schema}.{table.table_name}"
                'message': f"Successfully saved {res.rowcount} row{'' if res.rowcount == 1 else 's'}."
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

def is_error(pf, data_date):
    """
    Determine if a given pricing feed is in an error state for a given date.

    Args:
    - pf (str): The name of the pricing feed to check.
    - data_date (datetime.date): The date to check for.

    Returns:
    - tuple: A tuple containing a boolean indicating whether or not an error state was detected, and a datetime
            indicating the maximum timestamp of the error (or the current datetime if no error was detected).
    """
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
    """
    Determine if a given pricing feed has been successfully priced for a given date.

    Args:
    - pf (str): The name of the pricing feed to check.
    - data_date (datetime.date): The date to check for.

    Returns:
    - tuple: A tuple containing a boolean indicating whether or not successful pricing was detected, and a datetime
            indicating the maximum timestamp of the pricing completion (or the current datetime if no successful pricing
            was detected).
    """
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
    """
    Determine if a given pricing feed is currently in progress for a given date.

    Args:
    - pf (str): The name of the pricing feed to check.
    - data_date (datetime.date): The date to check for.

    Returns:
    - tuple: A tuple containing a boolean indicating whether or not pricing is currently in progress, and a datetime
            indicating the maximum timestamp of the in-progress state (or the current datetime if no in-progress state
            was detected).
    """
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

def get_feed_security_type(pf):
    """
    Get security type(s) for a pricing feed.

    Args:
    - pf (str): The name of the pricing feed to check.

    Returns:
    - str: The security type(s) for the pricing feed. Or None if a security type is not found.
    """
    if pf in PRICING_FEEDS:
        if 'security_type' in PRICING_FEEDS[pf]:
            return PRICING_FEEDS[pf]['security_type']
    return None

def get_normal_eta(pf):
    """
    Get normal ETA for a pricing feed.

    Args:
    - pf (str): The name of the pricing feed to check.

    Returns:
    - datetime: The normal ETA for the pricing feed. Or None if a normal ETA is not found.
    """
    if pf in PRICING_FEEDS:
        if 'normal_eta' in PRICING_FEEDS[pf]:
            return PRICING_FEEDS[pf]['normal_eta']
    return None

def is_delayed(pf, data_date):
    """
    Determine if a given pricing feed is delayed for a given date.

    Args:
    - pf (str): The name of the pricing feed to check.
    - data_date (datetime.date): The date to check for.

    Returns:
    - tuple: A tuple containing a boolean indicating whether or not a delay was detected, and a datetime
            indicating the current datetime (since this function only checks if a delay is present, it is always
            called at the time of the delay, so the current datetime is used to indicate the time of the delay).
    """ 
    if is_priced(pf, data_date)[0]:
        return False, datetime.now()
    if 'normal_eta' in PRICING_FEEDS[pf]:
        if get_normal_eta(pf) < datetime.now():
            return True, datetime.now()
    return False, datetime.now()

def is_pending(pf, data_date):
    """
    Determine if a given pricing feed is pending for a given date.

    Args:
    - pf (str): The name of the pricing feed to check.
    - data_date (datetime.date): The data date to check for.

    Returns:
    - tuple: A tuple with a boolean value indicating whether the pricing feed is pending, and a datetime 
            representing the timestamp of when the FTP Upload completed.
    """
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
    """
    Retrieve the pricing feed status for the given price date.

    Args:
    - price_date (optional): The price date to check for. Defaults to today's date.

    Returns:
    - Tuple: A tuple with a standard response dictionary (status/data/message) containing 
            pricing feed statuses, and an appropriate HTTP status code given the result of the operation.
    """
    statuses = {}
    pd = datetime.strptime(price_date, '%Y%m%d') if isinstance(price_date, str) else price_date
    if isinstance(pd, datetime):
        pd = pd.date()
    if pd >= date(year=2011, month=1, day=1):
        pd = get_current_bday(pd)
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
            delayed = is_delayed(pf, pd)
            if delayed[0]:
                statuses[pf] = {'status': 'DELAYED', 'asofdate': delayed[1].isoformat()}
                continue
            in_progress = is_in_progress(pf, pd)
            if in_progress[0]:
                statuses[pf] = {'status': 'IN PROGRESS', 'asofdate': in_progress[1].isoformat()}
                continue
            pending = is_pending(pf, pd)
            if pending[0]:
                statuses[pf] = {'status': 'PENDING', 'asofdate': pending[1].isoformat()}
                continue
            # If we reached this point, there is not status... but we still want all feeds included in results
            prev_bday_priced = is_priced(pf, get_previous_bday(pd))
            if prev_bday_priced[0]:
                # provide the prev bday "priced" time if there is no status yet:
                statuses[pf] = {'status': '-', 'asofdate': prev_bday_priced[1].isoformat()}
            else:
                statuses[pf] = {'status': '-', 'asofdate': datetime.now().isoformat()}
        for s in statuses:            
            eta = get_normal_eta(s).replace(year=pd.year, month=pd.month, day=pd.day)
            statuses[s]['normal_eta'] = eta.isoformat()
            sec_type = get_feed_security_type(s)
            statuses[s]['security_type'] = sec_type
        if len(statuses):
            return {
                'status': 'success',
                'data': statuses,
                'message': None
            }, 200
        else:
            return {
                'status': 'warning',
                'data': None,
                'message': 'No data was found.'
            }, 200
    except exc.SQLAlchemyError as e:
        logging.exception(e)
        return {
            'status': 'error',
            'data': None,
            'message': f'SQLAlchemy error: {e}'
        }, 500

def get_apx_SourceID(source: str) -> int:
    """
    Retrieve the APX Source ID for the given source.

    Args:
    - source (str): The source to get the APX Source ID for.

    Returns:
    - int: The APX Source ID for the given source.
    """
    if source in APX_PX_SOURCES:
        return APX_PX_SOURCES[source]
    return APX_PX_SOURCES['DEFAULT']

def price_file_name(from_date: str, file_suffix: str = '') -> str:
    """
    Generate the name of the price file, as required for IMEX.

    Args:
    - from_date (str): The date the prices were generated.
    - file_suffix (str, optional): The price type. Defaults to an empty string. This is
        used to build the desired file name for other price types (e.g. yield, duration).

    Returns:
    - str: The name of the price file.
    """
    name = datetime.strptime(from_date, "%Y-%m-%d").strftime("%m%d%y")
    return f"{name}{file_suffix}.pri"

def is_different_from_apx_price(px, pt, secs, apx_pxs):
    """
    Determine if the proposed price is different from what is currently in APX.

    Args:
    - px (Dict): A dictionary representing a price/yield/duration for a single security on a single date,
                    to be compared to current APX.
    - pt (str): The price type (price/yield/duration).
    - secs (DataFrame): A dataframe representing all currently valid securities from Secmaster.
    - apx_pxs (DataFrame): A dataframe representing the APX prices for current day.
    """
    apx_security_ids = secs.loc[secs['pms_symbol'] == px['apx_symbol'], 'pms_security_id']
    if not len(apx_security_ids.index):
        logging.exception(f"Could not find security with apx_symbol {px['apx_symbol']}!")
        raise SecurityNotFoundException('apx_symbol', px['apx_symbol'])
    apx_security_id = apx_security_ids.iloc[0]
    apx_px = apx_pxs.loc[apx_pxs['SecurityID'] == int(apx_security_id)]
    apx_px = apx_px.loc[apx_px['PriceTypeID'] == APX_PRICE_TYPES[pt]['price_type_id']]
    if not len(apx_px.index):
        return True  # No APX price found for this type/security/date
    apx_px = apx_px.to_dict('records')[0]
    # TODO_CLEANUP: remove below when not needed
    # apx_px = apx_pxs.loc[(apx_pxs['SecurityID'] == apx_security_id) & 
    #     (apx_pxs['PriceTypeID'] == APX_PRICE_TYPES[pt]['price_type_id'])]
    # logging.info(apx_security_id)
    # logging.info(APX_PRICE_TYPES[pt]['price_type_id'])
    # logging.info(secs)
    # logging.info(apx_px)
    # logging.info(px['source'])
    logging.debug(f"{apx_px['SourceID']} {APX_PX_SOURCES[px['source']]} {apx_px['PriceValue']} {px[pt]}")
    if apx_px['SourceID'] != APX_PX_SOURCES[px['source']]:
        logging.debug('Different source')
        return True
    elif apx_px['PriceValue'] != px[pt]:
        logging.debug('Different value')
        return True
    else:
        logging.debug('Same')
        return False

def get_prices_file_path():
    """
    Get base path for pricing files.
    """
    return os.path.join(config.DATA_DIR, 'lw', 'pricing')

def prices_to_tab_delim_files(prices):
    """
    Write the prices to tab-delimited files.

    Args:
    - prices (Dict): A dictionary containing the prices to write.

    Returns:
    - Tuple: A tuple with the folder path and a list of file paths.
    """
    base_path = get_prices_file_path()
    today_folder = prepare_dated_file_path(folder_name=base_path, date=date.today(), file_name='', rotate=False)
    files = changed_prices = []
    secs = vwSecurityTable().read()
    for from_date in prices:
        for pt in prices[from_date]:
            full_path = prepare_dated_file_path(folder_name=base_path, date=date.today()
                , file_name=price_file_name(from_date, file_suffix=APX_PRICE_TYPES[pt]['imex_file_suffix']), rotate=True)
            pxs = pd.DataFrame(columns=['apx_sec_type','apx_symbol',pt,'message','source'])
            apx_pxs = vPriceTable().read(price_date=from_date, price_type_id=APX_PRICE_TYPES[pt]['price_type_id'])
            for px in prices[from_date][pt]:
                if is_different_from_apx_price(px, pt, secs, apx_pxs):
                    if isinstance(px['source'], str):
                        px['source'] = get_apx_SourceID(px['source'])
                    pxs = pd.concat([pxs, pd.DataFrame([px])], ignore_index=True)
            pxs.to_csv(path_or_buf=full_path, sep='\t', header=False, index=False)
            pxs['from_date'] = from_date
            changed_prices = changed_prices + pxs.to_dict('records')
            files.append(full_path)
    return today_folder, files, changed_prices

def remove_other_price_types(px, pt):
    """
    Remove fields of price/yield/duration which represent prices types other than the one provided.

    Args:
    - px (Dict): A dictionary to remove field(s) from.
    - pt (str): The price type to keep in the dictionary. Should be price/yield/duration.

    Returns:
    - Dict: The updated dictionary with field(s) removed.
    """
    if pt not in apx_price_types_list():
        return px  # Requested type is not a valid type... this should not happen.
    else:
        for t in apx_price_types_list():
            if t == pt:
                continue
            else:
                # Remove "t" key from dict. dict.pop changes the dict, which was 
                # causing yield/duration to be removed from the dict provided as an arg 
                # to this method. We do not want this, therefore should use dict comprehension:
                px = {i:px[i] for i in px if i != t}
    return px

def apx_price_types_list():
    """
    Provide valid APX price types as a list.
    """
    return list(APX_PRICE_TYPES.keys())

def add_price(res_prices, px):
    """
    Add the given price to the dictionary of prices.

    Args:
    - res_prices (Dict): The dictionary to add the price to.
    - px (Dict): The price to add. Should contain one or more of APX_PRICE_TYPES.

    Returns:
    - Dict: The updated dictionary of prices.
    """
    price = px.copy() # as to not change what was passed when we "pop" below:
    from_date = price.pop('from_date')
    if from_date not in res_prices:
        res_prices[from_date] = {}
    for pt in APX_PRICE_TYPES:
        if pt in price:
            cleaned_px = remove_other_price_types(price, pt)
            if pt in res_prices[from_date]:
                res_prices[from_date][pt].append(cleaned_px)
            else:
                res_prices[from_date][pt] = [cleaned_px]
    return res_prices

def cache_result(endpointurl, request_type='GET', payload=None, result_code=200, result_text=None):
    """
    Save the result to the cache table.

    Args:
    - endpointurl: Endpoint URL.
    - request_type: Request type (POST/GET/PUT/DELETE).
    - payload: Payload submitted with the request.
    - result_code: HTTP return code of the response.
    - result_text: Text body of the HTTP response.

    # Returns: Pyodbc result object from the bulk insert operation.
    Returns: sqlalchemy.engine.ResultProxy object from the insert operation.
    """
    # api_cache_vals = pd.DataFrame.from_dict([{
    #     'endpointurl': endpointurl
    #     , 'request_type': request_type
    #     , 'payload': payload
    #     , 'result_code': result_code
    #     , 'result_text': result_text
    # }])
    api_cache_vals = [{
        'endpointurl': endpointurl
        , 'request_type': request_type
        , 'payload': payload
        , 'result_code': result_code
        , 'result_text': result_text
    }]
    try:
        # logging.info(f'result_text {type(result_text)}, payload {type(payload)}')
        pac_table = PricingApiCacheTable()
        insert_stmt = pac_table.table_def.insert().values(api_cache_vals)
        pac_insert_result = pac_table.execute_write(insert_stmt)
        delete_stmt = pac_table.table_def.delete().where(
            pac_table.table_def.c.modified_at < datetime.now() - timedelta(hours=24)
        )
        pac_delete_result = pac_table.execute_write(delete_stmt)
        return pac_insert_result
    except exc.SQLAlchemyError as e:
        logging.exception(e)
        return {
            'status': 'error',
            'data': None,
            'message': f'SQLAlchemy error: {e}'
        }, 500

def get_last_cache(endpointurl, request_type='GET', payload=None):
    """
    Get the most recent cached result matching the criteria.

    Returns: DataFrame. Should have only a single row representing the most recent cached result.
    """
    return PricingApiCacheTable().read_latest(endpointurl, request_type, payload)

def get_last_update(securities=False, prices=False, positions=False, data_date=None):
    """
    Get the timestamp of the most recent update of any of the requested data types.

    Returns: datetime of most recent update.
    """
    latest = datetime(1970, 1, 1)
    if securities:
        sec_latest = vwSecurityTable().latest_update()
        if sec_latest is not None:
            if sec_latest > latest:
                latest = sec_latest
    if prices:
        px_latest = vwPriceTable().latest_update(data_date=data_date)
        if px_latest is not None:
            if px_latest > latest:
                latest = px_latest
    if positions:
        aa_latest = vwApxAppraisalTable().latest_update(data_date=data_date)
        if aa_latest is None:
            pos_latest = vwPositionTable().latest_update()
            if pos_latest is not None:
                if pos_latest > latest:
                    latest = pos_latest
        elif aa_latest > latest:
            latest = aa_latest
    return latest

def trigger_imex(folder, f, mode='Ama'):
    """

    """
    conn_info, _ = config.get_conn_info(config.CONN_IMEX)
    hostname = conn_info['hostname']
    prefix = conn_info['cmd_prefix']
    regedit_cmd = f"C:\\Windows\\regedit /s {prefix}\\APX$\\exe\\ServerURL.reg"
    folder = folder.replace('R:', '\\\\dev-data\\lws$')
    f = f.replace('R:', '\\\\dev-data\\lws$')
    imex_cmd = f"{prefix}\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -{mode} \"-f{f}\" -ttab4 -u"
    
    payload = {'cmd': imex_cmd}
    logging.info(f'Submitting request for IMEX cmd: {imex_cmd}')
    response = requests.post(f'{IMEX_BASE_URL}/run-cmd', json=payload)
    logging.info('Returning from trigger_imex')
    return None, None, None

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # TODO: move user&pass to env config
    ssh.connect(hostname=hostname, username='cameron johnston', password='')

    stdin, stdout, stderr = ssh.exec_command('net use R: \\\\dev-data\\lws$')
    logging.info(f'Executing on {hostname}: {regedit_cmd}')
    stdin, stdout, stderr = ssh.exec_command(regedit_cmd)
    logging.info(f'Executing on {hostname}: {imex_cmd}')
    stdin, stdout, stderr = ssh.exec_command(imex_cmd)

    output = stdout.read().decode('utf-8')
    error = stderr.read().decode('utf-8')
    return_code = stdout.channel.recv_exit_status()

    logging.info(output)
    logging.info(error)
    logging.info(return_code)

    # if return_code != 0:
    #     print('Error:', error)
    # else:
    #     print(output)

    ssh.close()

    return output, error, return_code

# TODO_CLEANUP: remove when not needed
async def trigger_imex_async(folder, f, mode='Ama'):
    """

    """
    conn_info, _ = config.get_conn_info(config.CONN_IMEX)
    hostname = conn_info['hostname']
    prefix = conn_info['cmd_prefix']
    regedit_cmd = f"C:\\Windows\\regedit /s {prefix}\\APX$\\exe\\ServerURL.reg"
    folder = folder.replace('R:', '\\\\dev-data\\lws$')
    f = f.replace('R:', '\\\\dev-data\\lws$')
    imex_cmd = f"{prefix}\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -{mode} \"-f{f}\" -ttab4 -u"
    
    payload = {'cmd': imex_cmd}
    async with aiohttp.ClientSession() as session:
        logging.info(f'Submitting aiohttp async request for IMEX cmd: {imex_cmd}')
        async with session.post(f'{IMEX_BASE_URL}/run-cmd', data=payload) as response:
            # response_text = await response.text()
            logging.info('Returning from trigger_imex within aiohttp session')
            return None, None, None
    # async with requests.Session() as session:
    #     logging.info(f'Submitting async request for IMEX cmd: {imex_cmd}')
    #     response = await session.post(f'{IMEX_BASE_URL}/run-cmd', json=payload)
    #     logging.info(f'Got response {response.status_code}: {response.text}')
    # logging.info(f'Submitting request for IMEX cmd: {imex_cmd}')
    # response = requests.post(f'{IMEX_BASE_URL}/run-cmd', json=payload)
    logging.info('Returning from trigger_imex')
    return None, None, None

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # TODO: move user&pass to env config
    ssh.connect(hostname=hostname, username='cameron johnston', password='')

    stdin, stdout, stderr = ssh.exec_command('net use R: \\\\dev-data\\lws$')
    logging.info(f'Executing on {hostname}: {regedit_cmd}')
    stdin, stdout, stderr = ssh.exec_command(regedit_cmd)
    logging.info(f'Executing on {hostname}: {imex_cmd}')
    stdin, stdout, stderr = ssh.exec_command(imex_cmd)

    output = stdout.read().decode('utf-8')
    error = stderr.read().decode('utf-8')
    return_code = stdout.channel.recv_exit_status()

    logging.info(output)
    logging.info(error)
    logging.info(return_code)

    # if return_code != 0:
    #     print('Error:', error)
    # else:
    #     print(output)

    ssh.close()

    return output, error, return_code


def create_app(name=__name__, host='0.0.0.0', port=5000, debug=True):
    app = Flask(name)
    # app.app_context()
    logging.info(f'Starting local flask server on port {port}...')
    app.run(host=host, port=port, debug=debug)

