"""
Unit tests to test individual functions which support the API.
To run these unit tests: 
    -run cmd: C:
        cd lw
        cd kafka14
        cd scripts
        (dir where this file is located)
    -run cmd: pytest test_unittests.py -v  --cov=flaskrp_helper --ff -x
        (see pytest docs or run with -h for info on args)
    -see results
"""

import base64
from datetime import date, datetime
from flask import Flask
import json
import numpy as np
import os
import pytest
import pandas as pd
import socket
from sqlalchemy import exc

from unittest import mock  # from unittest.mock import MagicMock, patch

import flaskrp_helper



# globals
app = Flask(__name__)

# Import LW libs ( PF version )
# TODO_PROD: better way to do this - detect environment somehow? 
if os.environ.get('pythonpath') is None:
	pythonpath = '\\\\dev-data\\lws$\\cameron\\lws\\libpy\\lib'
	os.environ['pythonpath'] = pythonpath
	sys.path.append(pythonpath)

# @pytest.fixture()
# def app():
#     app = flaskrp_helper.create_app(port=8000)
#     app.config.update({
#         "TESTING": True,
#     })
#     print('starting on port 8000...')
#     yield app

@pytest.fixture
def example_dataframe():
    # Create an example dataframe with NaN and NaT values
    df = pd.DataFrame({'A': [1, 2, np.nan, 4], 'B': ['foo', np.nan, 'bar', pd.NaT]})
    return df.astype(object)

def test_NaN_NaT_to_none(example_dataframe):
    # Arrange
    expected = pd.DataFrame({'A': [1, 2, None, 4], 'B': ['foo', None, 'bar', None]}).astype(object)

    # Act
    result = flaskrp_helper.NaN_NaT_to_none(example_dataframe)

    # There is a known pandas bug (or maybe expected functionality - it's been debated)
    # which impacts the following test regarding None vs np.nan results:
    # https://github.com/pandas-dev/pandas/issues/44485
    # https://github.com/pandas-dev/pandas/issues/32265
    # https://github.com/pandas-dev/pandas/issues/42751
    # So to prevent this behaviour and ensure all np.nan values do get replaced with None,
    # we can assign "object" type to all columns.
    # Also note the example_dataframe fixture does the same.

    # Assert
    assert result.equals(expected)

def test_clean_no_data():
    # Arrange
    df = pd.DataFrame()
    expected = {"data":None,"message":"No data was found.","status":"warning"}

    # Act
    with app.app_context():  # "clean" function uses flask jsonify method, which requires app context
        result = flaskrp_helper.clean(df)
        res_dict = json.loads(result.get_data(as_text=True))
    
    # Assert
    assert result.status_code == 200
    assert res_dict == expected

def test_clean_with_data(example_dataframe):
    # Arrange
    expected = {"status":"success","data":[{"A":1.0,"B":"foo"},{"A":2.0,"B":None},{"A":None,"B":"bar"},{"A":4.0,"B":None}],"message":None}

    # Act
    with app.app_context():  # "clean" function uses flask jsonify method, which requires app context
        result = flaskrp_helper.clean(example_dataframe)
        res_dict = json.loads(result.get_data(as_text=True))
    
    # Assert
    assert result.status_code == 200
    assert res_dict == expected

def test_trim_px_sources():
    # Arrange
    df = pd.DataFrame({'source': ['BB_ABC', 'BB_DEF', 'BB_GHI_DERIVED', 'FTSETMX_PX', 'FUNDRUN_EQUITY', 'FIDESK_MANUALPRICE'
        , 'FIDESK_MISSINGPRICE', 'OTHER_SOURCE', 'LW_MANUAL', 'LW_OVERRIDE']})
    expected = pd.DataFrame({'source': ['BLOOMBERG', 'BLOOMBERG', 'FTSE', 'FUNDRUN'
        , 'OVERRIDE', 'MANUAL', 'MANUAL', 'OVERRIDE']})
    
    # Act
    result = flaskrp_helper.trim_px_sources(df)

    # Assert
    assert result.equals(expected)


def test_get_chosen_price():
    # Arrange
    df = pd.DataFrame({'lw_id': [1, 2, 3], 'source': ['BLOOMBERG', 'FTSE', 'RBC'], 'price': [10.0, 20.0, 30.0]})
    expected = pd.DataFrame({'lw_id': [2], 'source': ['FTSE'], 'price': [20.0]})

    # Act
    result = flaskrp_helper.get_chosen_price(df)

    # Assert
    assert result.equals(expected)


def test_get_chosen_price_none():
    # Arrange
    df = pd.DataFrame({'lw_id': [], 'source': [], 'price': []})
    expected = None

    # Act
    result = flaskrp_helper.get_chosen_price(df)

    # Assert
    assert result == expected


@pytest.fixture
def prices_with_manual():
    return pd.DataFrame({
        'source': ['FUNDRUN', 'MANUAL', 'BLOOMBERG'],
        'price': [100, 105, 98]
    })

@pytest.fixture
def prices_without_manual():
    return pd.DataFrame({
    'source': ['MARKIT', 'BLOOMBERG'],
    'price': [100, 98]
})

@pytest.fixture
def prices_missing():
    return pd.DataFrame({
    'source': [],
    'price': []
})

@mock.patch('flaskrp_helper.PricingManualPricingSecurityTable')
def test_get_manual_pricing_securities_default_today(mock_mps, fixed_date):
    # Arrange
    mock_mps.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW123', 'LW789', 'LW456']
        ,'valid_from_date': [date(1970,1,1), date(2123,1,1), None]
        ,'valid_to_date': [None, None, date(2100,1,1)]
    }))
    expected = pd.DataFrame({
        'lw_id': ['LW123', 'LW456']
        ,'valid_from_date': [date(1970,1,1), None]
        ,'valid_to_date': [None, date(2100,1,1)]
    })

    # Act
    result = flaskrp_helper.get_manual_pricing_securities()

    # Assert
    assert result.equals(expected)


def test_is_manual_with_manual(prices_with_manual):
    # Arrange
    expected = True

    # Act
    result = flaskrp_helper.is_manual(prices_with_manual)

    # Assert
    assert result == expected

def test_is_manual_without_manual(prices_without_manual):
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.is_manual(prices_without_manual)

    # Assert
    assert result == expected

def test_is_missing_with_external(prices_with_manual):
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.is_missing(prices_with_manual)

    # Assert
    assert result == expected

def test_is_missing_with_missing(prices_missing):
    # Arrange
    expected = True

    # Act
    result = flaskrp_helper.is_missing(prices_missing)

    # Assert
    assert result == expected

def test_is_override_with_manual_and_external(prices_with_manual):
    # Arrange
    expected = True

    # Act
    result = flaskrp_helper.is_override(prices_with_manual)

    # Assert
    assert result == expected

def test_is_override_without_manual(prices_without_manual):
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.is_override(prices_without_manual)

    # Assert
    assert result == expected

def test_first2chars_with_string():
    # Arrange
    expected = 'he'

    # Act
    result = flaskrp_helper.first2chars('hello')

    # Assert
    assert result == expected

def test_first2chars_with_none():
    # Arrange
    expected = None

    # Act
    result = flaskrp_helper.first2chars(None)

    # Assert
    assert result == expected

def test_add_valid_dates():
    # Arrange
    payload = {'key': 'value'}

    # Act
    output = flaskrp_helper.add_valid_dates(payload)

    # Assert
    assert 'valid_from_date' in output
    assert 'valid_to_date' in output
    assert output['valid_from_date'] == date.today()
    assert output['valid_to_date'] == None

def test_add_asof_with_existing_asofuser():
    # Arrange
    payload = {'key': 'value', 'asofuser': 'test_user'}

    # Act
    output = flaskrp_helper.add_asof(payload)

    # Assert
    assert 'modified_at' in output
    assert isinstance(output['modified_at'], str)
    assert output['modified_by'] == 'test_user'

def test_add_asof_with_existing_modified_by():
    # Arrange
    payload = {'key': 'value', 'modified_by': 'test_user'}

    # Act
    output = flaskrp_helper.add_asof(payload)

    # Assert
    assert 'modified_at' in output
    assert isinstance(output['modified_at'], str)
    assert output['modified_by'] == 'test_user'

def test_add_asof_without_existing_asofuser():
    # Arrange
    payload = {'key': 'value'}

    # Act
    output = flaskrp_helper.add_asof(payload)

    # Assert
    assert 'modified_at' in output
    assert isinstance(output['modified_at'], str)
    assert 'modified_by' in output
    assert output['modified_by'] == f"{os.getlogin()}_{socket.gethostname()}"

@pytest.fixture
def example_secs_with_sec_types():
    return pd.DataFrame({
        'pms_sec_type': ['cb', 'cm', 'cs', 'cu'],
        'security_id': [1, 2, 3, 4],
    })

def test_get_securities_by_sec_type(example_secs_with_sec_types):
    # Arrange
    expected = pd.DataFrame({'pms_sec_type': ['cu'], 'security_id': [4]})

    # Act
    result = flaskrp_helper.get_securities_by_sec_type(example_secs_with_sec_types, sec_type='cu')
    
    # Assert
    assert result.equals(expected)

def test_get_securities_by_sec_type_no_func(example_secs_with_sec_types):
    # Arrange
    expected = pd.DataFrame({'pms_sec_type': ['cu'], 'security_id': [4]})

    # Act
    result = flaskrp_helper.get_securities_by_sec_type(example_secs_with_sec_types, sec_type='cu', sec_type_func=None)
    
    # Assert
    assert result.equals(expected)

def test_get_securities_by_sec_type_bond(example_secs_with_sec_types):
    # Arrange
    expected = pd.DataFrame({'pms_sec_type': ['cb', 'cm'], 'security_id': [1, 2]})

    # Act
    result = flaskrp_helper.get_securities_by_sec_type(example_secs_with_sec_types, sec_type='bond')
    
    # Assert
    assert result.equals(expected)

def test_get_securities_by_sec_type_equity(example_secs_with_sec_types):
    # Arrange
    expected = pd.DataFrame({'pms_sec_type': ['cs', 'cu'], 'security_id': [3, 4]})

    # Act
    result = flaskrp_helper.get_securities_by_sec_type(example_secs_with_sec_types, sec_type='equity')
    
    # Assert
    assert result.equals(expected)

@pytest.fixture
def mock_appraisal():
    return pd.DataFrame({
        'ProprietarySymbol': ['LW123', 'LW456', 'LW789']
    })

@pytest.fixture
def mock_securities():
    return pd.DataFrame({
        'lw_id': ['LW123', 'LW456', 'LW789']
        ,'apx_sec_type': ['cbca', 'csus', 'cmca']
    })

# @mock.patch('flaskrp_helper.vwSecurityTable.read', return_value=pd.DataFrame({
#     'lw_id': ['LW123', 'LW456', 'LW789']
#     ,'apx_sec_type': ['cbca', 'csus', 'cmca']
# }))
# @mock.patch('flaskrp_helper.vwHeldSecurityTable.read', return_value=pd.DataFrame({
#     'lw_id': ['LW123', 'LW456', 'LW789']
#     ,'apx_sec_type': ['cbca', 'csus', 'cmca']
# }))
# @mock.patch('flaskrp_helper.ApxAppraisalTable.read_for_date', return_value=pd.DataFrame({
#     'ProprietarySymbol': ['LW123', 'LW456', 'LW789']
# }))

@mock.patch('flaskrp_helper.vwSecurityTable')
@mock.patch('flaskrp_helper.vwHeldSecurityTable')
@mock.patch('flaskrp_helper.vwApxAppraisalTable')
def test_get_held_securities(mock_appr, mock_held_sec, mock_sec):
    # Arrange  # TODO_REFACTOR: more code-efficient way to do this?
    mock_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW123', 'LW456', 'LW789']
        ,'pms_sec_type': ['cbca', 'csus', 'cmca']
    }))
    mock_held_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW123', 'LW789']
        ,'pms_sec_type': ['cbca', 'cmca']
    }))
    mock_appr.return_value.read_for_date = mock.Mock(return_value=pd.DataFrame({
        'ProprietarySymbol': ['LW123', 'LW789']
    }))
    expected = pd.DataFrame({
        'lw_id': ['LW123', 'LW789']
        ,'apx_sec_type': ['cbca', 'cmca']
    })

    # Act
    result = flaskrp_helper.get_held_securities('20230104')

    # Assert
    assert result.equals(expected)

@mock.patch('flaskrp_helper.vwSecurityTable')
@mock.patch('flaskrp_helper.vwHeldSecurityTable')
@mock.patch('flaskrp_helper.vwApxAppraisalTable')
def test_get_held_securities_equity(mock_appr, mock_held_sec, mock_sec):
    # Arrange  # TODO_REFACTOR: more code-efficient way to do this?
    mock_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW123', 'LW456', 'LW789']
        ,'pms_sec_type': ['cbca', 'csus', 'cmca']
    }))
    mock_held_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW456', 'LW789']
        ,'pms_sec_type': ['csus', 'cmca']
    }))
    mock_appr.return_value.read_for_date = mock.Mock(return_value=pd.DataFrame({
        'ProprietarySymbol': ['LW456', 'LW789']
    }))
    expected = pd.DataFrame({
        'lw_id': ['LW456']
        ,'apx_sec_type': ['csus']
    })

    # Act
    result = flaskrp_helper.get_held_securities('20230104', sec_type='equity')

    # Assert
    assert result.equals(expected)

@mock.patch('flaskrp_helper.vwSecurityTable')
@mock.patch('flaskrp_helper.vwHeldSecurityTable')
@mock.patch('flaskrp_helper.vwApxAppraisalTable')
def test_get_held_securities_equity_live_positions(mock_appr, mock_held_sec, mock_sec):
    # Arrange  # TODO_REFACTOR: more code-efficient way to do this?
    mock_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW123', 'LW456', 'LW789']
        ,'pms_sec_type': ['cbca', 'csus', 'cmca']
    }))
    mock_held_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['LW456', 'LW789']
        ,'pms_sec_type': ['csus', 'cmca']
    }))
    mock_appr.return_value.read_for_date = mock.Mock(return_value=pd.DataFrame({
        'ProprietarySymbol': []
    }))
    expected = pd.DataFrame({
        'lw_id': ['LW456']
        ,'apx_sec_type': ['csus']
    })

    # Act
    result = flaskrp_helper.get_held_securities('20230104', sec_type='equity')

    # Assert
    assert result.equals(expected)

@pytest.fixture
def empty_held_df():
    return pd.DataFrame({
        'lw_id': ['1', '2', '3', '4'],
        'prices': [None, None, None, None],
        'chosen_price': [None, None, None, None],
        'audit_trail': [None, None, None, None]
    })

@pytest.fixture
def curr_prices_df():
    return pd.DataFrame({
        'lw_id': ['1', '2', '2', '3', '4'],
        'price': [100, 50, 75, 200, 150],
        'source':['FUNDRUN', 'MARKIT', 'MANUAL', 'FTSE', 'FUNDRUN']
    })

@pytest.fixture
def prev_prices_df():
    return pd.DataFrame({
        'lw_id': ['1', '1', '2', '3', '4', '4'],
        'price': [90, 80, 40, 180, 140, 130],
        'source':['FUNDRUN', 'MARKIT', 'MANUAL', 'FTSE', 'FUNDRUN', 'MISSING']
    })

@pytest.fixture
def curr_audit_trail_df():
    return pd.DataFrame({
        'lw_id': ['1', '2', '2', '3', '4'],
        'action': ['Buy', 'Buy', 'Sell', 'Buy', 'Sell'],
        'date': ['2022-03-20', '2022-03-20', '2022-03-19', '2022-03-20', '2022-03-20']
    })

# TODO: update & re-activate this test 
# def test_add_prices(empty_held_df, curr_prices_df, prev_prices_df):
#     # Arrange
#     i = 1
#     lw_id = '2'

#     # Act
#     held_with_prices, prices = flaskrp_helper.add_prices(empty_held_df, i, lw_id, curr_prices_df, prev_prices_df)

#     # Assert
#     assert held_with_prices.loc[i, 'prices'] is not None
#     assert held_with_prices.loc[i, 'chosen_price'] is not None
#     assert isinstance(held_with_prices.loc[i, 'prices'], list)
#     assert isinstance(held_with_prices.loc[i, 'chosen_price'], list)
#     assert len(prices) == 3  # should be 3 rows for security with lw_id '2'

def test_add_audit_trail(empty_held_df, curr_audit_trail_df):
    # Arrange
    i = 3
    lw_id = '3'

    # Act
    held_with_audit_trail = flaskrp_helper.add_audit_trail(empty_held_df, i, lw_id, curr_audit_trail_df)

    # Assert
    assert held_with_audit_trail.loc[i, 'audit_trail'] is not None
    assert isinstance(held_with_audit_trail.loc[i, 'audit_trail'], list)
    assert len(held_with_audit_trail.loc[i, 'audit_trail']) == 1  # should be 1 row for security with lw_id '3'
    assert held_with_audit_trail.loc[i, 'audit_trail'][0]['action'] == 'Buy'


@pytest.fixture
def prices_with_source():
    return pd.DataFrame({'price': [1.0, 2.0, 3.0], 'source': ['MANUAL', 'FUNDRUN', 'BLOOMBERG']})

@pytest.fixture
def manually_priced_secs():
    return pd.DataFrame({'lw_id': ['456']})

def test_should_exclude_sec_manual_false():
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.should_exclude_sec('MANUAL', 'MANUAL')
    
    # Assert
    assert result == expected
    
def test_should_exclude_sec_manual_true():
    # Arrange
    expected = True

    # Act
    result = flaskrp_helper.should_exclude_sec('BLOOMBERG', 'MANUAL')

    # Assert
    assert result == expected

def test_should_exclude_sec_missing_true():
    # Arrange
    expected = True

    # Act
    result = flaskrp_helper.should_exclude_sec('MARKIT', 'MISSING')

    # Assert
    assert result == expected
    
def test_should_exclude_sec_missing_false():
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.should_exclude_sec(None, 'MISSING')

    # Assert
    assert result == expected
    
def test_should_exclude_sec_favourite():
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.should_exclude_sec('BLOOMBERG', 'FAVOURITE')

    # Assert
    assert result == expected
    
def test_should_exclude_sec_none():
    # Arrange
    expected = False

    # Act
    result = flaskrp_helper.should_exclude_sec('A_SOURCE', None)

    # Assert
    assert result == expected



@pytest.fixture
def example_data():
    return {'data_date': '20220101', 'curr_bday': '20220321', 'prev_bday': '20220318', 'sec_type': 'bond', 'price_type': 'manual'}

# TODO: update & re-activate this test 
# @mock.patch('flaskrp_helper.get_held_securities', return_value=pd.DataFrame({
#     'lw_id': ['LW123', 'LW789']
#     ,'apx_sec_type': ['csca', 'cmca']
# }))
# @mock.patch('flaskrp_helper.get_manual_pricing_securities', return_value=pd.DataFrame({
#     'lw_id': ['LW123']
# }))
# @mock.patch('flaskrp_helper.get_next_bday', return_value=date(2022,3,22))
# @mock.patch('flaskrp_helper.vwPriceTable')
# @mock.patch('flaskrp_helper.PricingAuditTrailTable')
# def test_get_held_security_prices(mock_audit_table, mock_price_table, mock_gnb, mock_gmps, mock_ghs):
#     # Arrange
#     mock_price_table.return_value.read = mock.Mock(return_value=pd.DataFrame(
#         {'lw_id': ['LW123', 'LW789', 'LW789'], 'source': ['MANUAL', 'FTSE', 'BB_BOND']}))
#     mock_audit_table.return_value.read = mock.Mock(return_value=pd.DataFrame(
#         {'lw_id': ['LW123'], 'reason': ['Reason #7: Other']}))
#     new_example_data = {'curr_bday': '20220321', 'prev_bday': '20220318'}
#     expected = {
#         'status': 'success',
#         'message': None,
#         'data': [
#             {
#                 'apx_sec_type': 'csca'
#                 , 'audit_trail': [{'lw_id': 'LW123', 'reason': 'Reason #7: Other'}]
#                 , 'chosen_price': {'lw_id': 'LW123', 'source': 'MANUAL'}
#                 , 'good_thru_date': 'Tue, 22 Mar 2022 00:00:00 GMT'
#                 , 'lw_id': 'LW123'
#                 , 'prices': [
#                     {'lw_id': 'LW123', 'source': 'MANUAL'}
#                     , {'lw_id': 'LW123', 'source': 'APX'}
#                 ]
#             }
#             , {'apx_sec_type': 'cmca'
#                 , 'audit_trail': None
#                 , 'chosen_price': [{'lw_id': 'LW789', 'source': 'FTSE'}]
#                 , 'good_thru_date': 'Tue, 22 Mar 2022 00:00:00 GMT'
#                 , 'lw_id': 'LW789'
#                 , 'prices': [
#                     {'lw_id': 'LW789', 'source': 'FTSE'}, {'lw_id': 'LW789', 'source': 'BLOOMBERG'}
#                     , {'lw_id': 'LW789', 'source': 'APX'}, {'lw_id': 'LW789', 'source': 'APX'}
#                 ]
#             }
#         ]
#     }

#     # Act
#     with app.app_context():
#         result = flaskrp_helper.get_held_security_prices(curr_bday='20220321', prev_bday='20220318')
#         res_dict = json.loads(result.get_data(as_text=True))
    
#     # Assert
#     assert result.status_code == 200
#     assert isinstance(res_dict, dict)
#     assert res_dict == expected
#     # TODO: add more valuable assertions; maybe more test functions too

def test_get_pricing_attachment_folder(example_data):
    # Act
    result = flaskrp_helper.get_pricing_attachment_folder(example_data['data_date'])

    # Assert
    assert isinstance(result, str)

def test_save_binary_files(example_data):
    # Arrange
    expected_status = 'success'
    expected_return_code = 201
    files = [{'name': 'test_file.txt', 'binary_content': base64.b64encode(b'\x00\x01\x02\x03').decode()}]

    # Act
    result = flaskrp_helper.save_binary_files(example_data['data_date'], files)

    # Assert
    assert result[0]['status'] == expected_status
    assert result[1] == expected_return_code

def test_save_binary_files_no_files(example_data):
    # Arrange
    expected_status = 'warning'
    expected_return_code = 200
    files = []

    # Act
    result = flaskrp_helper.save_binary_files(example_data['data_date'], files)

    # Assert
    assert result[0]['status'] == expected_status
    assert result[1] == expected_return_code

def test_delete_dir_contents(example_data):
    # Arrange
    expected_status = 'success'
    expected_return_code = 200

    # Act
    folder_path = flaskrp_helper.get_pricing_attachment_folder(example_data['data_date'])
    result = flaskrp_helper.delete_dir_contents(folder_path)

    # Assert
    assert result[0]['status'] == expected_status
    assert result[1] == expected_return_code

def test_delete_dir_contents_failure(example_data):
    # Arrange
    expected_status = 'error'
    expected_return_code = 500
    folder_path = '\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing\\202201\\99'
    with mock.patch('flaskrp_helper.os.remove', side_effect=Exception('Simulating delete failing')):

        # Act
        result = flaskrp_helper.delete_dir_contents(folder_path)

    # Assert
    assert result[1] == expected_return_code
    assert result[0]['status'] == expected_status
    assert 'Failed to delete files' in result[0]['message']

def test_valid_with_null_dates():
    # Arrange
    expected_length = 3
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c'],
        'valid_from_date': [pd.NaT, pd.NaT, pd.NaT],
        'valid_to_date': [pd.NaT, pd.NaT, pd.NaT]
    })
    
    # Act
    result = flaskrp_helper.valid(df)

    # Assert
    assert len(result) == expected_length
    
def test_valid_with_valid_dates():
    # Arrange
    expected_length = 3
    expected_set = {'b', 'd', 'e'}
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c', 'd', 'e'],
        'valid_from_date': [date(2022, 1, 1), date(2022, 2, 1)
            , date(2022, 3, 1), date(2022, 2, 1), None],
        'valid_to_date': [date(2022, 1, 31), date(2022, 2, 28)
            , date(2022, 3, 31), None, date(2022, 4, 30)]
    })
    data_date = date(2022, 2, 1)

    # Act
    result = flaskrp_helper.valid(df, data_date)

    # Assert that the result is correct
    assert len(result) == expected_length
    assert set(result['col1']) == expected_set
    
def test_valid_with_invalid_dates():
    # Arrange
    expected_length = 0
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c'],
        'valid_from_date': [date(2022, 1, 1), 
            date(2022, 2, 1), date(2022, 3, 1)],
        'valid_to_date': [date(2022, 1, 31), 
            date(2022, 2, 28), date(2022, 3, 31)]
    })
    data_date = date(2022, 4, 1)

    # Act
    result = flaskrp_helper.valid(df, data_date)

    # Assert
    assert len(result) == expected_length    

@mock.patch('flaskrp_helper.config')
def test_save_df_to_table_success(mock_config):
    # Arrange
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c'],
        'col2': [1, 2, 3]
    })
    table = mock.MagicMock()
    table.bulk_insert.return_value = mock.MagicMock(rowcount=3)
    table.database_key = 'DB'
    table.schema = 'schema'
    table.table_name = 'table'

    # Act
    result, code = flaskrp_helper.save_df_to_table(df, table)

    # Assert
    assert code == 201
    assert result['status'] == 'success'
    assert result['message'] == 'Successfully saved 3 rows.'
    
@mock.patch('flaskrp_helper.config')
def test_save_df_to_table_sqlalchemy_error(mock_config):
    # Arrange
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c'],
        'col2': [1, 2, 3]
    })
    table = mock.MagicMock()
    table.bulk_insert.return_value = mock.MagicMock(rowcount=3)
    table.bulk_insert.side_effect = exc.SQLAlchemyError('Something went wrong')
    expected_code = 500
    expected_res = {
        'status': 'error'
        , 'data': None
        , 'message': 'SQLAlchemy error: Something went wrong'
    }

    # Act
    result, status_code = flaskrp_helper.save_df_to_table(df, table)

    # Assert
    print(result)
    print(expected_res)
    assert status_code == expected_code
    assert result == expected_res
    
def test_save_df_to_table_rowcount_error():
    # Arrange
    expected = {
        'code': 500,
        'status': 'error',
        'message': 'Expected 3 rows to be saved, but there were 2!'
    }
    df = pd.DataFrame({
        'col1': ['a', 'b', 'c'],
        'col2': [1, 2, 3]
    })
    table = mock.MagicMock()
    table.bulk_insert.return_value = mock.MagicMock(rowcount=2)

    # Act
    result, code = flaskrp_helper.save_df_to_table(df, table)

    # Assert
    assert code == expected['code']
    assert result['status'] == expected['status']
    assert result['message'] == expected['message']



@pytest.fixture
def monitor_table():
    # monitor_table = mock.MagicMock()
    # monitor_table.return_value = mock.MagicMock(
    #     read=mock.MagicMock(
    #         # return_value=mock.MagicMock(
    #         #     columns=["run_status", "asofdate"],
    #         #     data=[[0, datetime.now()], [0, datetime.now()]],
    #         # )
    #         return_value=pd.DataFrame({
    #             "run_status": [0, 0], "asofdate": [datetime.now(), datetime.now()]
    #         })
    #     )
    # )
    with mock.patch("flaskrp_helper.MonitorTable") as monitor_table:
        monitor_table.read.return_value = pd.DataFrame({
            "run_status": [0, 0], "asofdate": [datetime.now(), datetime.now()]
        })
        yield monitor_table

@pytest.fixture
def fixed_datetime():
    with mock.patch("flaskrp_helper.datetime") as datetime_mock:
        datetime_mock.now.return_value = datetime(2022, 3, 20, 12, 0)
        datetime_mock.fromordinal.return_value = datetime(1970, 1, 1)
        yield datetime_mock

@pytest.fixture
def fixed_date():
    with mock.patch("flaskrp_helper.date") as date_mock:
        date_mock.today.return_value = date(2022, 1, 1)
        yield date_mock

def test_is_error_no_error(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [0, 0], "asofdate": [now, now]
    }))
    expected = {
        'res': False,
        'max_ts': datetime(2022, 3, 20, 12, 0)
    }

    # Act
    res, max_ts = flaskrp_helper.is_error("FTSE", '2022-03-20')

    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_error_error(monitor_table, fixed_datetime):
    # Arrange
    # Mock monitor table as returning run_status as 1 (error)
    # monitor_table.return_value.read.return_value.data = [[1, datetime.now()], [1, datetime.now()]]
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [0, 1], "asofdate": [now, now]
    }))
    expected = {
        'res': True,
        'max_ts': now
    }
    
    # Act
    res, max_ts = flaskrp_helper.is_error("FTSE", '2022-03-20')

    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_priced_returns_false_when_not_priced(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [-1, -1], "asofdate": [now, now]
    }))
    expected = {
        'res': False,
        'max_ts': datetime(2022, 3, 20, 12, 0)
    }

    # Act
    res, max_ts = flaskrp_helper.is_priced("FTSE", '2022-03-20')
    
    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_priced_returns_true_when_priced(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [0], "asofdate": [now]
    }))
    expected = {
        'res': True,
        'max_ts': now
    }

    # Act
    res, max_ts = flaskrp_helper.is_priced("FTSE", '2022-03-20')
    
    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_priced_invalid_feed(fixed_datetime):
    # Arrange
    expected = {
        'res': False,
        'max_ts': fixed_datetime.now()
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        res, max_ts = flaskrp_helper.is_priced("pf1", '2022-03-20')
    
    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_in_progress_not_in_progress(monitor_table, fixed_datetime):
    # Arrange
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [], "asofdate": []
    }))
    expected = {
        'res': False,
        'max_ts': datetime(2022, 3, 20, 12, 0)
    }

    # Act
    res, max_ts = flaskrp_helper.is_in_progress("FTSE", '2022-03-20')
    
    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

def test_is_in_progress_returns_true_when_in_progress(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [-1, -1], "asofdate": [now, now]
    }))
    expected = {
        'res': True,
        'max_ts': now
    }

    # Act
    res, max_ts = flaskrp_helper.is_in_progress("FTSE", '2022-03-20')
    
    # Assert
    assert res == expected['res']
    assert max_ts == expected['max_ts']

@mock.patch('flaskrp_helper.is_priced', return_value=(True, datetime(2022, 3, 20, 12, 0)))
def test_is_delayed_when_priced(mock_is_priced, monitor_table, fixed_datetime):
    # Arrange
    pf = 'FTSE'
    data_date = date(2022, 1, 1)
    expected = (False, fixed_datetime.now())

    # Act
    result = flaskrp_helper.is_delayed(pf, data_date)

    # Assert
    assert result == expected

@mock.patch('flaskrp_helper.is_priced', return_value=(False, datetime(2022, 1, 1, 14, 31)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
def test_is_delayed_when_not_priced_normal_eta_has_passed(mock_is_priced, mock_normal_eta):
    # Arrange
    pf = 'MARKIT'
    data_date = date(2022, 1, 1)
    current_time = datetime(2022, 1, 1, 14, 31)
    expected = (True, current_time)

    # Act
    with mock.patch('flaskrp_helper.datetime') as mock_datetime:
        mock_datetime.now.return_value = current_time
        mock_datetime.today.return_value = current_time
        result = flaskrp_helper.is_delayed(pf, data_date)

    # Assert
    assert result == expected


def test_is_pending_when_ftp_upload_complete(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [0, 0], "asofdate": [now, now]
    }))
    pf = 'FUNDRUN'
    data_date = date(2022, 1, 1)
    expected = (True, now)

    # Act
    result = flaskrp_helper.is_pending(pf, data_date)

    # Assert
    assert result == expected


def test_is_pending_when_ftp_upload_not_complete(monitor_table, fixed_datetime):
    # Arrange
    now = datetime.now()
    monitor_table.return_value.read = mock.Mock(return_value=pd.DataFrame({
        "run_status": [-1, -1], "asofdate": [now, now]
    }))
    pf = 'BLOOMBERG'
    data_date = date(2022, 1, 1)
    expected = (False, fixed_datetime.now())

    # Act
    result = flaskrp_helper.is_pending(pf, data_date)

    # Assert
    assert result == expected


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.get_current_bday', return_value=datetime.today())
@mock.patch('flaskrp_helper.get_previous_bday', return_value=datetime.today())
@mock.patch('flaskrp_helper.isinstance', return_value=False)
def test_get_pricing_feed_status_no_status(m1, m2, m3, m4, m5, m6, monitor_table, fixed_datetime):
    # Arrange
    expected_code = 200

    # Act
    response, status_code = flaskrp_helper.get_pricing_feed_status()

    # Assert
    assert status_code == expected_code
    assert "data" in response
    assert len(response["data"]) == len(flaskrp_helper.PRICING_FEEDS)
    for pf in response["data"]:
        assert "status" in response["data"][pf]
        if response["data"][pf]["status"] is not None:
            assert response["data"][pf]["status"] == '-'


@mock.patch('flaskrp_helper.is_error', return_value=(True, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(True, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_feed_security_type', return_value='pf1 security type')
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 1, 1))
def test_get_pricing_feed_status_error(m1, m2, m3, m4, m5, m6, m7, m8):
    # Arrange
    expected_code = 200
    expected_res = {
        'status': 'success'
        , 'data': {
            'pf1': {
                'status': 'ERROR'
                , 'asofdate': datetime(2022, 3, 20, 12, 0).isoformat()
                , 'normal_eta': datetime(2022, 1, 1, 13, 30).isoformat()
                , 'security_type': 'pf1 security type'
            }
        }
        , 'message': None
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date=date(2022, 1, 1))

    # Assert
    assert status_code == expected_code
    assert response == expected_res


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(True, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_feed_security_type', return_value='pf1 security type')
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 1, 1))
def test_get_pricing_feed_status_priced(m1, m2, m3, m4, m5, m6, m7, m8):
    # Arrange
    expected_code = 200
    expected_res = {
        'status': 'success'
        , 'data': {
            'pf1': {
                'status': 'PRICED'
                , 'asofdate': datetime(2022, 3, 20, 13, 0).isoformat()
                , 'normal_eta': datetime(2022, 1, 1, 13, 30).isoformat()
                , 'security_type': 'pf1 security type'
            }
        }
        , 'message': None
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date=date(2022, 1, 1))

    # Assert
    assert status_code == expected_code
    assert response == expected_res


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(False, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(True, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_feed_security_type', return_value='pf1 security type')
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 1, 1))
def test_get_pricing_feed_status_in_progress(m1, m2, m3, m4, m5, m6, m7, m8):
    # Arrange
    expected_code = 200
    expected_res = {
        'status': 'success'
        , 'data': {
            'pf1': {
                'status': 'IN PROGRESS'
                , 'asofdate': datetime(2022, 3, 20, 12, 0).isoformat()
                , 'normal_eta': datetime(2022, 1, 1, 13, 30).isoformat()
                , 'security_type': 'pf1 security type'
            }
        }
        , 'message': None
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date=date(2022, 1, 1))

    # Assert
    print(expected_res)
    print(response)
    assert status_code == expected_code
    assert response == expected_res


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(False, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(True, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_feed_security_type', return_value='pf1 security type')
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 1, 1))
def test_get_pricing_feed_status_delayed(m1, m2, m3, m4, m5, m6, m7, m8):
    # Arrange
    expected_code = 200
    expected_res = {
        'status': 'success'
        , 'data': {
            'pf1': {
                'status': 'DELAYED'
                , 'asofdate': datetime(2022, 3, 20, 14, 0).isoformat()
                , 'normal_eta': datetime(2022, 1, 1, 13, 30).isoformat()
                , 'security_type': 'pf1 security type'
            }
        }
        , 'message': None
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date=date(2022, 1, 1))

    # Assert
    assert status_code == expected_code
    assert response == expected_res


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(False, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(True, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_feed_security_type', return_value='pf1 security type')
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 1, 1))
def test_get_pricing_feed_status_pending(m1, m2, m3, m4, m5, m6, m7, m8):
    # Arrange
    expected_code = 200
    expected_res = {
        'status': 'success'
        , 'data': {
            'pf1': {
                'status': 'PENDING'
                , 'asofdate': datetime(2022, 3, 20, 15, 0).isoformat()
                , 'normal_eta': datetime(2022, 1, 1, 13, 30).isoformat()
                , 'security_type': 'pf1 security type'
            }
        }
        , 'message': None
    }
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date=date(2022, 1, 1))

    # Assert
    assert status_code == expected_code
    assert response == expected_res


@mock.patch('flaskrp_helper.is_error', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_priced', return_value=(True, datetime(2022, 3, 20, 13, 0)))
@mock.patch('flaskrp_helper.is_delayed', return_value=(False, datetime(2022, 3, 20, 14, 0)))
@mock.patch('flaskrp_helper.is_in_progress', return_value=(False, datetime(2022, 3, 20, 12, 0)))
@mock.patch('flaskrp_helper.is_pending', return_value=(False, datetime(2022, 3, 20, 15, 0)))
@mock.patch('flaskrp_helper.get_normal_eta', return_value=datetime(2022, 1, 1, 13, 30))
@mock.patch('flaskrp_helper.get_current_bday', return_value=date(2022, 3, 1))
def test_get_pricing_feed_status_sqlalchemy_error(m1, m2, m3, m4, m5, m6, m7):
    # Arrange
    expected_code = 500
    expected_res = {
        'status': 'error'
        , 'data': None
        , 'message': 'SQLAlchemy error: Something went wrong'
    }
    with mock.patch('flaskrp_helper.is_error', side_effect=exc.SQLAlchemyError('Something went wrong')):

        # Act
        response, status_code = flaskrp_helper.get_pricing_feed_status(price_date='20220301')

    # Assert
    assert status_code == expected_code
    assert response == expected_res


def test_get_apx_SourceID_bloomberg():
    # Arrange
    source = "BLOOMBERG"
    expected_result = 3004

    # Act
    result = flaskrp_helper.get_apx_SourceID(source) 
    
    # Assert 
    assert result == expected_result

def test_get_apx_SourceID_manual():
    # Arrange
    source = "MANUAL"
    expected_result = 3031

    # Act
    result = flaskrp_helper.get_apx_SourceID(source) 
    
    # Assert 
    assert result == expected_result

def test_get_apx_SourceID_default():
    # Arrange
    source = "abcdef"
    expected_result = 3000

    # Act
    result = flaskrp_helper.get_apx_SourceID(source) 
    
    # Assert 
    assert result == expected_result

def test_price_file_name_without_price_type():
    # Arrange
    from_date = "2022-01-02"
    expected_result = "010222.pri"

    # Act
    result = flaskrp_helper.price_file_name(from_date)
    
    # Assert
    assert result == expected_result

def test_price_file_name_with_price_type():
    # Arrange
    from_date = "2022-01-02"
    price_type = "yield"
    expected_result = "010222yield.pri"

    # Act
    result = flaskrp_helper.price_file_name(from_date, price_type)
    
    # Assert
    assert result == expected_result

def test_price_file_name_with_invalid_date_format():
    # Arrange
    from_date = "01-02-2022"

    # Act + Assert
    with pytest.raises(ValueError):
        flaskrp_helper.price_file_name(from_date)



@pytest.fixture
def example_prices():
    return {
        # 'from_date': date(2022,1,1),
        '2022-01-01': {
            'price': [
                {
                    'apx_sec_type': 'cbca',
                    'apx_symbol': 'ABC123',
                    'price': 50.0,
                    'source': 'BLOOMBERG'
                }
            ],
            'duration': [
                {
                    'apx_sec_type': 'cmca',
                    'apx_symbol': 'GHI789',
                    'duration': 3.3333,
                    'source': 'FTSE'
                }
            ],
            'yield': [
                {
                    'apx_sec_type': 'cbus',
                    'apx_symbol': 'DEF456',
                    'yield': 4.4444,
                    'source': 'MARKIT'
                }
            ]
        }
    }

def test_add_price(example_prices):
    # Arrange
    px = {
        'from_date': '2022-02-01',
        'apx_sec_type': 'CASH',
        'apx_symbol': 'GBP',
        'price': 70.0,
        'source': 'CJJ'
    }

    # Act
    res_prices = flaskrp_helper.add_price(example_prices, px)
    px_without_from_date = {k:px[k] for k in px if k != 'from_date'}

    # Assert
    print(res_prices)
    print(px)
    assert '2022-02-01' in res_prices
    assert 'price' in res_prices['2022-02-01']
    assert px_without_from_date in res_prices['2022-02-01']['price']

def test_remove_other_price_types():
    # Arrange
    px = {
        'apx_sec_type': 'CASH',
        'apx_symbol': 'JKL012',
        'price': 123.456,
        'yield': 4.4444,
        'duration': 5.5555,
        'source': 'MARKIT'
    }
    expected = {
        'price': {
            'apx_sec_type': 'CASH',
            'apx_symbol': 'JKL012',
            'price': 123.456,
            'source': 'MARKIT'
        },
        'yield': {
            'apx_sec_type': 'CASH',
            'apx_symbol': 'JKL012',
            'yield': 4.4444,
            'source': 'MARKIT'
        },
        'duration': {
            'apx_sec_type': 'CASH',
            'apx_symbol': 'JKL012',
            'duration': 5.5555,
            'source': 'MARKIT'
        }
    }

    for f in expected:
        # Act
        result = flaskrp_helper.remove_other_price_types(px, f)

        # Assert
        assert result == expected[f]

def test_add_duration_and_yield(example_prices):
    # Arrange
    px = {
        'from_date': '2022-03-01',
        'apx_sec_type': 'cbca',
        'apx_symbol': 'ABC123',
        'duration': 1.1111,
        'yield': 2.2222,
        'source': 'CJJ'
    }

    # Act
    res_prices = flaskrp_helper.add_price(example_prices, px)
    px_without_from_date = {k:px[k] for k in px if k != 'from_date'}
    px_duration = flaskrp_helper.remove_other_price_types(px_without_from_date, 'duration')
    px_yield = flaskrp_helper.remove_other_price_types(px_without_from_date, 'yield')

    # Assert
    assert '2022-03-01' in res_prices
    assert 'duration' in res_prices['2022-03-01']
    assert 'yield' in res_prices['2022-03-01']
    assert px_duration in res_prices['2022-03-01']['duration']
    assert px_yield in res_prices['2022-03-01']['yield']


@mock.patch('flaskrp_helper.get_prices_file_path', return_value='\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing')
@mock.patch('flaskrp_helper.vwSecurityTable')
@mock.patch('flaskrp_helper.vPriceTable')
def test_prices_to_tab_delim_files(mock_apx_price, mock_sec, m1, fixed_date, example_prices):
    # Arrange
    mock_sec.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'lw_id': ['ABC123', 'GHI789', 'DEF456']
        ,'pms_sec_type': ['cbca', 'cmca', 'cbus']
        ,'pms_symbol': ['ABC123', 'GHI789', 'DEF456']
        ,'pms_security_id': ['111', '222', '333']
    }))
    mock_apx_price.return_value.read = mock.Mock(return_value=pd.DataFrame({
        'SecurityID': [], 'PriceTypeID': []
    }))
    expected = {
        'today_folder': '\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing\\202201\\01\\'
        , 'files': [
            '\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing\\202201\\01\\010122.pri'
            , '\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing\\202201\\01\\010122_LWBondDur.pri'
            , '\\\\dev-data\\lws$\\cameron\\lws\\var\\data\\pricing\\202201\\01\\010122_LWBondYield.pri'
        ]
        , 'changed_prices': [
            {'apx_sec_type': 'cbca', 'apx_symbol': 'ABC123', 'price': 50.0, 'message': np.nan
                , 'source': 3004, 'from_date': '2022-01-01'}
            , {'apx_sec_type': 'cmca', 'apx_symbol': 'GHI789', 'duration': 3.3333, 'message': np.nan
                , 'source': 3006, 'from_date': '2022-01-01'}
            , {'apx_sec_type': 'cbus', 'apx_symbol': 'DEF456', 'yield': 4.4444, 'message': np.nan
                , 'source': 3005, 'from_date': '2022-01-01'}
        ]
    }

    # Act
    today_folder, files, changed_prices = flaskrp_helper.prices_to_tab_delim_files(example_prices)

    # Assert
    assert expected['today_folder'] == today_folder
    assert expected['files'] == files
    assert expected['changed_prices'] == changed_prices


def test_get_normal_eta():
    # Arrange
    expected = datetime(2022, 1, 1, 13, 30)
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        result = flaskrp_helper.get_normal_eta('pf1')

    # Assert
    assert result == expected


def test_get_normal_eta_invalid_feed():
    # Arrange
    expected = None
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'normal_eta': datetime(2022, 1, 1, 13, 30)}}):

        # Act
        result = flaskrp_helper.get_normal_eta('pf2')

    # Assert
    assert result == expected


def test_get_feed_security_type():
    # Arrange
    expected = 'pf1 security type'
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'security_type': 'pf1 security type'}}):

        # Act
        result = flaskrp_helper.get_feed_security_type('pf1')

    # Assert
    assert result == expected


def test_get_feed_security_type_invalid_feed():
    # Arrange
    expected = None
    with mock.patch('flaskrp_helper.PRICING_FEEDS', {'pf1': {'security_type': 'pf1 security type'}}):

        # Act
        result = flaskrp_helper.get_feed_security_type('pf2')

    # Assert
    assert result == expected


def test_is_different_from_apx_price_different_source():
    # Arrange
    expected = True
    px = {'apx_symbol': 'ABC', 'apx_security_id': '123', 'source': 'BLOOMBERG', 'price': 123.456}
    pt = 'price'
    secs = pd.DataFrame({'pms_symbol': ['ABC'], 'pms_security_id': ['123']})
    apx_pxs = pd.DataFrame({'SecurityID': [123], 'PriceTypeID': [1], 'SourceID': [3006], 'PriceValue': 123.456})

    # Act
    result = flaskrp_helper.is_different_from_apx_price(px, pt, secs, apx_pxs)

    # Assert
    assert result == expected


def test_is_different_from_apx_price_different_price():
    # Arrange
    expected = True
    px = {'apx_symbol': 'ABC', 'apx_security_id': '123', 'source': 'BLOOMBERG', 'price': 123.456}
    pt = 'price'
    secs = pd.DataFrame({'pms_symbol': ['ABC'], 'pms_security_id': ['123']})
    apx_pxs = pd.DataFrame({'SecurityID': [123], 'PriceTypeID': [1], 'SourceID': [3004], 'PriceValue': 456.123})

    # Act
    result = flaskrp_helper.is_different_from_apx_price(px, pt, secs, apx_pxs)

    # Assert
    assert result == expected


def test_is_different_from_apx_price_same():
    # Arrange
    expected = False
    px = {'apx_symbol': 'ABC', 'apx_security_id': '123', 'source': 'BLOOMBERG', 'price': 123.456}
    pt = 'price'
    secs = pd.DataFrame({'pms_symbol': ['ABC'], 'pms_security_id': ['123']})
    apx_pxs = pd.DataFrame({'SecurityID': [123], 'PriceTypeID': [1], 'SourceID': [3004], 'PriceValue': 123.456})

    # Act
    result = flaskrp_helper.is_different_from_apx_price(px, pt, secs, apx_pxs)

    # Assert
    assert result == expected


def test_is_different_from_apx_price_sec_not_found():
    # Arrange
    expected = False
    px = {'apx_symbol': 'ABC', 'apx_security_id': '123', 'source': 'BLOOMBERG', 'price': 123.456}
    pt = 'price'
    secs = pd.DataFrame({'pms_symbol': ['ABC'], 'pms_security_id': ['999']})
    apx_pxs = pd.DataFrame({'SecurityID': [999], 'PriceTypeID': [1], 'SourceID': [3004], 'PriceValue': 123.456})

    # Act
    try:
        result = flaskrp_helper.is_different_from_apx_price(px, pt, secs, apx_pxs)

    # Assert
        assert False
    except Exception as e:
        assert True



