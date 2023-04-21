"""
End-to-end integration tests to test the full functionality the API endpoints.
To run these end-to-end tests: 
    -run cmd: cd C:\lw\kafka14\scripts
        (dir where this file is located)
    -run cmd: pytest test_restapi.py -v --ff -x
        (see pytest docs or run with -h for info on args)
    -also useful is to run "not slow" tests by adding '-m "not slow"'
    -see results
"""

import base64
import pytest
import requests

BASE_URL = 'http://WS215:5000/api'

def success_with_data(response):
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()["status"] == "success"
    assert len(response.json()["data"]) > 0

def success_with_no_data(response):
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()["status"] == "warning"
    assert response.json()["data"] is None


@pytest.mark.end2end
def test_get_price_by_date():
    # Send a GET request to the endpoint with a valid date
    response = requests.get(f'{BASE_URL}/pricing/price/20230420')
    # Verify that the response is successful
    success_with_data(response)


@pytest.mark.end2end
def test_get_price_by_date_warning():
    # Send a GET request to the endpoint with a date which has no prices
    response = requests.get(f'{BASE_URL}/pricing/price/19230103')
    # Verify that the response is warning with no data
    success_with_no_data(response)


# TODO: consider testing /api/pricing/price - load to APX


@pytest.mark.end2end
def test_post_pricing_attachment_no_files():
    price_date = '20220101'
    # Send a POST request to the endpoint without 'files' in payload
    payload = {'not_files': [
        {'name': 'CJTEST1.txt', 'binary_content': base64.b64encode(b'\x00\x01\x02\x03').decode()},
        {'name': 'CJTEST2.txt', 'binary_content': base64.b64encode(b'\x04\x05\x06\x07').decode()}
    ]}
    response = requests.post(f'{BASE_URL}/pricing/attachment/{price_date}', json=payload)
    # Verify that the response is error (HTTP status code 422)
    assert response.status_code == 422
    # Should get an error, because we must provide 'files'
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_post_pricing_attachment_empty_files():
    price_date = '20220101'
    # Send a POST request to the endpoint with empty files in payload
    payload = {'files': []}
    response = requests.post(f'{BASE_URL}/pricing/attachment/{price_date}', json=payload)
    # Verify that the response is successful, but with warning
    success_with_no_data(response)
    

@pytest.mark.end2end
def test_pricing_attachment():
    price_date = '20220101'
    # Send a POST request to the endpoint with valid payload data
    payload = {'files': [
        {'name': 'CJTEST1.txt', 'binary_content': base64.b64encode(b'\x00\x01\x02\x03').decode()},
        {'name': 'CJTEST2.txt', 'binary_content': base64.b64encode(b'\x04\x05\x06\x07').decode()}
    ]}
    response = requests.post(f'{BASE_URL}/pricing/attachment/{price_date}', json=payload)
    # Verify that the response is successful (HTTP status code 201)
    assert response.status_code == 201
    # Verify that the response contains the expected data
    assert response.json()['status'] == 'success'
    
    # Send a GET request to the endpoint with a valid date
    response = requests.get(f'{BASE_URL}/pricing/attachment/{price_date}')
    success_with_data(response)
    assert len(response.json()['data']) >= len(payload['files'])
    
    # Send a DELETE request to the endpoint with a valid date
    response = requests.delete(f'{BASE_URL}/pricing/attachment/{price_date}')
    # Verify that the response is successful (HTTP status code 200)
    assert response.status_code == 200
    # Verify that the response contains the expected data
    assert response.json()['status'] == 'success'

    # Send a GET request to the endpoint with a valid date
    response = requests.get(f'{BASE_URL}/pricing/attachment/{price_date}')
    # Should get a warning, because we tried getting but there should be nothing there to get
    success_with_no_data(response)
    

@pytest.mark.end2end
@pytest.mark.slow
def test_post_held_security_prices():
    # Send a POST request to the endpoint with valid payload data
    payload = {'sec_type': 'bond', 'price_date': '2023-01-04'}
    response = requests.post(f'{BASE_URL}/pricing/held-security-price', json=payload)
    # Verify that the response is successful
    success_with_data(response)


@pytest.mark.end2end
def test_get_pricing_audit_reason():
    # Send a GET request to the endpoint
    response = requests.get(f'{BASE_URL}/pricing/audit-reason')
    success_with_data(response)


@pytest.mark.end2end
def test_post_notification_subscription_missing_req_field():
    payload = {"email": "test@example.com"}  # missing feed_name
    response = requests.post(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_delete_notification_subscription_missing_req_field():
    payload = {"email": "test@example.com"}  # missing feed_name
    response = requests.delete(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_post_notification_subscription_invalid_email():
    payload = {"email": "not_an_email", "feed_name": "test_feed", "email_on_complete": True}
    response = requests.post(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_delete_notification_subscription_invalid_email():
    payload = {"email": "not_an_email", "feed_name": "test_feed", "email_on_complete": True}
    response = requests.delete(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_notification_subscription():
    payload = {"email": "cameronj@leithwheeler.com", "feed_name": "test_feed", "email_on_complete": True}
    response = requests.post(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 201
    assert response.json()["status"] == "success"

    response = requests.get(f'{BASE_URL}/pricing/notification-subscription')
    success_with_data(response)

    payload = {"email": "cameronj@leithwheeler.com", "feed_name": "test_feed"}
    response = requests.delete(f'{BASE_URL}/pricing/notification-subscription', json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.end2end
@pytest.mark.slow
def test_get_transactions_by_date():
    response = requests.get(f'{BASE_URL}/transaction/20230331')
    success_with_data(response)


@pytest.mark.end2end
@pytest.mark.slow
def test_get_transactions_by_date_warning():
    # Now try for a date which will have no txns
    response = requests.get(f'{BASE_URL}/transaction/19230210')
    # Should get a warning when no data is found 
    success_with_no_data(response)


@pytest.mark.end2end
def test_get_pricing_feed_status():
    response = requests.get(f'{BASE_URL}/pricing/feed-status')
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert "status" in response.json()
    # Depending on time of day / status of non-Prod connection, 
    # we may or may not be returned any data. Therefore, we want this test to pass
    # in either case:
    if response.json()["status"] == "success":
        assert len(response.json()["data"])
    elif response.json()["status"] == "warning":
        assert response.json()["data"] is None


@pytest.mark.end2end
def test_get_pricing_feed_status_by_date():
    response = requests.get(f'{BASE_URL}/pricing/feed-status/20230104')
    success_with_data(response)


@pytest.mark.end2end
def test_get_pricing_feed_status_by_date_warning():
    response = requests.get(f'{BASE_URL}/pricing/feed-status/19230104')
    print(response)
    if response.json()["data"] is not None:
        # If test is ran later in the day, feeds may have "delayed" status.
        # In this case, we should not use the generic success_with_no_data function,
        # but rather do our own assertions here for each feed in the response:
        for f in response.json()["data"]:
            # If ran before the "normal_eta" time, status may be none.
            # But if there is a status, it should be "delayed":
            if response.json()["data"][f]["status"] is not None:
                assert response.json()["data"][f]["status"] == "DELAYED"
    else:
        success_with_no_data(response)


@pytest.mark.end2end
def test_post_manual_pricing_security_no_lwid():
    lw_ids = ["LW001", "LW002"]
    # test post
    data = {
        "not_lw_id": lw_ids
    }
    response = requests.post(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_delete_manual_pricing_security_no_lwid():
    lw_ids = ["LW001", "LW002"]
    # test delete
    data = {
        "not_lw_id": lw_ids
    }
    response = requests.delete(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_post_manual_pricing_security_single_lwid():
    lw_id = "LW001"
    # test post
    data = {
        "lw_id": lw_id
    }
    response = requests.post(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    # Should get an error because the API should require a list of lw_ids
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_delete_manual_pricing_security_single_lwid():
    lw_id = "LW001"
    # test delete
    data = {
        "lw_id": lw_id
    }
    response = requests.delete(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    # Should get an error because the API should require a list of lw_ids
    assert response.status_code == 422
    assert response.json()['status'] == 'error'


@pytest.mark.end2end
def test_manual_pricing_security():
    # First get pre-existing list - will re-post it at the end
    # This is necessary because the endpoint is designed to delete the existing dataset 
    # when handling a POST request.
    response = requests.get(f'{BASE_URL}/pricing/manual-pricing-security')
    orig_lw_ids = []
    if response.json()["data"] is not None:
        orig_lw_ids = [d["lw_id"] for d in response.json()["data"]]

    lw_ids = ["LW001", "LW002"]
    # test post
    data = {
        "lw_id": lw_ids
    }
    response = requests.post(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 201
    
    # test get
    response = requests.get(f'{BASE_URL}/pricing/manual-pricing-security')
    success_with_data(response)
    assert len(response.json()["data"]) == len(lw_ids)
    
    # test delete
    response = requests.delete(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 200
    assert response.json()["status"] == "success"

    # test delete - this time it should produce a warning since there is nothing to delete
    response = requests.delete(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 200
    assert response.json()["status"] == "warning"

    # re-post original security list
    data = {
        "lw_id": orig_lw_ids
    }
    response = requests.post(f'{BASE_URL}/pricing/manual-pricing-security', json=data)
    assert response.status_code == 201
    

@pytest.mark.end2end
def test_post_pricing_audit_trail_no_audit_trail():
    # test post
    data = {
        "not_audit_trail": [
            {
                "lw_id": "LW001",
                "source": "bloomberg",
                "reason": "manual pricing 1",
                "comment": "this is a comment 1"
            },
            {
                "lw_id": "LW002",
                "source": "reuters",
                "reason": "manual pricing 2",
                "comment": "this is a comment 2"
            }
        ]
    }
    price_date = '20220101'
    response = requests.post(f'{BASE_URL}/pricing/audit-trail/{price_date}', json=data)
    assert response.status_code == 422
    assert response.json()["status"] == "error"
    

@pytest.mark.end2end
def test_post_pricing_audit_trail_single_audit_trail():
    # test post
    data = {
        "audit_trail": {
            "lw_id": "LW001",
            "source": "bloomberg",
            "reason": "manual pricing 1",
            "comment": "this is a comment 1"
        }
    }
    price_date = '20220101'
    response = requests.post(f'{BASE_URL}/pricing/audit-trail/{price_date}', json=data)
    # Should error, because audit_trail must be a list
    assert response.status_code == 422
    assert response.json()["status"] == "error"
    

@pytest.mark.end2end
def test_post_pricing_audit_trail_missing_req_field():
    # test post
    data = {
        "audit_trail": [
            {
                "lw_id": "LW001",
                "source": "bloomberg",
                "reason": "manual pricing 1",
                "comment": "this is a comment 1"
            },
            {
                "lw_id": "LW002",
                "source": "reuters",
                "reason": "manual pricing 2 *** missing a comment *** "
            }
        ]
    }
    price_date = '20220101'
    response = requests.post(f'{BASE_URL}/pricing/audit-trail/{price_date}', json=data)
    assert response.status_code == 422
    assert response.json()["status"] == "error"
    

@pytest.mark.end2end
def test_pricing_audit_trail():
    # test post
    data = {
        "audit_trail": [
            {
                "lw_id": "LW001",
                "source": "bloomberg",
                "reason": "manual pricing 1",
                "comment": "this is a comment 1"
            },
            {
                "lw_id": "LW002",
                "source": "reuters",
                "reason": "manual pricing 2",
                "comment": "this is a comment 2"
            }
        ]
    }
    price_date = '20220101'
    response = requests.post(f'{BASE_URL}/pricing/audit-trail/{price_date}', json=data)
    assert response.status_code == 201
    
    # test get
    response = requests.get(f'{BASE_URL}/pricing/audit-trail/{price_date}')
    success_with_data(response)
    assert len(response.json()["data"]) >= len(data["audit_trail"])


@pytest.mark.end2end
def test_pricing_column_config_no_columns():
    user_id = "testuser1"
    # test post
    data = {
        "not_columns": [
            {
                "name": "column1",
                "hidden": True
            },
            {
                "name": "column2",
                "hidden": False
            }
        ]
    }
    response = requests.post(f'{BASE_URL}/pricing/column-config/{user_id}', json=data)
    assert response.status_code == 422
    assert response.json()["status"] == "error"


@pytest.mark.end2end
def test_pricing_column_config_single_column():
    user_id = "testuser1"
    # test post
    data = {
        "columns": 
            {
                "name": "column1",
                "hidden": True
            }
    }
    response = requests.post(f'{BASE_URL}/pricing/column-config/{user_id}', json=data)
    # Should error because the endpoint requires a list of columns
    assert response.status_code == 422
    assert response.json()["status"] == "error"


@pytest.mark.end2end
def test_pricing_column_config_missing_req_field():
    user_id = "testuser1"
    # test post
    data = {
        "columns": [
            {
                "name": "column1",
                "hidden": True
            },
            {
                "name": "column2"
            }
        ]
    }
    response = requests.post(f'{BASE_URL}/pricing/column-config/{user_id}', json=data)
    assert response.status_code == 422
    assert response.json()["status"] == "error"
    

@pytest.mark.end2end
def test_pricing_column_config():
    user_id = "testuser1"
    # test post
    data = {
        "columns": [
            {
                "column_name": "column1",
                "is_hidden": True
            },
            {
                "column_name": "column2",
                "is_hidden": False
            }
        ]
    }
    response = requests.post(f'{BASE_URL}/pricing/column-config/{user_id}', json=data)
    assert response.status_code == 201
    
    # test get
    response = requests.get(f'{BASE_URL}/pricing/column-config/{user_id}')
    assert response.status_code == 200

