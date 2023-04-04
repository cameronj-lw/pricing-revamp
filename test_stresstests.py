"""
Stress test to simulate multiple users calling the API at once.
To run this stress test: 
    -run cmd: locust -f C:\lw\kafka14\scripts\test_stresstests.py LWSecurityPricingUser --host=http://WS215:5000
        (update file and host as needed)
    -go to localhost:8089, you should see a locust interface
    -choose desired number of users in the "swarm", and spawn rate
    -monitor stats
"""

from locust import HttpUser, TaskSet, task

import json

class UserBehavior(TaskSet):
    @task
    def post_tests(self):
        self.client.post("/api/pricing/held-security-price"
                            , data=json.dumps({
                            "price_date": "20230331",
                            "sec_type": "bond"
                            })
                            , headers={'Content-Type':'application/json'}
                        )
        

class LWSecurityPricingUser(HttpUser):
    tasks = [UserBehavior]