import time
import unittest
import uuid

from core_api import create_customer, create_account, create_pib_async, fetch_posting_instruction
from environment import POSTING_CLIENT_ID, PIB_STATUS_ACCEPTED
from kafka_api import create_pib_kafka_async


class TestPostings(unittest.TestCase):
    def setUp(self):
        self.internal_account_id = "1"
        self.product_id = 'simple_contract_kk'

        email = f"kk_{str(uuid.uuid4())}@gmail.com"
        response = create_customer({
            'request_id': str(uuid.uuid4()),
            'customer': {
                'status': 'CUSTOMER_STATUS_ACTIVE',
                'identifiers': [
                    {
                        'identifier_type': 'IDENTIFIER_TYPE_EMAIL',
                        'identifier': email
                    }
                ],
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MISS",
                    "first_name": "Maurice",
                    "middle_name": "",
                    "last_name": "Lang",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "USA",
                    "email_address": email,
                    "mobile_phone_number": "",
                    "home_phone_number": "",
                    "business_phone_number": "",
                    "contact_method": "CUSTOMER_CONTACT_METHOD_EMAIL",
                    "country_of_residence": "USA",
                    "country_of_taxation": "USA",
                    "accessibility": "CUSTOMER_ACCESSIBILITY_AUDIO",
                    "external_customer_id": "222"
                },
                "additional_details": {}
            }
        })

        self.assertEqual(200, response.status_code)
        self.customer_id = response.json()['id']
        print('----------- SETUP ------------')
        print(f"customer_id={self.customer_id}")

        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_id": self.product_id,
                "stakeholder_ids": [
                    self.customer_id
                ],
                "instance_param_vals": {
                    "internal_account": self.internal_account_id,
                    "opening_bonus": "20.0",
                    "interest_rate": "0.05",
                    "monthly_withdrawal_fee": "1",
                    "minimum_monthly_withdrawal": "0"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_OPEN"
            }
        })

        self.assertEqual(200, response.status_code)
        self.account_id = response.json()['id']
        print(f"account_id={self.account_id}")
        print('----------- END ------------')

    # If sending posting without RequestId
    # the request is rejected
    def test_create_posting_with_no_request_id(self):
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(400, response.status_code)

    # 'api_type': 'POSTINGS_API_TYPE_LOW_PRIORITY'
    def test_create_posting_with_low_priority(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'api_type': 'POSTINGS_API_TYPE_LOW_PRIORITY',
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=self.account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_ACCEPTED, postings_result['posting_instruction_batches'][0]['status'])

    # 'api_type': 'POSTINGS_API_TYPE_HIGH_PRIORITY'
    def test_create_posting_with_high_priority(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'api_type': 'POSTINGS_API_TYPE_HIGH_PRIORITY',
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=self.account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_ACCEPTED, postings_result['posting_instruction_batches'][0]['status'])

    # topic: vault.core.postings.requests.low_priority.v1
    # note: the client id must have register response_topic_low_priority
    def test_create_posting_via_kafka_with_low_priority(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': 'deposits-core-kk',
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "3",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        }, low_priority=True)
        time.sleep(3)
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=self.account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_ACCEPTED, postings_result['posting_instruction_batches'][0]['status'])