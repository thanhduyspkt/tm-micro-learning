import time
import unittest
import uuid

from core_api import create_customer, create_account, create_pib_async, fetch_posting_instruction, fetch_balances_live
from environment import POSTING_CLIENT_ID, PIB_STATUS_ACCEPTED, PIB_STATUS_REJECTED, POSTING_CREATED_TOPIC, \
    POSTING_RESPONSE_TOPIC
from kafka_api import create_pib_kafka_async
from kafka_kk import KafkaUtils


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

    # topic: vault.core.postings.requests.v1
    def test_create_posting_via_kafka_with_high_priority(self):
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
        })
        time.sleep(3)
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=self.account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_ACCEPTED, postings_result['posting_instruction_batches'][0]['status'])

    # supported_denomination=[USD]
    # send posting denomination SGD
    # there's no pre posting code hook to validate denomination
    # TM will accept this posting
    def test_create_posting_with_no_pre_posting_hook(self):
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": '709',
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
        account_id = response.json()['id']

        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': 'deposits-core-kk',
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "3",
                            "denomination": "SGD",
                            "target_account": {
                                "account_id": account_id
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_ACCEPTED, postings_result['posting_instruction_batches'][0]['status'])

    # raise error in pre_posting hook
    # Rest Posting API returns status 200
    # Fetch Posting response via Rest API -> status is REJECTED
    # Consume event from posting created topic -> status is REJECTED
    # No event from the response topic
    def test_create_posting_with_insufficient_fund(self):
        kafka_utils = KafkaUtils(None, 10)
        kafka_utils.start_consuming_messages_from_topics([POSTING_CREATED_TOPIC, "integration.postings_api.deposits-core-kkk-high.response", "integration.postings_api.deposits-core-kkk-low.response"], None)
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": '742',
                "stakeholder_ids": [
                    self.customer_id
                ],
                "instance_param_vals": {
                    "internal_account": self.internal_account_id,
                    "opening_bonus": "20.0",
                    "interest_rate": "0.05",
                    # "monthly_withdrawal_fee": "1",
                    # "minimum_monthly_withdrawal": "0"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_OPEN"
            }
        })

        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': "deposits-core-kkk",
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": account_id
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
        postings_result = fetch_posting_instruction(client_batch_id=f"{client_batch_id}_1", account_id=account_id)
        self.assertEqual(1, len(postings_result['posting_instruction_batches']))
        self.assertEqual(PIB_STATUS_REJECTED, postings_result['posting_instruction_batches'][0]['status'])

        # message_map = kafka_utils.get_messages_map()
        response_messages = kafka_utils.get_messages_from_posting_created_topic(POSTING_CREATED_TOPIC, f"{client_batch_id}_1")
        self.assertEqual(1, len(response_messages))
        self.assertEqual(PIB_STATUS_REJECTED, response_messages[0]['posting_instruction_batch']['status'])

    # credit 30 USD to main account
    # 1% of credit amount (0.3 USD) will be sent to CASHBACK address
    def test_create_posting_with_cashback(self):
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": '743',
                "stakeholder_ids": [
                    self.customer_id
                ],
                "instance_param_vals": {
                    "internal_account": self.internal_account_id,
                    "opening_bonus": "20.0",
                    "interest_rate": "0.05",
                    # "monthly_withdrawal_fee": "1",
                    # "minimum_monthly_withdrawal": "0"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_OPEN"
            }
        })

        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': "deposits-core-kkk",
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'inbound_hard_settlement': {
                            "amount": "30",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": account_id
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
        response = fetch_balances_live(account_id=account_id, address='CASHBACK')
        self.assertEqual(200, response.status_code)
        balances = response.json()['balances']
        self.assertEqual(1, len(balances))
        self.assertEqual('0.3', balances[0]['amount'])
