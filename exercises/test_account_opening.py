import datetime
import json
import time
import unittest
import uuid

from environment import POSTING_CLIENT_ID, POSTING_RESPONSE_TOPIC, POSTING_CREATED_TOPIC
from kafka_kk import KafkaUtils
from core_api import create_account, create_customer, close_account, activate_account, create_pib_async, \
    pending_closure_account, cancel_account, fetch_posting_instruction, update_parameters, trigger_post_activation_code_hooks, trigger_close_code_hooks, get_schedules_status
from kafka_api import create_pib_kafka_async

class TestTMAccountOpening(unittest.TestCase):
    def setUp(self):
        self.internal_account_id = "1"
        self.product_id = 'simple_contract_kk'
        self.failed_product_id = 'kk_product_failed'
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

    def test_active_account_account(self):
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
        account_id = response.json()['id']
        print(response.content)
        print(account_id)

        # response = close_account(account_id, str(uuid.uuid4()))
        # print(response.content)
        # self.assertEqual(400, response.status_code)
        # error_message = json.loads(response.content)
        # self.assertEqual('invalid request to update Account', error_message['message'])

    # Cannot close an account with status PENDING
    def test_close_pending_account(self):
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
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })

        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        response = close_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(400, response.status_code)
        error_message = json.loads(response.content)
        self.assertEqual('invalid request to update Account', error_message['message'])

    # cannot directly close an account with OPEN status
    def test_close_active_account(self):
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
                "status": "ACCOUNT_STATUS_ACTIVE"
            }
        })

        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)

        response = activate_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code, f"activate account should be success")

        response = create_pib_async({
            'request_id': str(uuid.uuid4()),
            'posting_instruction_batch': {
                'client_batch_id': str(uuid.uuid4()),
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_hard_settlement': {
                            "amount": "20",
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
        time.sleep(10)
        response = close_account(account_id, str(uuid.uuid4()))
        # will get the error message invalid request to update Account
        print(response.content)
        self.assertEqual(400, response.status_code)

    # update status to ACCOUNT_STATUS_PENDING
    # and zero all account balances
    # then we can update status to ACCOUNT_STATUS_CLOSED
    def test_close_pending_closure_account(self):
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
                "status": "ACCOUNT_STATUS_ACTIVE"
            }
        })

        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)

        response = activate_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code, f"activate account should be success")

        # this is a must to zero balances of the account
        # if not, will get the error message account close has been requested but balance is not zero
        response = create_pib_async({
            'request_id': str(uuid.uuid4()),
            'posting_instruction_batch': {
                'client_batch_id': str(uuid.uuid4()),
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_hard_settlement': {
                            "amount": "20",
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
        time.sleep(10)

        # the ordering of zero balance and update account status to pending closure doesn't matter
        response = pending_closure_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code)

        response = close_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code)

    # cannot cancel an active account
    def test_cancel_an_active_account(self):
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
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        response = activate_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code)

        response = cancel_account(account_id, str(uuid.uuid4()))
        # will get the error message invalid request to update Account
        print(response.content)
        self.assertEqual(400, response.status_code)

    # allow cancel an account only if account status is PENDING
    def test_cancel_an_pending_account(self):
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
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        response = cancel_account(account_id, str(uuid.uuid4()))
        print(response.content)
        self.assertEqual(200, response.status_code)

    # all postings are rejected if the target account status is PENDING
    def test_submit_posting_pending_account(self):
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
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)

        client_batch_id = str(uuid.uuid4())
        filter_func = lambda msg: (msg['posting_instruction_batch']['client_batch_id'] == f"{client_batch_id}_1" or
                                   msg['posting_instruction_batch']['client_batch_id'] == f"{client_batch_id}_2")
        kafka_utils = KafkaUtils(topic=POSTING_CREATED_TOPIC, timeout=10)
        kafka_utils.start_consuming_messages(filter_func)

        response = create_pib_async(
            {
                'request_id': str(uuid.uuid4()),
                'posting_instruction_batch': {
                    'client_batch_id': f"{client_batch_id}_1",
                    'client_id': POSTING_CLIENT_ID,
                    'posting_instructions': [
                        {
                            'client_transaction_id': str(uuid.uuid4()),
                            'outbound_hard_settlement': {
                                "amount": "20",
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
            }
        )
        self.assertEqual(200, response.status_code)

        response = create_pib_async(
            {
                'request_id': str(uuid.uuid4()),
                'posting_instruction_batch': {
                    'client_batch_id': f"{client_batch_id}_2",
                    'client_id': POSTING_CLIENT_ID,
                    'posting_instructions': [
                        {
                            'client_transaction_id': str(uuid.uuid4()),
                            'inbound_hard_settlement': {
                                "amount": "20",
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
            }
        )
        self.assertEqual(200, response.status_code)

        messages = kafka_utils.get_messages()
        self.assertEqual(2, len(messages))
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED',
                         messages[0]['posting_instruction_batch']['status'])
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED',
                         messages[1]['posting_instruction_batch']['status'])

    # when an account is opened with PENDING status
    # there's a few events are stream out
    # vault.api.v1.accounts.account.created
    # vault.core_api.v1.accounts.account.events
    # vault.core_api.v2.accounts.account.events
    def test_consume_events_when_account_status_pending(self):
        kafka_utils = KafkaUtils(topic=None, timeout=10)
        kafka_utils.start_consuming_messages_from_topics(
            ['vault.api.v1.accounts.account.created', 'vault.core_api.v1.accounts.account.events', 'vault.core_api.v2.accounts.account.events'],
            lambda msg: True,
        )
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
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)
        messages_map = kafka_utils.get_messages_map()
        account_created_events = messages_map['vault.api.v1.accounts.account.created']
        self.assertEqual(1, len([a for a in account_created_events if a['account']['id'] == account_id]), "expect the message from vault.api.v1.accounts.account.created")
        account_events = messages_map['vault.core_api.v1.accounts.account.events']
        self.assertEqual(1, len([a for a in account_events if a['account_created']['account']['id'] == account_id]), "expect the message from vault.core_api.v1.accounts.account.events")
        account_events_v2 = messages_map['vault.core_api.v2.accounts.account.events']
        self.assertEqual(1, len(account_events_v2), "expect the message from vault.core_api.v2.accounts.account.events")

#  The activation and closure account updates are automatically queued when an account status changes to ACCOUNT_STATUS_OPEN and ACCOUNT_STATUS_PENDING_CLOSURE, respectively

    # post activation code hooks fails -> No opening bonus PIB created
    # no scheduled created
    # but the sub-sequence PIBs are still able to send this account
    def test_post_activate_code_hook_failed_then_send_pib(self):
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_id": self.failed_product_id,
                "stakeholder_ids": [
                    self.customer_id
                ],
                "instance_param_vals": {
                    "internal_account": self.internal_account_id,
                    "opening_bonus": "21.0",
                    "interest_rate": "0.05",
                    "monthly_withdrawal_fee": "1",
                    "minimum_monthly_withdrawal": "0"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)
        response = activate_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)

        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        kafka_utils =  KafkaUtils(None, timeout=20)
        kafka_utils.start_consuming_messages_from_topics([POSTING_CREATED_TOPIC], None)
        create_pib_kafka_async({
            'request_id': f"{request_id}_1",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
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
        response = create_pib_async({
            'request_id': f"{request_id}_2",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "1",
                            "denomination": "USD",
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
        message_map = kafka_utils.get_messages_map()
        message_from_posting_created_topic = message_map[POSTING_CREATED_TOPIC]
        self.assertEqual(2, len(message_from_posting_created_topic))

    # activation code hook fails because opening bonus is odd (21.0)
    # update opening bonus 20.0
    # re-trigger the activation code hooks
    # activation code hook is still failed, it keeps using the 21.0
    # my assumption is the activation code hooks took a snapshot, when re-triggering, it will use that snapshot to execute the code
    def test_rerun_post_activation_code_hooks_that_was_failed(self):
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "691",
                "stakeholder_ids": [
                    self.customer_id
                ],
                "instance_param_vals": {
                    "internal_account": self.internal_account_id,
                    "opening_bonus": "21.0",
                    "interest_rate": "0.05",
                    "monthly_withdrawal_fee": "1",
                    "minimum_monthly_withdrawal": "0"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_PENDING"
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        print(account_id)
        response = activate_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)

        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        # kafka_utils = KafkaUtils(None, timeout=20)
        # kafka_utils.start_consuming_messages_from_topics([POSTING_CREATED_TOPIC], None)
        create_pib_kafka_async({
            'request_id': f"{request_id}_1",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'inbound_hard_settlement': {
                            "amount": "10",
                            "denomination": "USD",
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
        response = create_pib_async({
            'request_id': f"{request_id}_2",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'inbound_hard_settlement': {
                            "amount": "1",
                            "denomination": "USD",
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
        pib_response = fetch_posting_instruction(client_batch_id=None, account_id=account_id)
        self.assertEqual(2, len(pib_response['posting_instruction_batches']))

#         fix the activation code hooks
        response = update_parameters(account_id=account_id, request_id=str(uuid.uuid4()), instance_param_vals={
            'opening_bonus': '20.0'
        })
        self.assertEqual(200, response.status_code)
        time.sleep(3)
#         wait until the update parameters request completed

        response = trigger_post_activation_code_hooks(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        pib_response = fetch_posting_instruction(client_batch_id=None, account_id=account_id)
        self.assertEqual(3, len(pib_response['posting_instruction_batches']))

    # "cannot supply an opening timestamp for a non-open account"
    def test_create_pending_account_with_opening_timestamp(self):
        current_time = datetime.datetime.utcnow()
        # Format it to ISO 8601 format with microseconds and 'Z'
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
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
                "status": "ACCOUNT_STATUS_PENDING",
                "opening_timestamp": opening_timestamp,
            }
        })

        # "cannot supply an opening timestamp for a non-open account"
        self.assertEqual(400, response.status_code)

    def test_create_open_account_with_opening_timestamp(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)

    # A set of denominations accessible from within the Smart Contract:
    #
    # - If the Product Version specifies supported denominations, then this set must be a subset of those (if none are specified here then the field will be set to the Product Version's supported denominations).
    # - If the Product Version does not specify denominations, then it will be set to the permitted_denominations defined in values.yaml
    #
    # The permitted_denominations field on the account resource is not used when validating incoming posting instructions' denominations. To validate that the denominations used by incoming posting instructions are in this list, the check must be implemented in the pre_posting_hook of the Smart Contract of the customer account that the posting instruction is targeting. This field cannot be updated.
    # https://docs.thoughtmachine.net/vault-core/5-3/EN/api/core_api/?resultIndex=4&query=opening_timestamp#Accounts-Account
    def test_supported_denominations(self):
        pass

    # vault will validate min, max of instance parameters
    # max interest_rate = 1
    # vault will reject creating an account with interest rate is 2
    def test_vault_validate_instance_parameters_min_max(self):
        response = create_account(
            {
                'request_id': str(uuid.uuid4()),
                "account": {
                    "product_id": self.product_id,
                    "stakeholder_ids": [
                        self.customer_id
                    ],
                    "instance_param_vals": {
                        "internal_account": self.internal_account_id,
                        "opening_bonus": "20.0",
                        "interest_rate": "2",
                        "monthly_withdrawal_fee": "1",
                        "minimum_monthly_withdrawal": "0"
                    },
                    "details": {},
                    "status": "ACCOUNT_STATUS_OPEN",
                }
            }
        )
        self.assertEqual(400, response.status_code)

    # update status to PENDING_CLOSURE, the account schedulers are still enabled
    # using schedules:getBatch to get schedule status
    def test_pending_closure_account_with_schedules(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        # wait for the account to be active
        time.sleep(3)

        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)

        # query schedule for this account, they're still enabled for sure
        print(f"check schedules of the account {account_id}")
        schedule_status = get_schedules_status(account_id)
        disable_schedules = [k for k, v in schedule_status.items() if v != 'SCHEDULE_STATUS_ENABLED']
        self.assertEqual(0, len(disable_schedules), "should have no disabled schedule")


    # when the account is pending closure
    # close_code will be triggered
    # it will all remaining amount to internal account
    # we can close the account right after that
    def test_close_code_hook_when_pending_closure_account(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "702",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        # wait for the account to be active
        time.sleep(3)
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(1, len(pib_response['posting_instruction_batches']))

        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(2, len(pib_response['posting_instruction_batches']))

        response = close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)

    # when close_code hook fails
    # this account status is pending closure
    # still accept postings
    def test_close_code_hook_failed_when_pending_closure_account(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "703",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        # wait for the account to be active
        time.sleep(3)
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(1, len(pib_response['posting_instruction_batches']))

        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        # close code will be failed, not clearing posting executing
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(1, len(pib_response['posting_instruction_batches']))

        response = close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(400, response.status_code)

        request_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': f"{request_id}_2",
            'posting_instruction_batch': {
                'client_batch_id': f"{request_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{request_id}_2",
                        'inbound_hard_settlement': {
                            "amount": "1",
                            "denomination": "USD",
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
        # still accept posting
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(2, len(pib_response['posting_instruction_batches']))

    # can re-trigger the failed close_code using /account-updates API
    # close_code will not use the snapshot like activation_code
    # once close_code finished and all balances are zero out
    # we can close the account
    def test_close_code_hook_failed_when_pending_closure_account_then_re_execute(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "703",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        # wait for the account to be active
        time.sleep(3)
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(1, len(pib_response['posting_instruction_batches']))

        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        # close code will be failed, not clearing posting executing
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(1, len(pib_response['posting_instruction_batches']))

        response = close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(400, response.status_code)

        request_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': f"{request_id}_2",
            'posting_instruction_batch': {
                'client_batch_id': f"{request_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{request_id}_2",
                        'inbound_hard_settlement': {
                            "amount": "1",
                            "denomination": "USD",
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
        # still accept posting
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(2, len(pib_response['posting_instruction_batches']))

        response = trigger_close_code_hooks(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)
        pib_response = fetch_posting_instruction(None, account_id)
        self.assertEqual(3, len(pib_response['posting_instruction_batches']))

        response = close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)

    # once account status update to pending closure
    # TM will automatically queue an account update for deactivation hook
    # the stream topic vault.core_api.v1.accounts.account_update.events will tell us the status of deactivation hook
    # status is ACCOUNT_UPDATE_STATUS_REJECTED
    def test_detect_deactivation_code_hook_failed(self):
        kafka_utils = KafkaUtils(None, timeout=10)
        kafka_utils.start_consuming_messages_from_topics(['vault.core_api.v1.accounts.account_update.events'], None)
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "703",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        # wait for the account to be active
        time.sleep(3)
        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        message_map = kafka_utils.get_messages_map()
        messages = message_map['vault.core_api.v1.accounts.account_update.events']
        rejected_deactivation_hook_events = [m for m in messages if m.get('account_update_updated') is not None and m['account_update_updated']['account_update']['status'] == 'ACCOUNT_UPDATE_STATUS_REJECTED' and m['account_update_updated']['account_update'].get('closure_update') is not None]
        self.assertEqual(1, len(rejected_deactivation_hook_events))

    # account is active
    # cannot directly the account
    # get error code 400, msg: {"violation_type":"BLOCKED_BY_ACCOUNT_STATUS","metadata":{"account.status":"ACCOUNT_STATUS_OPEN"}
    def test_directly_close_open_account(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "708",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']
        time.sleep(3)
        response = close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(400, response.status_code)

    # account status OPEN -> PENDING CLOSURE -> CLOSED
    # all schedules are disabled
    def test_close_account_with_schedules(self):
        current_time = datetime.datetime.utcnow()
        opening_timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        response = create_account({
            'request_id': str(uuid.uuid4()),
            "account": {
                "product_version_id": "708",
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
                "status": "ACCOUNT_STATUS_OPEN",
                "opening_timestamp": opening_timestamp,
            }
        })
        self.assertEqual(200, response.status_code)
        account_id = response.json()['id']

        # wait for the account to be active
        time.sleep(3)

        response = pending_closure_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)

        close_account(account_id, str(uuid.uuid4()))
        self.assertEqual(200, response.status_code)
        time.sleep(3)

        # query schedule for this account, they're still enabled for sure
        print(f"check schedules of the account {account_id}")
        schedule_status = get_schedules_status(account_id)
        disable_schedules = [k for k, v in schedule_status.items() if v == 'SCHEDULE_STATUS_DISABLED']
        self.assertEqual(3, len(disable_schedules), "should have no enabled schedule")



