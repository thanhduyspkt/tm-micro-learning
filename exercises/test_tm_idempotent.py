import json
import unittest
import uuid

from core_api import create_pib_async, create_account, create_customer, fetch_posting_instruction
from environment import POSTING_CLIENT_ID, POSTING_RESPONSE_TOPIC, POSTING_CREATED_TOPIC
from kafka_api import create_pib_kafka_async
from kafka_kk import KafkaUtils


class TestTMIdempotent(unittest.TestCase):

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

    # Not having the same behavior with HTTP posting
    # send identical requests to posting request topic
    # 2 messages are consumed from the posting response topic
    # consumer has to handle idempotency by checking created_request_id
    def test_create_pib_via_kaka_twice_then_consume_posting_response_from_response_topic_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        filter_message = lambda message: message[
                                             'client_batch_id'] == f"{client_batch_id}_1"
        kafka_utils = KafkaUtils(POSTING_RESPONSE_TOPIC, 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
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

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "3",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        messages = kafka_utils.get_messages()
        self.assertEqual(2, len(messages),
                         f"2 posting messages are created in {POSTING_RESPONSE_TOPIC}")

    # Not having the same behavior with HTTP posting
    # send identical requests to posting request topic
    # 2 messages are consumed from vault.api.v1.postings.posting_instruction_batch.created
    # consumer has to handle idempotency by checking created_request_id
    def test_create_pib_via_kaka_twice_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
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

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "VND",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        messages = kafka_utils.get_messages()
        self.assertEqual(2, len(messages), "2 posting messages are created in vault.api.v1.postings.posting_instruction_batch.created")

    # 1st posting is sent via kafka api
    # 2nd posting is sent via http api with the same request_id
    # TM only capture the first one
    def test_create_pib_twice_both_channels_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
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
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")

    # same request_id
    # same request body
    # TM only capture the first one
    def test_create_pib_via_http_twice_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())
        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
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
        self.assertEqual(200, response.status_code)

        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': client_tx_id,
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")

    # the same request_id
    # even the client_batch_id is different in 2 requests
    # TM only capture the first one
    def test_create_pibs_via_http_with_the_same_request_id_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        response = create_pib_async({
            'request_id': request_id,
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
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")

    # 1st posting is rejected via kafka api
    # 2nd posting with same request_id via http api, although it's a valid posting, it is not captured by TM
    # only one message is stream out to posting response topic
    def test_create_rejected_pibs_via_kafka_then_via_http_with_the_same_request_id_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        filter_message = lambda message: message['client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils(POSTING_RESPONSE_TOPIC, 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "VND",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": 'sadasd11',
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED', messages[0]['status'])

    # 1st posting is rejected via http api
    # 2nd posting with same request_id via kafka api, although it's a valid posting, it is not captured by TM
    # only one message is stream out to posting response topic
    def test_create_rejected_pibs_via_http_then_via_kafka_with_the_same_request_id_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        filter_message = lambda message: message['client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils(POSTING_RESPONSE_TOPIC, 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "VND",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": 'sadasd11',
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED', messages[0]['status'])

    # create 2 PIBs via Kafka api with the same request_id
    # 2 rejected postings are stream out to posting response topic
    def test_create_rejected_pibs_via_kafka_with_the_same_request_id_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        filter_message = lambda message: message['client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils(POSTING_RESPONSE_TOPIC, 10)
        kafka_utils.start_consuming_messages(filter_message)

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "VND",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": 'sadasd11',
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })

        messages = kafka_utils.get_messages()
        self.assertEqual(2, len(messages), "Only one posting instruction response is expected")
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED', messages[0]['status'])

    # this test case is failed
    # not sure why, but the rejected postings are not stream out to the response topic
    # instead, the messages are stream out to vault.api.v1.postings.posting_instruction_batch.created topic
    def test_create_rejected_pibs_with_the_same_request_id_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        # filter_message = lambda message: message['client_batch_id'] == f"{client_batch_id}_1" or \
        #                                  message['client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils(POSTING_RESPONSE_TOPIC, 10)
        kafka_utils.start_consuming_messages_from_topics([POSTING_RESPONSE_TOPIC, POSTING_CREATED_TOPIC], None)


        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_1",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "VND",
                            "target_account": {
                                "account_id": self.account_id
                            },
                            "internal_account_id": '1',
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        # print(response.content)
        self.assertEqual(200, response.status_code)

        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        messages_map = kafka_utils.get_messages_map()
        messages_from_posting_response = messages_map[POSTING_RESPONSE_TOPIC]
        self.assertEqual(1, len(messages_from_posting_response), "Only one posting instruction response is expected")
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED', messages_from_posting_response[0]['status'])

        messages_from_posting_created = [x for x in messages_map[POSTING_CREATED_TOPIC] if x['posting_instruction_batch']['client_batch_id'] == f"{client_batch_id}_1" or x['posting_instruction_batch']['client_batch_id'] == f"{client_batch_id}_2"]
        self.assertEqual(1, len(messages_from_posting_created), "Only one posting instruction response is expected")
        self.assertEqual('POSTING_INSTRUCTION_BATCH_STATUS_REJECTED', messages_from_posting_created[0]['posting_instruction_batch']['status'])
        print(json.dumps(messages_from_posting_created[0], indent=2))

    # 2 requests with different request_id
    # TM captures both of them
    def test_create_separated_pibs_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        client_tx_id = str(uuid.uuid4())

        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        response = create_pib_async({
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
                                "account_id": self.account_id
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)

        response = create_pib_async({
            'request_id': f"{request_id}_2",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{client_tx_id}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        messages = kafka_utils.get_messages()
        self.assertEqual(2, len(messages), "Only one posting instruction response is expected")

    # 2 different customers info
    # but using the same request_id
    # TM will not create a new customer for 2nd request, only return the customer created by the 1st request
    def test_http_create_different_customers_but_same_request_id(self):
        request_id = str(uuid.uuid4())

        first_customer_info = {
            'request_id': request_id,
            'customer': {
                'id': str(uuid.uuid4()),
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }

        response = create_customer(first_customer_info)
        self.assertEqual(200, response.status_code, 'first request should be accepted')

        second_customer_info = {
            'request_id': request_id,
            'customer': {
                'id': 'my-random-id-12345',
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }
        response = create_customer(second_customer_info)
        self.assertEqual(200, response.status_code,
                         'second request is ACCEPTED, TM will check the request_id already exists, it returns the created customer, ignore the submited customer')

    # create the same customer twice
    # TM will accept the 2nd request, it returns the customer created by the first request
    def test_http_create_customer_idempotent_with_the_same_request_id(self):
        request_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())

        customer_info = {
            'request_id': request_id,
            'customer': {
                'id': customer_id,
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }
        response = create_customer(customer_info)
        self.assertEqual(200, response.status_code, 'first request should be accepted')

        response = create_customer(customer_info)
        self.assertEqual(200, response.status_code,
                         'second request is still accepted, with the same request_id, this API is idempotent')

    # with the same customer info
    # and different request_id
    # TM will reject the 2nd request, says the customer already exists
    def test_http_create_customers_if_request_id_is_different_and_the_same_customer_id(self):
        request_id = str(uuid.uuid4())

        customer_id = str(uuid.uuid4())
        first_customer_info = {
            'request_id': request_id,
            'customer': {
                'id': customer_id,
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }
        response = create_customer(first_customer_info)
        self.assertEqual(200, response.status_code, 'first request should be accepted')

        request_id_new = str(uuid.uuid4())
        second_customer_info = {
            'request_id': request_id_new,
            'customer': {
                'id': customer_id,
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }
        response = create_customer(second_customer_info)
        self.assertEqual(409, response.status_code,
                            'second request should be rejected, TM knows that customer ID already exists')

    # with different request_id
    # even 2 postings are identical
    # TM is not idempotent, TM still creates 2 separate posting resources
    def test_create_two_identical_postings_but_different_request_id(self):
        client_batch_id = str(uuid.uuid4())
        transaction_id = str(uuid.uuid4())

        request_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': transaction_id,
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}",
                                                                 self.account_id)
        self.assertEqual(1, len(posting_instruction_response['posting_instruction_batches']),
                         f"{client_batch_id} should be created in TM")

        request_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': f"{request_id}_1",
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': transaction_id,
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        })
        self.assertEqual(200, response.status_code)
        posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}",
                                                                 self.account_id)
        self.assertEqual(2, len(posting_instruction_response['posting_instruction_batches']),
                         f"{client_batch_id} should be created in TM")

    # with the same request_id
    # the http async posting is idempotent
    # it only creates one posting instruction batch in TM account
    def test_http_async_posting_with_the_same_request_id_and_client_id(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
        response = create_pib_async({
            'request_id': request_id,
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
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        }
        )
        self.assertEqual(200, response.status_code)
        posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}_1",
                                                                 self.account_id)
        self.assertEqual(1, len(posting_instruction_response['posting_instruction_batches']),
                         f"{client_batch_id}_1 should be created in TM")

        create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': POSTING_CLIENT_ID,
                'posting_instructions': [
                    {
                        'client_transaction_id': str(uuid.uuid4()),
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        }
        )
        self.assertEqual(200, response.status_code)
        posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}_2",
                                                                 self.account_id)
        self.assertEqual(0, len(posting_instruction_response['posting_instruction_batches']),
                         f"{client_batch_id}_2 should not be created in TM")

    # with the same request_id, but different client_id
    # it only creates one posting instruction batch into TM
    # it's so confused ???
    def test_http_async_posting_with_the_same_request_id_but_different_client_id(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())

        filter_message = lambda message: message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_1" or \
                                         message['posting_instruction_batch'][
                                             'client_batch_id'] == f"{client_batch_id}_2"
        kafka_utils = KafkaUtils('vault.api.v1.postings.posting_instruction_batch.created', 10)
        kafka_utils.start_consuming_messages(filter_message)

        response = create_pib_async({
            'request_id': request_id,
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
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        }
        )
        self.assertEqual(200, response.status_code)
        posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}_1",
                                                                 self.account_id)
        self.assertEqual(1, len(posting_instruction_response['posting_instruction_batches']),
                         f"{client_batch_id}_1 should be created in TM")

        response = create_pib_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_2",
                'client_id': 'deposits-core-old',
                'posting_instructions': [
                    {
                        'client_transaction_id': f"{str(uuid.uuid4())}_2",
                        'outbound_authorisation': {
                            "amount": "10",
                            "denomination": "USD",
                            "target_account": {
                                "account_id": self.account_id,
                            },
                            "internal_account_id": self.internal_account_id,
                        },
                        "instruction_details": {}
                    }
                ]
            }
        }
        )
        self.assertEqual(200, response.status_code)
        # posting_instruction_response = fetch_posting_instruction(f"{client_batch_id}_2",
        #                                                           self.account_id)
        #  this is so confused
        # self.assertEqual(0, len(posting_instruction_response['posting_instruction_batches']),
        #                  f"{client_batch_id}_2 should not be created in TM")

        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages))

    # request_id is isolated on each API
    # create customer and account with the same request_id
    # a new customer is created
    # a new account is created
    # this means request_id scope with a specific resource only
    def test_http_create_customer_and_account_with_the_same_request_id(self):
        request_id = str(uuid.uuid4())

        customer_info = {
            'request_id': request_id,
            'customer': {
                'id': str(uuid.uuid4()),
                "status": "CUSTOMER_STATUS_ACTIVE",
                "customer_details": {
                    "title": "CUSTOMER_TITLE_MR",
                    "first_name": "Daniel",
                    "middle_name": "Abbas",
                    "last_name": "Chorley",
                    "dob": "1980-12-25",
                    "gender": "CUSTOMER_GENDER_MALE",
                    "nationality": "British",
                    "email_address": "api_user@domain.com",
                }
            }
        }
        response = create_customer(customer_info)
        self.assertEqual(200, response.status_code, 'new customer should be created')

        account_info = {
            "request_id": request_id,
            "account": {
                "product_id": "film_th_saving_account",
                "stakeholder_ids": [
                    response.json()['id']
                ],
                "instance_param_vals": {
                    "opening_balance": "0",
                    "opening_denomination": "USD"
                },
                "details": {},
                "status": "ACCOUNT_STATUS_ACTIVE"
            }
        }
        response = create_account(account_info)
        self.assertEqual(200, response.status_code, 'new account should be created')


if __name__ == '__main__':
    unittest.main()
    # asyncio.run(unittest.main())
