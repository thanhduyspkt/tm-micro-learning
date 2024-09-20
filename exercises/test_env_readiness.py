import unittest
import uuid
from core_api import create_customer, create_account, create_pib_async
from kafka_api import create_pib_kafka_async
from kafka_kk import KafkaUtils
from environment import POSTING_CLIENT_ID

class TestEnvReadiness(unittest.TestCase):
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

    # send PIB to request topic
    # receive response from response topic related to the posting client id
    def test_create_pib_via_kafka_then_consume_posting_response_from_the_response_topic_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())

        filter_message = lambda message: message[
                                             'client_batch_id'] == f"{client_batch_id}_1"
        kafka_utils = KafkaUtils('integration.postings_api.deposits-core-new-test.response', 10)
        kafka_utils.start_consuming_messages(filter_message)
        create_pib_kafka_async({
            'request_id': request_id,
            'posting_instruction_batch': {
                'client_batch_id': f"{client_batch_id}_1",
                'client_id': POSTING_CLIENT_ID,
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
        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")
        self.assertEqual(messages[0]['status'], 'POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED')

# send PIB to request topic
    # can consume the streamed out message from vault.api.v1.postings.posting_instruction_batch.created
    def test_create_pib_via_kafka_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())

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
        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")

    # create posting using HTTP posting API
    # can consume message from topic vault.api.v1.postings.posting_instruction_batch.created
    def test_create_pib_then_consume_posting_response_successfully(self):
        request_id = str(uuid.uuid4())
        client_batch_id = str(uuid.uuid4())
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
        messages = kafka_utils.get_messages()
        self.assertEqual(1, len(messages), "Only one posting instruction response is expected")