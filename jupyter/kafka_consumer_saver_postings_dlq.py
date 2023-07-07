# Import some necessary modules
from pymongo import MongoClient
import json
import os
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'

# PIB_REQUEST_TOPIC = 'vault.core.postings.requests.v1'
PIB_DLQ_TOPIC = 'vault.core.postings.requests.dlq.v1'
# CUSTOMER_CREATION_TOPIC = 'vault.api.v1.customers.customer.created'
# ACCOUNT_CREATED_TOPIC = 'vault.api.v1.accounts.account.created'
# ACCOUNT_EVENTS_TOPIC = 'vault.core_api.v1.accounts.account.events'
# ACCOUNT_STATUS_TOPIC = 'vault.api.v1.accounts.account.status.updated'
# ACCOUNT_UPDATE_TOPIC = 'vault.core_api.v1.accounts.account_update.events'
# PRODUCT_VERSION_CREATED_TOPIC = 'vault.api.v1.products.product_version.created'
# POSTING_CLIENT_API_RESPONSE_TOPIC = "vault.core.postings.async_creation_api.responses"


def main():
    # connect to DB
    db = connect_to_mongo_db()

    try:

        topic = PIB_DLQ_TOPIC
        group_id = 'for-pvga-consumption'
        auto_offset_reset = 'earliest'
        kafka_security_protocol = "SSL"
        api_version = (0, 8, 2)

        consumer = KafkaConsumer(topic,
                                 group_id=group_id, auto_offset_reset=auto_offset_reset,
                                 bootstrap_servers=KAFKA_ENDPOINT,
                                 security_protocol=kafka_security_protocol,
                                 api_version=api_version)

        logging.info("Consumer for topic {} has started".format(topic))

        for msg in consumer:
            # logging.info("%s:%d:%d: key=%s value=%s" % (record.topic, record.partition,
            #                                              record.offset, record.key,
            #                                              record.value))
            print(msg)
            record_consumed = json.loads(msg.value)
            # header_consumer = json.loads(msg.headers)
            # key_consumer = json.loads(msg.key)

            # print(json.dumps(key_consumer, indent=1))
            # print(json.dumps(header_consumer, indent=1))

            # print(json.dumps(record_consumed, indent=1))
            insert_into_mongo_db(db, record_consumed)

    except:
        print("Could not insert into MongoDB")


def connect_to_mongo_db():
    # Connect to MongoDB and vault_customer database
    try:
        client = MongoClient('localhost', 27017)
        db = client.vault_events

        print("Connected successfully!")
    except:
        print("Could not connect to MongoDB")
    return db


def insert_into_mongo_db(db, record_consumed):
    try:
        db.postings_dlq.insert_one(record_consumed)
    except:
        print("record badly formatted to be inserted into mondo db")
        # print(json.dumps(record_consumed, indent=1))

        """ you can use the following query string to search for bad_record in mongo_db 
        collection later -> { "bad_record": { "$exists": true } }
        """
        bad_record = {'bad_record': record_consumed}
        db.postings_dlq.insert_one(bad_record)


if __name__ == '__main__':
    main()


# db = client.pizza_rec
# print(client.list_database_names())

# id = '1503821867142587933'
# myCursor = db.coba_info.find({'id': id})
#
# for document in myCursor:
#     pprint(document)

# db.postings_dlq.insert_one(record_consumed)
# Create dictionary and ingest data into MongoDB
# DB_CONNECTION.postings_dlq.insert_one(record_consumed)
# customer_id = record_consumed['customer']['id']
# print("Customer_id created: %s" % (customer_id))
# pizza_rec = {'name': name, 'shop': shop, 'phoneNumber': phoneNumber, 'address': address, 'pizzas': pizzas}
# pizza_rec = json.loads('''
# {
#     "id": "1503821867142587933",
#     "status": "CUSTOMER_STATUS_ACTIVE",
#     "identifiers": [
#         {
#             "identifier_type": "IDENTIFIER_TYPE_EMAIL",
#             "identifier": "agvp14@test.com"
#         }
#     ],
#     "customer_details": {
#         "title": "CUSTOMER_TITLE_MISS",
#         "first_name": "Twoj",
#         "middle_name": "",
#         "last_name": "Stary",
#         "dob": "1956-01-08",
#         "gender": "CUSTOMER_GENDER_UNKNOWN",
#         "nationality": "British",
#         "email_address": "",
#         "mobile_phone_number": "+4464947779",
#         "home_phone_number": "+44310370076",
#         "business_phone_number": "+44347816009",
#         "contact_method": "CUSTOMER_CONTACT_METHOD_UNKNOWN",
#         "country_of_residence": "GB",
#         "country_of_taxation": "GB",
#         "accessibility": "CUSTOMER_ACCESSIBILITY_UNKNOWN",
#         "external_customer_id": ""
#     },
#     "additional_details": {}
# }
# ''')

# rec_id1 = db.coba_info.insert_one(pizza_rec)
# print("Data inserted with record ids", rec_id1)
