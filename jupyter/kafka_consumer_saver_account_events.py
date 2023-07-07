# Import some necessary modules
from pymongo import MongoClient
import json
import os
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'

ACCOUNT_EVENTS_TOPIC = 'vault.core_api.v1.accounts.account.events'

'''
An AccountEvent is generated when an account event is published. We support:
    1. Account creation (AccountCreatedEvent)
    2. Updates of account details (AccountUpdatedEvent)
    3. Updates of account status (AccountUpdatedEvent)
    4. Updates of account stakeholder IDs (AccountUpdatedEvent)
'''

def main():
    # connect to DB
    db = connect_to_mongo_db()

    try:

        topic = ACCOUNT_EVENTS_TOPIC
        # group_id = str(os.getpid())
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
            record_consumed = json.loads(msg.value)
            # print(json.dumps(record_consumed, indent=1))
            insert_into_mongo_db(db,record_consumed)

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


def insert_into_mongo_db(db,record_consumed):
    try:
        db.account_events.insert_one(record_consumed)
    except:
        print("record badly formatted to be inserted into mondo db")
        print(json.dumps(record_consumed, indent=1))

        """ you can use the following query string to search for bad_record in mongo_db 
        collection later -> { "bad_record": { "$exists": true } }
        """
        bad_record = {'bad_record': record_consumed}
        db.postings_requests.insert_one(bad_record)


if __name__ == '__main__':
    main()