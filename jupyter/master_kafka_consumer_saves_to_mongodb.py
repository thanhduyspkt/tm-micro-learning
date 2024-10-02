# Import some necessary modules
from pymongo import MongoClient
import json
import os
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_ENDPOINT = 'bootstrap.kafka.partner-shared-sandbox.thirsty-fish-dephub.tmachine.io:443'

# BALANCE_EVENTS_TOPIC = 'vault.core_api.v1.balances.account_balance.events'

'''
An AccountBalanceEvent is generated whenever a customer's current balance changes and 
provides a view of the total state of the live balance.
'''


def main():
    # connect to DB
    db = connect_to_mongo_db()

    try:

        # topics = ['vault.core_api.v1.accounts.account.events',
        #           'vault.core.postings.requests.dlq.v1']

        topics = [
            'vault.core_api.v1.accounts.account.events',
            'vault.core_api.v1.accounts.account_update.events',
            'vault.core_api.v1.accounts.account_update_batch.events',
            'vault.core_api.v1.accounts.account_post_posting_execution_failure.events',
            'vault.core_api.v1.accounts.account_schedule_job_execution_failure.events',

            # 'vault.core_api.v1.parameters.parameter.events', <--- does not exist in Vault SaaS
            # 'vault.core_api.v1.parameters.parameter_value.events', <--- does not exist in Vault SaaS

            'vault.core_api.v1.calendar.calendar.events',
            'vault.core_api.v1.calendar.calendar_event.events',
            'vault.core_api.v1.calendar.calendar_period.events',

            'vault.core_api.v1.contracts.contract_notification.events',

            'vault.api.v1.customers.customer.created',
            'vault.api.v1.customers.customer.customer_details.updated',
            'vault.core_api.v1.customers.customer_address.events',

            'vault.core_api.v1.flags.flag.events',

            'vault.core_api.v1.plans.plan.events',
            'vault.core_api.v1.plans.account_plan_assoc.events',
            'vault.core_api.v1.plans.plan_update.events',
            'vault.core_api.v1.plans.plan_migration.events',

            'vault.core_api.v1.restrictions.restriction_set.events',
            'integration.postings_api.deposits-core-kkk-high.response',
            'integration.postings_api.deposits-core-kkk-low.response'
            'vault.api.v1.postings.posting_instruction_batch.created',
            'vault.core.postings.requests.v1',
            'vault.core_api.v1.postings.enriched_posting_instruction_batch.events',
            'vault.core.postings.requests.dlq.v1',

            'vault.core_api.v1.balances.account_balance.events',

            'vault.core_api.v1.payment_devices.payment_device.events',
            'vault.core_api.v1.payment_devices.payment_device_link.events',
            'vault.core_api.v1.payment_orders.payment_order.events',
            'vault.core_api.v1.payment_orders.payment_order_execution.events',

            'vault.api.v1.products.product_version.created',
            'vault.api.v1.products.product_version.parameter.updated',
            'vault.core_api.v1.scheduler.operation.events',]

        # group_id = str(os.getpid())
        group_id = 'for-pvga-master-consumer-v2'
        auto_offset_reset = 'latest'
        kafka_security_protocol = "SSL"
        api_version = (0, 8, 2)

        consumer = KafkaConsumer(group_id=group_id, auto_offset_reset=auto_offset_reset,
                                 bootstrap_servers=KAFKA_ENDPOINT,
                                 security_protocol=kafka_security_protocol)

        consumer.subscribe(topics=topics)

        logging.info("Consumer for topic {} has started".format(topics))

        while True:
            print('polling...')
            records = consumer.poll(timeout_ms=1000)
            for topic_partition, consumer_records in records.items():
                # Parse records
                for consumer_record in consumer_records:
                    print(str(consumer_record.value.decode('utf-8')))
                    print(topic_partition.topic)
                    insert_into_mongo_db(
                        db, topic_partition.topic, json.loads(consumer_record.value))
                continue

        # for msg in consumer:
        #     print(">>>> 3")
        #     # logging.info("%s:%d:%d: key=%s value=%s" % (record.topic, record.partition,
        #     #                                              record.offset, record.key,
        #     #                                              record.value))
        #     record_consumed = json.loads(msg.value)
        #     print(record_consumed.topic)
        #     # print(json.dumps(record_consumed, indent=1))
        #     # insert_into_mongo_db(db, record_consumed)

    except:
        print("Kafka Consumer failed")


def connect_to_mongo_db():
    username = "mongodb"
    password = "mongodb"
    host = "localhost"
    port = "27017"
    # Connect to MongoDB and vault_customer database
    try:
        client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin")
        db = client.vault_events

        print("Connected successfully!")
    except:
        print("Could not connect to MongoDB")
    return db


def insert_into_mongo_db(db, topic, record_consumed):
    try:
        # print("inside insert_into_mongo_db method")
        # collection_name = topic.replace('vault.core_api.', '')
        db[topic].insert_one(record_consumed)
        # db.balance_events.insert_one(record_consumed)
    except Exception as ex:
        print(f"record badly formatted to be inserted into mondo db {ex}")
        print(json.dumps(record_consumed, indent=1))

        """ you can use the following query string to search for bad_record in mongo_db 
        collection later -> { "bad_record": { "$exists": true } }
        """
        bad_record = {'bad_record': record_consumed}
        db[topic].insert_one(bad_record)
        # db.postings_requests.insert_one(bad_record)


if __name__ == '__main__':
    main()
