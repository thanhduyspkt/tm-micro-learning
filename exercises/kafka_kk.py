import json
import time
import uuid
from threading import Thread
from time import time_ns

from environment import TM_KAFKA_URL, TM_KAFKA_PROTOCOL, TM_KAFKA_SASL_MECHANISM, TM_KAFKA_SASL_PLAIN_PASSWORD, TM_KAFKA_SASL_PLAIN_USERNAME
from kafka import KafkaConsumer, KafkaProducer

PIB_TOPIC = 'vault.api.v1.postings.posting_instruction_batch.created'
PIB_REQUEST_TOPIC = 'vault.core.postings.requests.v1'
# group_id = 'for-pvga-consumption'
# auto_offset_reset = 'earliest'
# security_protocol = 'SASL_SSL'
# sasl_mechanism = 'SCRAM-SHA-512'
# sasl_plain_username = 'pl-dev-tm-user'
# sasl_plain_password = 'OGFlOTFjMjE5NTg2ZmUyNjc3MTQyNzlk'
api_version = (0, 8, 2)

# conf = {
#     'bootstrap.servers': TM_KAFKA_URL,
#     'group.id': 'kakakaka',  # Consumer group ID
#     'auto.offset.reset': 'earliest',
#     'security.protocol': TM_KAFKA_PROTOCOL,
#     # 'sasl.mechanism': TM_KAFKA_SASL_MECHANISM,
#     # 'sasl.username': TM_KAFKA_SASL_PLAIN_USERNAME,
#     # 'sasl.password': TM_KAFKA_SASL_PLAIN_PASSWORD,
# }

group_id = 'kaka_group_13'

def produce_messages(topic, messages):
    producer = KafkaProducer(
        bootstrap_servers=[TM_KAFKA_URL],
        security_protocol=TM_KAFKA_PROTOCOL,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        for message in messages:
            # message_payload = json.dumps(message)
            producer.send(topic, message)
        producer.flush()
    finally:
        producer.close()


def consumer_messages(topic, filter_func, consolidate_func, consuming_time = 30):
    kafka_consumer = KafkaConsumer(
        topic,  # Replace with your Kafka topic
        bootstrap_servers=[TM_KAFKA_URL],  # Replace with your Kafka broker(s)
        security_protocol=TM_KAFKA_PROTOCOL,
        auto_offset_reset='latest',  # Start reading from the latest message
        enable_auto_commit=True,  # Auto-commit the offsets
        group_id=group_id,  # Replace with your consumer group ID,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    start_time = time.time()
    try:
        while time.time() - start_time < consuming_time:
            print(f"polling messages from topic {topic}")
            message = kafka_consumer.poll(timeout_ms=1000)
            if message:
                for tp, messages in message.items():
                    for msg in messages:
                        json_message = json.loads(msg.value)
                        if filter_func(json_message):
                            consolidate_func(msg.value)
            else:
                print("No message received...")
    finally:
        print("close kafka consumer")
        kafka_consumer.close()

class KafkaUtils:
    def __init__(self, topic, timeout):
        self.topic = topic
        self.timeout = timeout
        self.messages = []
        self.messages_map = {
            'temp': [],
        }
        self.thread = None

    def get_messages(self):
        self.thread.join()
        return self.messages

    def get_messages_map(self):
        self.thread.join()
        print("--- SUMMARY ---")
        for k, v in self.messages_map.items():
            print(f"topic: {k} has {len(v)} messages")

        print("--- END ---")
        return self.messages_map

    def start_consuming_messages_from_topics(self, topics, filter_func):
        kafka_consumer = KafkaConsumer(
            *topics,  # Replace with your Kafka topic
            bootstrap_servers=[TM_KAFKA_URL],  # Replace with your Kafka broker(s)
            security_protocol=TM_KAFKA_PROTOCOL,
            auto_offset_reset='latest',  # Start reading from the latest message
            enable_auto_commit=True,  # Auto-commit the offsets
            group_id=group_id,  # Replace with your consumer group ID,
            value_deserializer=lambda x: x.decode('utf-8')
        )

        for topic in topics:
            self.messages_map[topic] = []

        start_time = time.time()

        def _consumer_messages():
            try:
                while time.time() - start_time < self.timeout:
                    print(f"polling messages ...")
                    message = kafka_consumer.poll(timeout_ms=100)
                    if message:
                        for tp, messages in message.items():
                            for msg in messages:
                                kafka_topic = msg.topic
                                json_message = json.loads(msg.value)
                                if filter_func is None or filter_func(json_message):
                                    self.messages_map[kafka_topic].append(json_message)
                                else:
                                    print(f"skip message {msg.value}")
                    else:
                        print("No message received...")
            finally:
                print("close kafka consumer")
                kafka_consumer.close()

        thread = Thread(target=_consumer_messages)
        thread.start()
        self.thread = thread
        time.sleep(3)

    def start_consuming_messages(self, filter_func):
        kafka_consumer = KafkaConsumer(
            self.topic,  # Replace with your Kafka topic
            bootstrap_servers=[TM_KAFKA_URL],  # Replace with your Kafka broker(s)
            security_protocol=TM_KAFKA_PROTOCOL,
            auto_offset_reset='latest',  # Start reading from the latest message
            enable_auto_commit=True,  # Auto-commit the offsets
            group_id=group_id,  # Replace with your consumer group ID,
            value_deserializer=lambda x: x.decode('utf-8')
        )

        start_time = time.time()

        def _consumer_messages():
            try:
                while time.time() - start_time < self.timeout:
                    print(f"polling messages from topic {self.topic}")
                    message = kafka_consumer.poll(timeout_ms=100)
                    if message:
                        for tp, messages in message.items():
                            for msg in messages:
                                json_message = json.loads(msg.value)
                                if filter_func(json_message):
                                    self.messages.append(json_message)
                                else:
                                    print(f"skip message {msg.value}")
                    else:
                        print("No message received...")
            finally:
                print("close kafka consumer")
                kafka_consumer.close()


        thread = Thread(target=_consumer_messages)
        thread.start()
        self.thread = thread
        time.sleep(3)