import json
import os
from dataclasses import dataclass
from typing import Dict, Callable, Any
from kafka import KafkaConsumer, KafkaProducer
import requests
import logging

CONTENT_TYPE = 'application/json'

logging.basicConfig(level=logging.INFO)
KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'
CORE_API_URL = "https://core-api.partner-eph-6.tmachine.io"
WORKFLOW_API_URL = "https://workflows-api.partner-eph-6.tmachine.io"
X_AUTH_TOKEN = "A0006786022557907328897!rI1MCYa35TdEFt3kka7xh5edAoXEHfXGzntcA4vSxAseR+Cu+rseyz+j9Ql4WffZD8IsAZ9DUKDttPlqvSNsrfZd6To="

PIB_URL_BATCH_GET = "/v1/posting-instruction-batches:batchGet"
PIB_TOPIC = 'vault.api.v1.postings.posting_instruction_batch.created'
PIB_REQUEST_TOPIC = 'vault.core.postings.requests.v1'


def main():
    details = TMApiClient.TMConnectionDetails(core_api_url=CORE_API_URL,
                                              workflow_api_url=WORKFLOW_API_URL,
                                              token=X_AUTH_TOKEN,
                                              kafka_url=KAFKA_ENDPOINT)
    client = TMApiClient(details)
    # logging.info(client.call_core_api(PIB_URL_BATCH_GET, {"ids": "35ca2636-bc89-4786-abd2-492becd7e004"}))


class TMApiClient:
    @dataclass
    class TMConnectionDetails:
        core_api_url: str
        workflow_api_url: str
        token: str
        kafka_url: str
        kafka_security_protocol: str = "SSL"
        content_type: str = CONTENT_TYPE

    def __init__(self, conn: TMConnectionDetails):
        self.connection_details = conn
        self.default_headers = {
            'Content-Type': conn.content_type,
            'X-Auth-Token': conn.token
        }

    def call_core_api(self, endpoint: str, query_params: Dict[str, str], method: str = 'get', body: Any = None,
                      headers: Dict[str, str] = {}):
        return self.__call_api(url=self.connection_details.core_api_url + endpoint,
                               query_params=query_params,
                               method=method,
                               body=body,
                               extra_headers=headers)

    def call_workflow_api(self, endpoint: str, query_params: Dict[str, str], method: str = 'get', body: Any = None,
                      headers: Dict[str, str] = {}):
        return self.__call_api(url=self.connection_details.workflow_api_url + endpoint,
                               query_params=query_params,
                               method=method,
                               body=body,
                               extra_headers=headers)

    def __call_api(self, url: str, query_params: Dict[str, str], method: str, body: Any, extra_headers: Dict[str, str]):
        response = requests.request(method=method,
                                    url=url,
                                    params=query_params,
                                    data=body,
                                    headers={**self.default_headers, **extra_headers}
                                    )
        if not response.ok:
            raise Exception('Failed getting response for request %s, %s %s' % (
                str(response.request), response.status_code, response.text))
        return response.json()

    def publish_kafka_message(self, topic: str, key: str, value: Any) -> None:
        """
        Publish message to particular kafka topic.
        :param topic: Topic to where send the message
        :param key: Key of the message converted to bytes
        :param value: Value of the message, that can be converted to json and the to bytes
        :return: None
        """
        producer = KafkaProducer(bootstrap_servers=[self.connection_details.kafka_url],
                                 security_protocol=self.connection_details.kafka_security_protocol)

        producer.send(topic, key=key.encode('utf-8'), value=json.dumps(value).encode('utf-8'))

    def subscribe_to_kafka_topic(self,
                                 topic: str,
                                 group_id: str = str(os.getpid()),
                                 offset: str = 'earliest',
                                 callback: Callable[[str, str], None] = lambda *args: None,
                                 ) -> None:
        """
        Subscribe particular kafka topic to receive messages in json format encoded in utf-8
        :param topic: topic name to subscribe the consumer
        :param group_id: configure group id of consumers. Default value is process Id.
        :param offset: configure the offset for consumer between earliest,latest or exact offset value.
        Default value is earliest.
        :param callback: function that consumes message as arguments of key and value.
        :return: None
        """
        consumer = KafkaConsumer(topic,
                                 group_id=group_id, auto_offset_reset=offset,
                                 bootstrap_servers=[self.connection_details.kafka_url],
                                 security_protocol=self.connection_details.kafka_security_protocol)
        logging.debug("Consumer for topic {} has started".format(topic))
        for record in consumer:
            logging.debug("%s:%d:%d: key=%s value=%s" % (record.topic, record.partition,
                                                         record.offset, record.key,
                                                         record.value))
            callback(record.key.decode('utf8'), json.loads(record.value.decode('utf8').replace("'", '"')))


if __name__ == '__main__':
    main()
