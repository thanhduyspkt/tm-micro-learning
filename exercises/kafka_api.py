import json

from kafka_kk import produce_messages

def create_pib_kafka_async(posting_instruction_batch, low_priority = False):
    print(f"request: {json.dumps(posting_instruction_batch, indent=2)}")
    topic = 'vault.core.postings.requests.v1'
    if low_priority:
        topic = 'vault.core.postings.requests.low_priority.v1'
    print(f"BEGIN - sending request to topic {topic}")
    if isinstance(posting_instruction_batch, list):
        produce_messages(topic, posting_instruction_batch)
    else:
        produce_messages(topic, [posting_instruction_batch])
    print(f"END - sending request to topic {topic}")