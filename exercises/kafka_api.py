import json

from kafka_kk import produce_messages

def create_pib_kafka_async(posting_instruction_batch):
    print(f"BEGIN - sending request to topic vault.core.postings.requests.v1")
    print(f"request: {json.dumps(posting_instruction_batch, indent=2)}")
    if isinstance(posting_instruction_batch, list):
        produce_messages("vault.core.postings.requests.v1", posting_instruction_batch)
    else:
        produce_messages("vault.core.postings.requests.v1", [posting_instruction_batch])
    print(f"END - sending request to topic vault.core.postings.requests.v1")