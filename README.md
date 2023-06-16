# Postman
Here is Postman collection that contains essential Core/Workflow API usage
## Getting started
Download the Postman collection and import it into Your local Postman App.
In the Collection check the Variables as there are defined
* core_api - Core API
* token - Service token with allowed all endpoints


# Python
Here is the Python script that allows You to code any exercise, related to the Vault integration via REST or Kafka.
## Getting started
There's class `TMApiClient` that allows doing:
* call Core API
* call Workflow API
* receive events from Streaming API
* publish request i.e. to Posting API

`TMApiClient` requires `TMConnectionDetails` to initialize the class. `TMConnectionDetails` defines :
* Core API URL
* Workflow API URL
* Vault Kafka URL
* token (which will be used for both - workflow and core API)

## Examples
1. Call Core API for Posting Instruction Batches
```python
   KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'
   CORE_API_URL = "https://core-api.partner-eph-6.tmachine.io"
   WORKFLOW_API_URL = "https://workflows-api.partner-eph-6.tmachine.io"
   X_AUTH_TOKEN = "A0006786022557907328897!rI1MCYa35TdEFt3kka7xh5edAoXEHfXGzntcA4vSxAseR+Cu+rseyz+j9Ql4WffZD8IsAZ9DUKDttPlqvSNsrfZd6To="

   PIB_URL_BATCH_GET = "/v1/posting-instruction-batches:batchGet"
   details = TMApiClient.TMConnectionDetails(core_api_url=CORE_API_URL,
                                             workflow_api_url=WORKFLOW_API_URL,
                                             token=X_AUTH_TOKEN,
                                             kafka_url=KAFKA_ENDPOINT)
   client = TMApiClient(details)
   client.call_core_api(PIB_URL_BATCH_GET, {"ids": "35ca2636-bc89-4786-abd2-492becd7e004"})
```
2. Receive Posting Instruction Batches events, printing these where key is equal to "test"
```python
   KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'
   CORE_API_URL = "https://core-api.partner-eph-6.tmachine.io"
   WORKFLOW_API_URL = "https://workflows-api.partner-eph-6.tmachine.io"
   X_AUTH_TOKEN = "A0006786022557907328897!rI1MCYa35TdEFt3kka7xh5edAoXEHfXGzntcA4vSxAseR+Cu+rseyz+j9Ql4WffZD8IsAZ9DUKDttPlqvSNsrfZd6To="

   PIB_TOPIC = 'vault.api.v1.postings.posting_instruction_batch.created'
   details = TMApiClient.TMConnectionDetails(core_api_url=CORE_API_URL,
                                             workflow_api_url=WORKFLOW_API_URL,
                                             token=X_AUTH_TOKEN,
                                             kafka_url=KAFKA_ENDPOINT)
   client = TMApiClient(details)
   client.subscribe_to_kafka_topic(topic=PIB_TOPIC,
                                    callback=lambda key, value: print("key %s value %s" % (key, value)) if (key == "test") else None)
```
3. Publish request for to Posting API
```python
   KAFKA_ENDPOINT = 'bootstrap.kafka.partner-eph-6.tmachine.io:443'
   CORE_API_URL = "https://core-api.partner-eph-6.tmachine.io"
   WORKFLOW_API_URL = "https://workflows-api.partner-eph-6.tmachine.io"
   X_AUTH_TOKEN = "A0006786022557907328897!rI1MCYa35TdEFt3kka7xh5edAoXEHfXGzntcA4vSxAseR+Cu+rseyz+j9Ql4WffZD8IsAZ9DUKDttPlqvSNsrfZd6To="

   PIB_REQUEST_TOPIC = 'vault.core.postings.requests.v1'
   details = TMApiClient.TMConnectionDetails(core_api_url=CORE_API_URL,
                                              workflow_api_url=WORKFLOW_API_URL,
                                              token=X_AUTH_TOKEN,
                                              kafka_url=KAFKA_ENDPOINT)
   client = TMApiClient(details)
   request = json.loads("""{
   "request_id":"test",
   "posting_instruction_batch":{
      "client_id":"AsyncCreatePostingInstructionBatch",
      "client_batch_id":"test",
      "posting_instructions":[
         {
            "client_transaction_id":"test",
            "instruction_details":null,
            "override":null,
            "transaction_code":null,
            "outbound_authorisation":null,
            "inbound_authorisation":null,
            "authorisation_adjustment":null,
            "settlement":null,
            "release":null,
            "inbound_hard_settlement":{
                "amount":"20",
                "denomination":"USD",
                "target_account":{
                    "account_id":"40d78ae3-24f8-5393-251d-6ac5811c0433"
                },
                "internal_account_id":"migration_sample",
                "advice":true

            }
         }
      ],
      "batch_details":{
         "force_override":"true"
      }
   }}
    """)

    client.publish_kafka_message(PIB_REQUEST_TOPIC, "test", request)
```
