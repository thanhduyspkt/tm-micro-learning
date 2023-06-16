# Postman
Here is Postman collection that contains essential Core/Workflow API usage
## Getting started
Download the Postman collection and import it into Your local Postman App.
In the Collection check the Variables as there are defined
* core_api - Core API
* token - Service token with allowed all endpoints


# Python

## Getting started


```python
    details = TMApiClient.TMConnectionDetails(core_api_url=CORE_API_URL,
                                              workflow_api_url=WORKFLOW_API_URL,
                                              token=X_AUTH_TOKEN,
                                              kafka_url=KAFKA_ENDPOINT)
    client = TMApiClient(details)
    # logging.info(client.call_core_api(PIB_URL_BATCH_GET, {"ids": "35ca2636-bc89-4786-abd2-492becd7e004"}))
    # client.subscribe_to_kafka_topic(PIB_TOPIC, handle=lambda key, value: print("key %s value %s" % (key, value)))
    to_publish = """{
   "request_id":"marcin",
   "posting_instruction_batch":{
      "client_id":"AsyncCreatePostingInstructionBatch",
      "client_batch_id":"marcin_test",
      "posting_instructions":[
         {
            "client_transaction_id":"marcin_test222",
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
    """
    to_publish2 = json.loads(to_publish)
    client.publish_kafka_message(PIB_REQUEST_TOPIC, "key", to_publish2)
    client.subscribe_to_kafka_topic(
        topic=PIB_TOPIC,
        callback=lambda key, value: print("key %s value %s" % (key, value)) if (key == "marcin_test") else None)

```
