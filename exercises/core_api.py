import json
from shutil import Error
from symbol import parameters

import requests
from environment import TM_CORE_URL, TM_TOKEN


def fetch_balances_live(account_id, address = 'DEFAULT', page_size=100):
    response = requests.get(
        url=f"{TM_CORE_URL}/v1/balances/live?account_ids={account_id}&account_addresses={address}&page_size={page_size}",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        }
    )
    print(response.content)
    return response

def trigger_close_code_hooks(account_id, request_id):
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/account-updates",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            'account_update': {
                'account_id': account_id,
                'closure_update': {}
            }
        }
    )
    print(response.content)
    return response

def trigger_post_activation_code_hooks(account_id, request_id):
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/account-updates",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            'account_update': {
                'account_id': account_id,
                'activation_update': {}
            }
        }
    )
    print(response.content)
    return response

def update_parameters_v2(product_version_id, request_id, items_to_add):
    response = requests.put(
        url=f"{TM_CORE_URL}/v1/product-versions/{product_version_id}:updateParams",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            "items_to_add": items_to_add,
        }
    )
    print(response.content)
    return response

def update_parameters(account_id, request_id, instance_param_vals):
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/account-updates",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            'account_update': {
                'account_id': account_id,
                'instance_param_vals_update': {
                    'instance_param_vals': instance_param_vals,
                }
            }
        }
    )

    print(response.content)

    return response


def cancel_account(account_id, request_id):
    return requests.put(
        url=f"{TM_CORE_URL}/v1/accounts/{account_id}",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            'account': {
                'status': 'ACCOUNT_STATUS_CANCELLED'
            },
            'update_mask': {
                'paths': [
                    "status"
                ]
            }
        }
    )

def activate_account(account_id, request_id):
    return requests.put(
        url=f"{TM_CORE_URL}/v1/accounts/{account_id}",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'request_id': request_id,
            'account': {
                'status': 'ACCOUNT_STATUS_OPEN'
            },
            'update_mask': {
                'paths': [
                    "status"
                ]
            }
        }
    )

def pending_closure_account(account_id, request_id):
    response = requests.put(
        url=f"{TM_CORE_URL}/v1/accounts/{account_id}",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            "request_id": request_id,
            "account": {
                'status': 'ACCOUNT_STATUS_PENDING_CLOSURE'
            },
            'update_mask': {
                'paths': [
                    "status"
                ]
            }
        },
    )
    print(response.content)
    return response

def close_account(account_id, request_id):
    response = requests.put(
        url=f"{TM_CORE_URL}/v1/accounts/{account_id}",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            "request_id": request_id,
            "account": {
                'status': 'ACCOUNT_STATUS_CLOSED'
            },
            'update_mask': {
                'paths': [
                    "status"
                ]
            }
        },
    )
    print(response.content)
    return response

def create_account(account_info):
    print(f"BEGIN - {TM_CORE_URL}/v1/accounts")
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/accounts",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json=account_info,
    )
    print(response.content)
    print(f"END - {TM_CORE_URL}/v1/accounts")
    return response

def create_pib_async(posting_instruction_batch):
    print(f"BEGIN - {TM_CORE_URL}/v1/posting-instruction-batches:asyncCreate")
    print(f"request:{json.dumps(posting_instruction_batch, indent=2)}")
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/posting-instruction-batches:asyncCreate",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json=posting_instruction_batch
    )
    print(response.content)
    print(f"END - {TM_CORE_URL}/v1/posting-instruction-batches:asyncCreate")
    return response

def fetch_posting_instruction(client_batch_id, account_id, page_size=10):
    if client_batch_id is not None:
        response = requests.get(
            url=f"{TM_CORE_URL}/v1/posting-instruction-batches?client_batch_ids={client_batch_id}&account_ids={account_id}&page_size={page_size}",
            headers={
                'X-Auth-Token': TM_TOKEN,
                'Content-Type': 'application/json',
            },
        )
    else:
        response = requests.get(
            url=f"{TM_CORE_URL}/v1/posting-instruction-batches?account_ids={account_id}&page_size={page_size}",
            headers={
                'X-Auth-Token': TM_TOKEN,
                'Content-Type': 'application/json',
            },
        )

    return response.json()

def create_customer(customer_info):
    print(f"BEGIN - {TM_CORE_URL}/v1/customers")
    response = requests.post(
        url=f"{TM_CORE_URL}/v1/customers",
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
        json=customer_info,
    )
    print(response.content)
    print(f"END - {TM_CORE_URL}/v1/customers")
    return response

def get_schedules_associations(account_id, page_size=10):
    response = requests.get(
        url=f"{TM_CORE_URL}/v1/account-schedule-assocs",
        params={
            'account_id': account_id,
            'page_size': page_size,
        },
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
    )
    return response

def get_schedulers_batch(schedule_ids, page_size=10):
    response = requests.get(
        url=f"{TM_CORE_URL}/v1/schedules:batchGet",
        params={
            'ids': schedule_ids,
            'page_size': page_size,
        },
        headers={
            'X-Auth-Token': TM_TOKEN,
            'Content-Type': 'application/json',
        },
    )
    return response

def get_schedules_status(account_id):
    response = get_schedules_associations(account_id)
    if response.status_code != 200:
        raise Error('get get_schedules_associations error')
    schedule_ids = [item['schedule_id'] for item in response.json()['account_schedule_assocs']]

    if len(schedule_ids) == 0:
        raise Error(f"{account_id} has no schedule")
    response = get_schedulers_batch(schedule_ids)
    if response.status_code != 200:
        raise Error('get get_schedulers_batch error')

    schedules = response.json()['schedules']
    schedule_status = {}
    for schedule_id in schedule_ids:
        schedule_status[schedule_id] = schedules[schedule_id]['status']

    return schedule_status