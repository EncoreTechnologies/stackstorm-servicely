#!/usr/bin/env python
# Copyright 2019 Encore Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from st2common.runners.base_action import Action
import st2client
import st2client.commands.action
import st2client.models
from st2client.client import Client
import socket
import requests
import time
import json


class BaseAction(Action):
    def __init__(self, config):
        """Creates a new BaseAction given a StackStorm config object (kwargs works too)
        :param config: StackStorm configuration object for the pack
        :returns: a new BaseAction
        """
        super(BaseAction, self).__init__(config)
        self.PENDING_STATUSES = [
            st2client.commands.action.LIVEACTION_STATUS_REQUESTED,
            st2client.commands.action.LIVEACTION_STATUS_SCHEDULED,
            st2client.commands.action.LIVEACTION_STATUS_DELAYED,
            st2client.commands.action.LIVEACTION_STATUS_PAUSING,
            st2client.commands.action.LIVEACTION_STATUS_PAUSED,
            st2client.commands.action.LIVEACTION_STATUS_RESUMING,
            st2client.commands.action.LIVEACTION_STATUS_RUNNING
        ]

    def setup_st2_client(self, st2_token):
        st2_fqdn = socket.getfqdn()
        st2_url = "https://{}/".format(st2_fqdn)

        st2_client = Client(base_url=st2_url, api_key=st2_token)

        return st2_client

    def lookup_servicely_token(self, st2_client, server):
        """
        Lookup the encrypted token for a given Servicely server from the keystore.

        :param st2_client: ST2 client instance
        :param server: Server hostname in format "env.servicely.ai"
        :returns: Token string if found, None otherwise
        """
        return_value = None
        try:
            # Parse environment from server (e.g., "encoretest.servicely.ai" -> "encoretest")
            env_name = server.split('.')[0]
            keystore_key = f"servicely.{env_name}.token"

            # Retrieve encrypted token from keystore using ST2 client
            key_pair = st2_client.keys.get_by_name(name=keystore_key, decrypt=True)

            if key_pair and key_pair.value:
                self.logger.info(f"Successfully retrieved token for server {server} from keystore key {keystore_key}")
                return_value = key_pair.value
            else:
                self.logger.error(f"Token not found in keystore for key: {keystore_key}")

        except Exception as e:
            self.logger.error(f"Failed to lookup token for server {server}: {str(e)}")

        return return_value

    def send_servicely_results(self, record_id, server, endpoint, token, payload):
        headers = {'Authorization': f'Bearer {token}'}
        servicely_Async_url = "https://{0}{1}".format(server, endpoint)

        try:
            st2_final_response = requests.post(
                servicely_Async_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            st2_final_response.raise_for_status()
            self.logger.info(f"Successfully posted results for record {record_id}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"method: POST")
            self.logger.error(f"headers: {headers}")
            self.logger.error(f"url: {servicely_Async_url}")
            self.logger.error(f"payload: {payload}")
            self.logger.error(f"response: {st2_final_response.content}")
            self.logger.error(f"Failed to post results for record {record_id}: {str(e)}")
            # raise
            pass

        return True

    def update_servicely_state(self, server, endpoint, token, queue_name, record_id, execution_id, task, state='processing'):
        headers = {'Authorization': f'Bearer {token}'}
        servicely_Async_url = "https://{0}{1}".format(server, endpoint)

        record_subject = task.get('Subject')
        record_payload = task.get('Payload')

        async_id_url = servicely_Async_url + "/{}".format(record_id)
        update_payload = {
            'Queue': queue_name,
            'Subject': record_subject,
            'Source': execution_id,
            'State': state
        }

        try:
            update_response = requests.patch(
                async_id_url,
                json=update_payload,
                headers=headers,
                timeout=30
            )
            update_response.raise_for_status()
            self.logger.info(f"Successfully updated record {record_id} to {state} state")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"method: PATCH")
            self.logger.error(f"headers: {headers}")
            self.logger.error(f"url: {async_id_url}")
            self.logger.error(f"payload: {update_payload}")
            self.logger.error(f"response: {update_response.content}")
            self.logger.error(f"Failed to update record {record_id} to {state} state: {str(e)}")
            # raise
            pass

        return True

    def execute_action(self, exec_name, exec_params, st2_token, is_async=False, wait_time_sec=1):
        st2_client = self.setup_st2_client(st2_token)

        execution_instance = st2client.models.LiveAction()
        execution_instance.action = exec_name
        execution_instance.parameters = exec_params

        # Catch any errors when triggering executions from the sensor
        execution = None
        try:
            execution = st2_client.liveactions.create(execution_instance)
        except Exception as e:
            self.logger.info(
                "Error triggering an execution for {}:\n{}\nExiting sensor!".format(exec_name, e)
            )
            return False

        self.logger.info('Starting {}...'.format(execution.action['name']))

        return_value = execution.id
        if not is_async:
            while execution.status in self.PENDING_STATUSES:
                self.logger.info('Action {} is still running. Waiting to finish.'.format(execution.action['name']))
                time.sleep(wait_time_sec)
                execution = st2_client.liveactions.get_by_id(execution.id)

            return_value = execution.to_dict()

        return return_value

    def parse_record_payload(self, record_payload):
        """Parse record_payload and extract parameters and is_async flag (case-insensitive)."""
        default_result = {'parameters': {}, 'is_async': None, 'servicely_parameters': {}}

        if not isinstance(record_payload, str):
            return default_result

        # Convert to lowercase for comparison
        payload_lower = record_payload.lower()

        # Handle special empty cases (lowercase)
        empty_patterns = [
            '{parameters={}}',
            '{parameters={}, is_async=true}',
            '{parameters={}, is_async=false}'
        ]

        if payload_lower in empty_patterns:
            # Extract is_async from the pattern if present
            if 'is_async=true' in payload_lower:
                return {'parameters': {}, 'is_async': True}
            elif 'is_async=false' in payload_lower:
                return {'parameters': {}, 'is_async': False}
            return default_result

        try:
            parsed = json.loads(record_payload)
            if isinstance(parsed, list):
                return default_result
            if isinstance(parsed, dict):
                parsed_lower = {k.lower(): v for k, v in parsed.items()}
                default_result = {
                    'parameters': parsed_lower.get('parameters', {}),
                    'is_async': parsed_lower.get('is_async', None),
                    'servicely_parameters': parsed_lower.get('servicely_parameters', {})
                }
        except (json.JSONDecodeError, KeyError, TypeError):
            return default_result

        return default_result

    def fetch_paginated_data(self, url, params, timeout=30):
        all_results = []
        current_page = 1

        while True:
            try:
                params_with_page = params.copy()
                params_with_page['page'] = current_page

                response = requests.get(
                    url,
                    params=params_with_page,
                    timeout=timeout
                )
                response.raise_for_status()

                page_data = response.json()
                if isinstance(page_data, list):
                    all_results.extend(page_data)
                else:
                    all_results.append(page_data)

                next_page = response.headers.get('x-next-page', '').strip()

                if not next_page:
                    break

                current_page = int(next_page)

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Failed to fetch data from {url}")
                self.logger.error(f"Parameters: {params_with_page}")
                self.logger.error(f"Error: {str(e)}")
                raise

        return all_results

    def fetch_and_post_paginated_data(
        self,
        url,
        params,
        queue_name,
        subject,
        server,
        endpoint,
        token,
        execution_id=None,
        timeout=30
    ):
        total_items = 0
        pages_posted = 0
        current_page = 1

        while True:
            try:
                params_with_page = params.copy()
                params_with_page['page'] = current_page

                response = requests.get(
                    url,
                    params=params_with_page,
                    timeout=timeout
                )
                response.raise_for_status()

                page_data = response.json()
                if not page_data:
                    break

                if isinstance(page_data, list):
                    item_count = len(page_data)
                    total_items += item_count
                else:
                    item_count = 1
                    total_items += 1

                self.logger.info(
                    f"Page {current_page}: {item_count} items"
                )

                self.post_to_servicely_queue(
                    queue_name=queue_name,
                    subject=subject,
                    payload=page_data,
                    server=server,
                    endpoint=endpoint,
                    token=token,
                    execution_id=execution_id
                )
                pages_posted += 1

                next_page = response.headers.get('x-next-page', '').strip()

                if not next_page:
                    break

                current_page = int(next_page)
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Failed to fetch data from {url}")
                self.logger.error(f"Parameters: {params_with_page}")
                self.logger.error(f"Error: {str(e)}")
                raise

        return {
            'total_items': total_items,
            'pages_posted': pages_posted
        }

    def post_data_in_chunks(
        self,
        data,
        queue_name,
        subject,
        server,
        endpoint,
        token,
        execution_id=None,
        chunk_size=100,
        state="ready"
    ):
        chunks_posted = 0

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]

            self.logger.info(
                f"Posting chunk {chunks_posted + 1}: {len(chunk)} items"
            )

            self.post_to_servicely_queue(
                queue_name=queue_name,
                subject=subject,
                payload=chunk,
                server=server,
                endpoint=endpoint,
                token=token,
                execution_id=execution_id,
                state=state
            )
            chunks_posted += 1

            if i + chunk_size < len(data):
                time.sleep(1)

        return chunks_posted

    def post_to_servicely_queue(
        self,
        queue_name,
        subject,
        payload,
        server,
        endpoint,
        token,
        execution_id=None,
        state="ready"
    ):
        headers = {'Authorization': f'Bearer {token}'}
        servicely_url = f"https://{server}{endpoint}"

        request_body = {
            "Queue": queue_name,
            "Subject": subject,
            "QueueType": "input",
            "State": state,
            "Payload": json.dumps(payload)
        }

        if execution_id:
            request_body["Source"] = execution_id

        try:
            response = requests.post(
                servicely_url,
                json=request_body,
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            self.logger.info(
                f"Successfully posted to queue {queue_name}"
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to post to Servicely queue")
            self.logger.error(f"URL: {servicely_url}")
            self.logger.error(f"Queue: {queue_name}, Subject: {subject}")
            if isinstance(payload, list):
                self.logger.error(f"Payload: list with {len(payload)} items")
            else:
                self.logger.error(f"Payload: {type(payload).__name__}")
            self.logger.error(f"Error: {str(e)}")
            raise

        return True

    def run(self, **kwargs):
        raise RuntimeError("run() not implemented")
