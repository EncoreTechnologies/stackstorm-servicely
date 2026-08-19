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
import copy


class BaseAction(Action):
    def __init__(self, config):
        """Creates a new BaseAction given a StackStorm config object (kwargs works too)
        :param config: StackStorm configuration object for the pack
        :returns: a new BaseAction
        """
        super(BaseAction, self).__init__(config)
        self.hostname = socket.getfqdn()
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

    def inject_connection_params(self, st2_client, subject, exec_params,
                                 server, token, endpoint, queue_name):
        # Pass the calling Servicely connection into any action that declares
        # one of the reserved 'servicely_*' params. Declaring the param is how
        # an action opts in
        connection_params = {
            'servicely_server': server,
            'servicely_token': token,
            'servicely_endpoint': endpoint,
            'servicely_queue_name': queue_name,
        }

        try:
            action_meta = st2_client.actions.get_by_ref_or_id(subject)
        except Exception as e:
            self.logger.error("Could not look up parameters for {}: {}".format(subject, str(e)))
            return exec_params

        if not action_meta:
            self.logger.error("Action {} not found, skipping connection injection".format(subject))
            return exec_params

        declared = getattr(action_meta, 'parameters', None) or {}
        for name, value in connection_params.items():
            if name in declared:
                exec_params[name] = value
                self.logger.info("Injecting {} from Servicely connection".format(name))

        return exec_params

    def send_servicely_results(self, record_id, server, endpoint, token, payload):
        headers = {'Authorization': f'Bearer {token}'}
        servicely_Async_url = "https://{0}{1}".format(server, endpoint)

        # Add ST2 server hostname and queue name inside the Payload sent to Servicely
        try:
            inner_payload = json.loads(payload.get('Payload', '{}'))
            if isinstance(inner_payload, dict):
                inner_payload['hostname'] = self.hostname
                inner_payload['queue_name'] = payload.get('Queue', '')
                payload['Payload'] = json.dumps(inner_payload)
        except (json.JSONDecodeError, TypeError):
            pass

        st2_final_response = None
        try:
            st2_final_response = requests.post(
                servicely_Async_url,
                json=payload,
                headers=headers,
                timeout=120
            )
            st2_final_response.raise_for_status()
            self.logger.info(f"Successfully posted results for record {record_id}")
        except requests.exceptions.RequestException as e:
            if st2_final_response is not None:
                response_content = st2_final_response.content
            else:
                response_content = 'no response (request failed before send)'
            self.logger.error(f"method: POST")
            self.logger.error(f"headers: {headers}")
            self.logger.error(f"url: {servicely_Async_url}")
            self.logger.error(f"payload: {payload}")
            self.logger.error(f"response: {response_content}")
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

        update_response = None
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
            if update_response is not None:
                response_content = update_response.content
            else:
                response_content = 'no response (request failed before send)'
            self.logger.error(f"method: PATCH")
            self.logger.error(f"headers: {headers}")
            self.logger.error(f"url: {async_id_url}")
            self.logger.error(f"payload: {update_payload}")
            self.logger.error(f"response: {response_content}")
            self.logger.error(f"Failed to update record {record_id} to {state} state: {str(e)}")
            # raise
            pass

        return True

    def execute_action(self, exec_name, exec_params, st2_token, is_async=False, wait_time_sec=1):
        st2_client = self.setup_st2_client(st2_token)

        execution_instance = st2client.models.LiveAction()
        execution_instance.action = exec_name
        execution_instance.parameters = exec_params

        execution = st2_client.liveactions.create(execution_instance)

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
        default_result = {
            'parameters': {},
            'is_async': None,
            'servicely_parameters': {},
            'subject_override': None,
            'batch_size': None
        }

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
                empty_result = dict(default_result)
                empty_result['is_async'] = True
                return empty_result
            elif 'is_async=false' in payload_lower:
                empty_result = dict(default_result)
                empty_result['is_async'] = False
                return empty_result
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
                    'servicely_parameters': parsed_lower.get('servicely_parameters', {}),
                    'subject_override': parsed_lower.get('subject_override', None),
                    'batch_size': parsed_lower.get('batch_size', None)
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
        timeout=30,
        c_parent=None
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
                    execution_id=execution_id,
                    c_parent=c_parent
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
        state="ready",
        c_parent=None
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
                state=state,
                c_parent=c_parent
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
        state="ready",
        c_parent=None
    ):
        headers = {'Authorization': f'Bearer {token}'}
        servicely_url = f"https://{server}{endpoint}"

        # Add ST2 server hostname and queue name to all payloads sent to Servicely
        # If payload is a list, wrap it in a dict so we can include the fields
        if isinstance(payload, list):
            payload = {
                'data': payload,
                'hostname': self.hostname,
                'queue_name': queue_name
            }
        elif isinstance(payload, dict):
            payload['hostname'] = self.hostname
            payload['queue_name'] = queue_name

        request_body = {
            "Queue": queue_name,
            "Subject": subject,
            "QueueType": "input",
            "State": state,
            "Payload": json.dumps(payload)
        }

        if execution_id:
            request_body["Source"] = execution_id

        if c_parent:
            request_body["C_parent"] = c_parent

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
            self.logger.error(f"Request body: {json.dumps(request_body)}")
            if isinstance(payload, list):
                self.logger.error(f"Payload: list with {len(payload)} items")
            else:
                self.logger.error(f"Payload: {type(payload).__name__}")
            self.logger.error(f"Error: {str(e)}")
            raise

        return True

    def find_batchable_list(self, execution_result, batch_size):
        """Locate a list in an action result that is eligible for batching.

        Returns a (path, list) tuple where path is the list of keys leading to
        the list within execution_result, or (None, None) when nothing
        qualifies. Two shapes are handled:
          - the action result is itself a list longer than batch_size
          - the action result is a dict with exactly one top-level value that
            is a list longer than batch_size
        Anything else (multiple large lists, more deeply nested lists, or
        lists at or below batch_size) is left for a single post.
        """
        try:
            action_result = execution_result['result']['result']
        except (KeyError, TypeError):
            return None, None

        if isinstance(action_result, list):
            if len(action_result) > batch_size:
                return ['result', 'result'], action_result
            return None, None

        if isinstance(action_result, dict):
            large_keys = [
                key for key, value in action_result.items()
                if isinstance(value, list) and len(value) > batch_size
            ]
            if len(large_keys) == 1:
                key = large_keys[0]
                return ['result', 'result', key], action_result[key]
            if len(large_keys) > 1:
                self.logger.info(
                    f"Multiple batchable lists {large_keys} found; "
                    f"posting result as a single record"
                )

        return None, None

    def set_by_path(self, obj, path, value):
        """Set a nested value in obj by following a list of keys (path)."""
        target = obj
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    def normalize_batch_size(self, batch_size, default=500):
        """Convert batch_size to a positive int, falling back to default.

        Tolerates a batch_size supplied by Servicely as an int, a numeric
        string, or None (no override).
        """
        try:
            batch_size = int(batch_size)
        except (TypeError, ValueError):
            return default
        if batch_size < 1:
            return default
        return batch_size

    def send_execution_result(self, record_id, server, endpoint, token,
                              queue_name, subject, execution_id,
                              execution_result, batch_size=None):
        """Post an execution result back to Servicely, batching large lists.

        Batching is opt-in: it only happens when batch_size is supplied. In
        that case, when the action result contains a list longer than
        batch_size, a header record (the execution result with that list
        emptied) is posted first, followed by the list contents in batches.
        When batch_size is not supplied, or nothing qualifies, the full
        execution result is posted as a single record.
        """
        if batch_size is None:
            list_path, list_data = None, None
        else:
            batch_size = self.normalize_batch_size(batch_size)
            list_path, list_data = self.find_batchable_list(
                execution_result, batch_size
            )

        if list_path is None:
            st2_payload = {
                "Queue": queue_name,
                "QueueType": "input",
                "Subject": subject,
                "State": "ready",
                "id": record_id,
                "Source": execution_id,
                "C_parent": record_id,
                "Payload": json.dumps(execution_result)
            }
            self.send_servicely_results(
                record_id, server, endpoint, token, st2_payload
            )
            return True

        # A large list was found: post the surrounding result as a header
        # record, then the list contents in batches. post_to_servicely_queue
        # and post_data_in_chunks raise on failure so the caller can mark the
        # record errored.
        self.logger.info(
            f"Batching {len(list_data)} items for record {record_id} "
            f"in chunks of {batch_size}"
        )

        header = copy.deepcopy(execution_result)
        self.set_by_path(header, list_path, [])

        self.post_to_servicely_queue(
            queue_name=queue_name,
            subject=subject,
            payload=header,
            server=server,
            endpoint=endpoint,
            token=token,
            execution_id=execution_id,
            c_parent=record_id
        )

        self.post_data_in_chunks(
            data=list_data,
            queue_name=queue_name,
            subject=subject,
            server=server,
            endpoint=endpoint,
            token=token,
            execution_id=execution_id,
            chunk_size=batch_size,
            c_parent=record_id
        )

        return True

    def run(self, **kwargs):
        raise RuntimeError("run() not implemented")
