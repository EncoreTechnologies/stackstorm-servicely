#!/usr/bin/env python
# Copyright 2023 Encore Technologies
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
from st2reactor.sensor.base import PollingSensor
import st2client
import st2client.commands.action
from st2client.client import Client
import requests
import socket
import json

PENDING_STATUSES = [
    st2client.commands.action.LIVEACTION_STATUS_REQUESTED,
    st2client.commands.action.LIVEACTION_STATUS_SCHEDULED,
    st2client.commands.action.LIVEACTION_STATUS_DELAYED,
    st2client.commands.action.LIVEACTION_STATUS_PAUSING,
    st2client.commands.action.LIVEACTION_STATUS_PAUSED,
    st2client.commands.action.LIVEACTION_STATUS_RESUMING,
    st2client.commands.action.LIVEACTION_STATUS_RUNNING
]

__all__ = [
    'ServicelyQueueSensor'
]


class ServicelyQueueSensor(PollingSensor):
    def __init__(self, sensor_service, config=None, poll_interval=None):
        super(ServicelyQueueSensor, self).__init__(sensor_service=sensor_service,
                                              config=config,
                                              poll_interval=poll_interval)
        self._logger = self._sensor_service.get_logger(__name__)
        self.trigger_ref = "servicely.task_run"
        self.trigger_ref_async = "servicely.task_start"

    def setup(self):
        self.connections = self._config['servicely']['connections']

        # Per-server tuning driven by each box's datastore. A fast processing
        # server overrides poll_interval and turns on process_one so it handles
        # a single task at a time; the default server leaves these alone and
        # behaves as before.
        poll_interval = self._config['servicely'].get('poll_interval')
        if poll_interval:
            self._poll_interval = int(poll_interval)

        self.process_one = str(self._config['servicely'].get('process_one', 'false')).lower() == 'true'

        # An st2 client is only needed to detect in-flight work when serializing
        if self.process_one:
            st2_token = self._config['servicely']['st2_token']
            self.async_key_name = 'servicely.executions'

            st2_fqdn = socket.getfqdn()
            st2_url = "https://{}/".format(st2_fqdn)

            self.st2_client = Client(base_url=st2_url, api_key=st2_token)

    def poll(self):
        # When serializing, hold off on new work while a task is still being
        # processed (synchronously or asynchronously)
        if self.process_one and self._has_in_flight_work():
            self._logger.info("Task in progress, skipping poll until it completes")
            return True

        for conn in self.connections:
            server = conn['server']
            endpoint = conn['endpoint']
            token = conn['token']
            queue_name = conn['queue_name']

            try:
                dispatched = self._poll_connection(server, endpoint, token, queue_name)
            except Exception as e:
                self._logger.error(f"Failed to poll connection {server}: {str(e)}")
                continue

            # Only one task at a time, so stop once something is dispatched
            if self.process_one and dispatched:
                break

        return True

    def _poll_connection(self, server, endpoint, token, queue_name):
        """Poll a single Servicely connection for queue items.

        Returns True if a trigger was dispatched for at least one record.
        """
        servicely_queue_url = "https://{0}{1}?Queue={2}&QueueType=output&State=ready&fields=Subject,Payload,id,Queue".format(
            server, endpoint, queue_name)
        headers = {'Authorization': f'Bearer {token}'}

        self._logger.info(f"Polling servicely queue: {servicely_queue_url}")

        try:
            response = requests.get(servicely_queue_url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                self._logger.info("No results found in queue")
                return False
            else:
                raise
        except requests.exceptions.RequestException:
            raise

        queue_results = response.json()

        if not queue_results:
            self._logger.info("No results found in queue")
            return False

        self._logger.info(f"Found {len(queue_results['data'])} results in queue")

        dispatched = False
        for result in queue_results['data']:
            try:
                record_id = result.get('id')
                record_payload = result.get('Payload')
                parsed_payload = self.parse_record_payload(record_payload)

                trigger_payload = {
                    'queue_name': queue_name,
                    'record_id': record_id,
                    'task': result,
                    'server': server,
                    'endpoint': endpoint,
                    'token': token,
                    'servicely_parameters': parsed_payload.get('servicely_parameters', {})
                }
                is_async = parsed_payload['is_async']

                if is_async:
                    self._sensor_service.dispatch(trigger=self.trigger_ref_async, payload=trigger_payload)
                else:
                    self._sensor_service.dispatch(trigger=self.trigger_ref, payload=trigger_payload)

                self._logger.info(f"dispatched trigger for record {record_id}")
                dispatched = True

                # Only one task at a time, so stop after the first dispatch
                if self.process_one:
                    break
            except KeyError as e:
                self._logger.error(f"Missing required field in result {record_id}: {str(e)}")
                continue
            except Exception as e:
                self._logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
                continue

        return dispatched

    def _has_in_flight_work(self):
        """Check whether this server is already processing a servicely task.

        Covers the synchronous path (task_run executions still in a pending
        status) and the asynchronous path (executions launched by task_start
        and tracked in the servicely.executions datastore key until the
        execution monitor finalizes them).
        """
        try:
            servicely_executions = self.st2_client.keys.get_by_name(name=self.async_key_name)
            if servicely_executions and json.loads(servicely_executions.value):
                return True
        except Exception as e:
            self._logger.error(f"Failed to check key {self.async_key_name}: {str(e)}")

        for action_ref in (self.trigger_ref, self.trigger_ref_async):
            try:
                executions = self.st2_client.liveactions.query(action=action_ref, limit=5)
                if any(execution.status in PENDING_STATUSES for execution in executions):
                    return True
            except Exception as e:
                self._logger.error(f"Failed to check executions for {action_ref}: {str(e)}")

        return False

    def parse_record_payload(self, record_payload):
        """Parse record_payload and extract parameters, is_async flag, and servicely_parameters (case-insensitive)."""
        default_result = {'parameters': {}, 'is_async': None, 'servicely_parameters': {}, 'subject_override': None}

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
                return {'parameters': {}, 'is_async': True, 'servicely_parameters': {}, 'subject_override': None}
            elif 'is_async=false' in payload_lower:
                return {'parameters': {}, 'is_async': False, 'servicely_parameters': {}, 'subject_override': None}
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
                    'subject_override': parsed_lower.get('subject_override', None)
                }
        except (json.JSONDecodeError, KeyError, TypeError):
            return default_result

        return default_result

    def cleanup(self):
        pass

    def add_trigger(self, trigger):
        pass

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass
