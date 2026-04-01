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
import requests
import json

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

    def poll(self):
        for conn in self.connections:
            server = conn['server']
            endpoint = conn['endpoint']
            token = conn['token']
            queue_name = conn['queue_name']

            try:
                self._poll_connection(server, endpoint, token, queue_name)
            except Exception as e:
                self._logger.error(f"Failed to poll connection {server}: {str(e)}")
                continue

        return True

    def _poll_connection(self, server, endpoint, token, queue_name):
        """Poll a single Servicely connection for queue items."""
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
                return
            else:
                raise
        except requests.exceptions.RequestException:
            raise

        queue_results = response.json()

        if not queue_results:
            self._logger.info("No results found in queue")
            return

        self._logger.info(f"Found {len(queue_results['data'])} results in queue")

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
            except KeyError as e:
                self._logger.error(f"Missing required field in result {record_id}: {str(e)}")
                continue
            except Exception as e:
                self._logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
                continue

    def parse_record_payload(self, record_payload):
        """Parse record_payload and extract parameters, is_async flag, and servicely_parameters (case-insensitive)."""
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
                return {'parameters': {}, 'is_async': True, 'servicely_parameters': {}}
            elif 'is_async=false' in payload_lower:
                return {'parameters': {}, 'is_async': False, 'servicely_parameters': {}}
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

    def cleanup(self):
        pass

    def add_trigger(self, trigger):
        pass

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass
