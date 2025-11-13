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
        self.server = self._config['servicely']['server']
        self.token = self._config['servicely']['token']
        self.queue_name = self._config['servicely']['queue_name']
        
        self.servicely_queue_url = "https://{0}/v1/AsyncQueue?Queue={1}&QueueType=output&State=ready&fields=Subject,Payload,id,Queue".format(self.server, self.queue_name)
        self.headers = {'Authorization': f'Bearer {self.token}'}

    def poll(self):
        # Fetch queue results with specific error handling
        self._logger.info(f"Polling servicely queue: {self.servicely_queue_url}")
        
        try:
            response = requests.get(self.servicely_queue_url, headers=self.headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                self._logger.info("No results found in queue")
                return True
            else:
                raise  # Re-raise the exception for other HTTP errors
        except requests.exceptions.RequestException:
            raise  # Re-raise for other request-related exceptions (timeout, connection errors, etc.)
        
        queue_results = response.json()
        
        if not queue_results:
            self._logger.info("No results found in queue")
            return True
            
        self._logger.info(f"Found {len(queue_results['data'])} results in queue")
            
        # Process each result with specific error handling for each REST call
        for result in queue_results['data']:
            try:
                record_id = result.get('id')
                record_payload = result.get('Payload')
                parsed_payload = self.parse_record_payload(record_payload)

                trigger_playload = {
                    'queue_name': self.queue_name,
                    'record_id': record_id,
                    'task': result,
                    'servicely_parameters': parsed_payload.get('servicely_parameters', {})
                }
                is_async = parsed_payload['is_async']

                if is_async:
                    self._sensor_service.dispatch(trigger=self.trigger_ref_async, payload=trigger_playload)
                else:
                    self._sensor_service.dispatch(trigger=self.trigger_ref, payload=trigger_playload)

                self._logger.info(f"dispatched trigger for record {record_id}")
            except KeyError as e:
                self._logger.error(f"Missing required field in result {record_id}: {str(e)}")
                continue
            except Exception as e:
                self._logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
                continue
        
        return True
    
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
