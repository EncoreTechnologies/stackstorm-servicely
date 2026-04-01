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
import st2client.models
from st2client.client import Client
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
    'ServicelyExecutionMonitor'
]


class ServicelyExecutionMonitor(PollingSensor):
    def __init__(self, sensor_service, config=None, poll_interval=None):
        super(ServicelyExecutionMonitor, self).__init__(sensor_service=sensor_service,
                                              config=config,
                                              poll_interval=poll_interval)
        self._logger = self._sensor_service.get_logger(__name__)
        self.trigger_ref = "servicely.task_finish"

    def setup(self):
        st2_token = self._config['servicely']['st2_token']
        self.st2_key_name = 'servicely.executions'

        st2_fqdn = socket.getfqdn()
        st2_url = "https://{}/".format(st2_fqdn)

        self.st2_client = Client(base_url=st2_url, api_key=st2_token)

    def poll(self):
        # Fetch queue results with specific error handling
        self._logger.info(f"Checking for Async Executions: {self.st2_key_name}")
        servicely_executions_dict = {}

        try:
            servicely_executions = self.st2_client.keys.get_by_name(name=self.st2_key_name)

            if servicely_executions:
                servicely_executions_dict = json.loads(servicely_executions.value)
        except Exception as e:
            self._logger.error(f"Unexpected error processing st2 key {self.st2_key_name}: {str(e)}")
            raise

        if not servicely_executions_dict:
            self._logger.info("No active Async Executions")
            return True

        self._logger.info(f"Found {len(servicely_executions_dict)} async executions to check")

        # Process each result with specific error handling for each REST call
        for execution_id, task in servicely_executions_dict.items():
            try:
                execution = self.st2_client.liveactions.get_by_id(execution_id)
                if execution.status in PENDING_STATUSES:
                    self._logger.info(f"Execution {execution_id} is still running")
                    continue

                self._logger.info(f"Execution {execution_id} is finished")
                record_id = task.get('id')

                # Extract source connection info stored by task_start
                source_conn = task.get('source_connection', {})

                trigger_payload = {
                    'queue_name': source_conn.get('queue_name', ''),
                    'record_id': record_id,
                    'execution_id': execution_id,
                    'task': task,
                    'server': source_conn.get('server', ''),
                    'endpoint': source_conn.get('endpoint', ''),
                    'token': source_conn.get('token', '')
                }
                self._sensor_service.dispatch(trigger=self.trigger_ref, payload=trigger_payload)
                self._logger.info(f"dispatched trigger for execution {execution_id}")
            except Exception as e:
                self._logger.error(f"Unexpected error processing execution {execution_id}: {str(e)}")
                continue

        return True

    def cleanup(self):
        pass

    def add_trigger(self, trigger):
        pass

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass
