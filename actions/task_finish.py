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
# Disable message for: No name 'vim' in module 'pyVmomi' (no-name-in-module)
from lib.action_base import BaseAction
import os
import json
from st2client.models import KeyValuePair


class TaskFinish(BaseAction):
    def __init__(self, config):
        super(TaskFinish, self).__init__(config)

    def run(self, server, endpoint, token, st2_token, queue_name, record_id, execution_id, task):
        """Main entry point for the StackStorm actions to execute the operation.
        :returns: Dictionary of networks
        """
        parent_execution_id = os.environ.get('ST2_ACTION_EXECUTION_ID')

        # Store original server/token/queue (received as parameters) for state updates
        original_server = server
        original_token = token
        original_queue_name = queue_name

        try:
            record_subject = task.get('Subject')

            st2_client = self.setup_st2_client(st2_token)

            execution_result = st2_client.liveactions.get_by_id(execution_id)
            if execution_result.status in self.PENDING_STATUSES:
                self.logger.info(f"Execution {execution_id} is still running")
                return False

            servicely_executions = st2_client.keys.get_by_name(
                name='servicely.executions'
            )

            servicely_executions_dict = {}
            if servicely_executions:
                servicely_executions_dict = json.loads(servicely_executions.value)

            # Check if task has servicely_parameters override stored by task_start
            servicely_parameters = task.get('servicely_parameters', {})

            # Use override parameters if they exist, otherwise use original
            result_server = servicely_parameters.get('server', original_server)
            result_token = servicely_parameters.get('token', original_token)
            result_queue_name = servicely_parameters.get('queue_name', original_queue_name)

            if execution_id in servicely_executions_dict:
                del servicely_executions_dict[execution_id]

            st2_key_pair = KeyValuePair(name='servicely.executions', value=json.dumps(servicely_executions_dict))
            st2_client.keys.update(st2_key_pair)

            # Send results to the override server/queue if specified, otherwise to original
            st2_payload = {
                "Queue": result_queue_name,
                "QueueType": "input",
                "Subject": record_subject,
                "State": "ready",
                "id": record_id,
                'Source': parent_execution_id,
                "Payload": json.dumps(execution_result.to_dict())
            }

            # Try to send results to the result server (may be overridden)
            try:
                self.send_servicely_results(record_id, result_server, endpoint, result_token, st2_payload)
            except Exception as e:
                # If sending to override server fails, update original server's state to error
                error_msg = f"Failed to send results to {result_server}: {str(e)}"
                self.logger.error(error_msg)
                self.update_servicely_state(original_server, endpoint, original_token, original_queue_name, record_id, execution_id, task, 'error')
                return {'success': False, 'error': error_msg}

            # Determine final state based on execution result
            failed_statuses = ['failed', 'timeout', 'abandoned', 'canceled']
            if execution_result.status in failed_statuses:
                final_state = 'error'
            else:
                final_state = 'processed'

            # Always update the state on the ORIGINAL server
            self.update_servicely_state(original_server, endpoint, original_token, original_queue_name, record_id, execution_id, task, final_state)
        except KeyError as e:
            self.logger.error(f"Missing required field in result {record_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
            raise

        return True
