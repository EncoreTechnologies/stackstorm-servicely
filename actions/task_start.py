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
from st2client.models import KeyValuePair
import json
import os


class TaskStart(BaseAction):
    def __init__(self, config):
        super(TaskStart, self).__init__(config)

    def run(self, server, endpoint, token, st2_token, queue_name, record_id, task):
        """Main entry point for the StackStorm actions to execute the operation.
        :returns: Dictionary of networks
        """
        execution_id = os.environ.get('ST2_ACTION_EXECUTION_ID')
        headers = {'Authorization': f'Bearer {token}'}
        servicely_Async_url = "https://{0}{1}".format(server, endpoint)

        # Store original server/token/queue for state updates
        original_server = server
        original_token = token
        original_queue_name = queue_name

        try:
            record_subject = task.get('Subject')
            record_payload = task.get('Payload')

            self.update_servicely_state(original_server, endpoint, original_token, original_queue_name, record_id, execution_id, task, 'processing')

            # Parse servicely_parameters for potential overrides
            parsed_payload = self.parse_record_payload(record_payload)
            exec_params = parsed_payload['parameters']
            servicely_parameters = parsed_payload.get('servicely_parameters', {})

            # Handle servicely_parameters overrides for sending results
            result_server = original_server
            result_token = original_token
            result_queue_name = original_queue_name

            if servicely_parameters:
                # Check for queue_name override
                if 'queue_name' in servicely_parameters:
                    result_queue_name = servicely_parameters['queue_name']

            try:
                # Handle watchman actions - pass servicely connection parameters
                if record_subject.startswith('servicely.watchman_'):
                    exec_params['queue_name'] = result_queue_name
                    exec_params['subject'] = record_subject
                    exec_params['execution_id'] = execution_id
                    exec_params['servicely_server'] = result_server
                    exec_params['servicely_endpoint'] = endpoint
                    exec_params['servicely_token'] = result_token

                # Handle st2_actions_get - pass servicely connection parameters
                if record_subject == 'servicely.st2_actions_get':
                    exec_params['server'] = result_server
                    exec_params['endpoint'] = endpoint
                    exec_params['token'] = result_token
                    exec_params['queue_name'] = result_queue_name

                execution_result = self.execute_action(record_subject, exec_params, st2_token, is_async=True)
            except Exception as e:
                self.logger.info(f"Failed to execute action for record {record_id}: {str(e)}")

                # Send error back to Servicely
                error_payload = {
                    'success': False,
                    'error': str(e),
                    'action': record_subject
                }
                st2_payload = {
                    "Queue": result_queue_name,
                    "QueueType": "input",
                    "Subject": record_subject,
                    "State": "ready",
                    "id": record_id,
                    'Source': execution_id,
                    "Payload": json.dumps(error_payload)
                }
                self.send_servicely_results(record_id, result_server, endpoint, result_token, st2_payload)
                self.update_servicely_state(original_server, endpoint, original_token, original_queue_name, record_id, execution_id, task, 'error')
                return {'success': False, 'error': str(e)}

            st2_client = self.setup_st2_client(st2_token)

            servicely_executions = st2_client.keys.get_by_name(
                name='servicely.executions'
            )

            servicely_executions_dict = {}
            if servicely_executions:
                servicely_executions_dict = json.loads(servicely_executions.value)

            # Store task with connection info so execution monitor and task_finish can use them
            task_with_params = task.copy()

            # Source connection info for the execution monitor to pass to task_finish
            # This is the original connection where the output record came from
            task_with_params['source_connection'] = {
                'server': original_server,
                'endpoint': endpoint,
                'token': original_token,
                'queue_name': original_queue_name
            }

            # Result routing overrides for task_finish (may differ from source
            # if servicely_parameters override was specified in the payload)
            task_with_params['servicely_parameters'] = {
                'server': result_server,
                'endpoint': endpoint,
                'token': result_token,
                'queue_name': result_queue_name
            }
            servicely_executions_dict[execution_result] = task_with_params

            st2_key_pair = KeyValuePair(name='servicely.executions', value=json.dumps(servicely_executions_dict))
            st2_client.keys.update(st2_key_pair)
        except KeyError as e:
            self.logger.error(f"Missing required field in result {record_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
            raise

        return True
