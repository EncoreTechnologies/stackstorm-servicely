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


class TaskRun(BaseAction):
    def __init__(self, config):
        super(TaskRun, self).__init__(config)

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

            execution_success = True
            execution_result = None

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

                if record_payload:
                    try:
                        payload_data = json.loads(record_payload)
                        if isinstance(payload_data, list):
                            exec_params['filter_criteria'] = payload_data
                    except (json.JSONDecodeError, TypeError):
                        pass

                execution_result = self.execute_action(
                    record_subject,
                    exec_params,
                    st2_token
                )
            except Exception as e:
                execution_success = False
                execution_result = {
                    'success': False,
                    'error': str(e),
                    'action': record_subject
                }
                self.logger.error(
                    f"Failed to execute action for record {record_id}: {str(e)}"
                )

            # Send results to the override server/queue if specified, otherwise to original
            st2_payload = {
                "Queue": result_queue_name,
                "QueueType": "input",
                "Subject": record_subject,
                "State": "ready",
                "id": record_id,
                'Source': execution_id,
                "Payload": json.dumps(execution_result)
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

            # Always update the state on the ORIGINAL server
            final_state = 'processed' if execution_success else 'error'
            self.update_servicely_state(
                original_server,
                endpoint,
                original_token,
                original_queue_name,
                record_id,
                execution_id,
                task,
                final_state
            )
        except KeyError as e:
            self.logger.error(f"Missing required field in result {record_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
            raise

        return True
