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
        
    def run(self, server, token, st2_token, queue_name, record_id, task):
        """Main entry point for the StackStorm actions to execute the operation.
        :returns: Dictionary of networks
        """
        execution_id = os.environ.get('ST2_ACTION_EXECUTION_ID')
        headers = {'Authorization': f'Bearer {token}'}
        servicely_Async_url = "https://{0}/v1/AsyncQueue".format(server)

        try:
            record_subject = task.get('Subject')
            record_payload = task.get('Payload')

            self.update_servicely_state(server, token, queue_name, record_id, execution_id, task, 'processing')

            try:
                # exec_params = {}
                # if type(record_payload) is str and record_payload != '{parameters={}}':
                #     exec_params = json.loads(record_payload)['parameters']
                exec_params = self.parse_record_payload(record_payload)['parameters']
                execution_result = self.execute_action(record_subject, exec_params, st2_token, is_async=True)
            except Exception as e:
                self.logger.info(f"Failed to execute action for record {record_id}: {str(e)}")
                raise
            
            st2_client = self.setup_st2_client(st2_token)

            servicely_executions = st2_client.keys.get_by_name(
                name='servicely.executions'
            )

            servicely_executions_dict = {}
            if servicely_executions:
                servicely_executions_dict = json.loads(servicely_executions.value)

            servicely_executions_dict[execution_result] = task

            st2_key_pair = KeyValuePair(name='servicely.executions', value=json.dumps(servicely_executions_dict))
            st2_client.keys.update(st2_key_pair)
        except KeyError as e:
            self.logger.error(f"Missing required field in result {record_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
            raise
        
        return True
