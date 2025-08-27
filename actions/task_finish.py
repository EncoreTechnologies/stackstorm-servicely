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
        
    def run(self, server, token, st2_token, queue_name, record_id, execution_id, task):
        """Main entry point for the StackStorm actions to execute the operation.
        :returns: Dictionary of networks
        """
        parent_execution_id = os.environ.get('ST2_ACTION_EXECUTION_ID')
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
            
            if execution_id in servicely_executions_dict:
                del servicely_executions_dict[execution_id]
            
            st2_key_pair = KeyValuePair(name='servicely.executions', value=json.dumps(servicely_executions_dict))
            st2_client.keys.update(st2_key_pair)

            st2_payload = {
                "Queue": queue_name,
                "QueueType": "input",
                "Subject": record_subject,
                "State": "ready",
                "id": record_id,
                'Source': parent_execution_id,
                "Payload": json.dumps(execution_result.to_dict())
            }
            self.send_servicely_results(record_id, server, token, st2_payload)
            self.update_servicely_state(server, token, queue_name, record_id, execution_id, task, 'processed')
        except KeyError as e:
            self.logger.error(f"Missing required field in result {record_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error processing record {record_id}: {str(e)}")
            raise
        
        return True
