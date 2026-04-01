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


class St2ActionsGet(BaseAction):
    def __init__(self, config):
        super(St2ActionsGet, self).__init__(config)

    def normalize_default(self, st2_client, action_value):
        return_param = {'defaulted': True}
        if 'secret' not in action_value or ('secret' in action_value and not action_value['secret']):
            # Check if default is a string and contains template syntax {{ }}
            default_value = action_value['default']

            # If default is not a string or doesn't contain template syntax, use it as-is
            if not isinstance(default_value, str) or '{{' not in default_value:
                return_param['default'] = default_value
                return return_param

            # Remove {{ and }} and strip whitespace
            key_name = default_value.strip().strip('{}').strip()

            if '|' in key_name:
                key_name = key_name.split('|')[0].strip()

            # Remove 'st2kv.system.' prefix
            if key_name.startswith('st2kv.system.'):
                key_name = key_name[len('st2kv.system.'):]

            try:
                key_value = st2_client.keys.get_by_name(name=key_name)
                # Check if key_value is a list or has a value attribute
                if isinstance(key_value, list):
                    if len(key_value) > 0:
                        return_param['default'] = key_value[0].value if hasattr(key_value[0], 'value') else key_value[0]
                    else:
                        return_param['default'] = None
                elif hasattr(key_value, 'value'):
                    return_param['default'] = key_value.value
                else:
                    return_param['default'] = key_value
            except Exception as e:
                self.logger.error(f"Unexpected error getting key: {key_name}\ndefault: {action_value['default']}\nerror: {str(e)}")
                return_param['default'] = None
        else:
            return_param['secret'] = True

        return return_param

    def process_parameters(self, st2_client, parameters):
        """Recursively process parameters, handling nested structures."""
        all_params = []

        for key, value in parameters.items():
            # Skip parameters without a 'type' field
            if 'type' not in value:
                continue

            return_param = {
                'name': key,
                'type': value['type'],
                'required': value.get('required', False)
            }

            if 'description' in value:
                return_param['description'] = value['description']

            if 'default' in value:
                return_param.update(self.normalize_default(st2_client, value))

            # Handle array type with object items that have parameters
            if (value['type'] == 'array' and 'items' in value):
                if (value['items']['type'] == 'object' and 'parameters' in value['items']):
                    # Recursively process nested parameters
                    nested_params = self.process_parameters(st2_client, value['items']['parameters'])
                    return_param['items'] = {
                        'type': 'object',
                        'parameters': nested_params
                    }

            all_params.append(return_param)

        return all_params

    def structure_action(self, st2_client, action):
        return_action_dict = {
            'ref': action.ref,
            'name': action.name,
            'pack': action.pack,
            'description': action.description
        }

        if action.parameters:
            return_action_dict['parameters'] = self.process_parameters(st2_client, action.parameters)

        return return_action_dict

    def run(self, server, endpoint, token, st2_token, queue_name):
        """Main entry point for the StackStorm actions to execute the operation.
        :returns: Dictionary of networks
        """
        execution_id = os.environ.get('ST2_ACTION_EXECUTION_ID')
        pack_separated_actions = {}

        try:
            st2_client = self.setup_st2_client(st2_token)
            all_actions = st2_client.actions.get_all()

            # Build the pack-separated actions dictionary
            for action in all_actions:
                action_dict = self.structure_action(st2_client, action)
                if action.pack not in pack_separated_actions:
                    pack_separated_actions[action.pack] = []

                pack_separated_actions[action.pack].append(action_dict)

            # Send each pack's actions to Servicely one at a time
            total_packs = len(pack_separated_actions)
            packs_posted = 0

            for pack_name, actions in pack_separated_actions.items():
                pack_payload = {
                    'pack': pack_name,
                    'actions': actions
                }

                self.logger.info(
                    f"Posting pack {pack_name}: {len(actions)} actions (Pack {packs_posted + 1}/{total_packs})"
                )

                self.post_to_servicely_queue(
                    queue_name=queue_name,
                    subject="servicely.st2_actions_get",
                    payload=pack_payload,
                    server=server,
                    endpoint=endpoint,
                    token=token,
                    execution_id=execution_id
                )
                packs_posted += 1

            self.logger.info(f"Successfully posted {packs_posted} packs to Servicely")

        except Exception as e:
            self.logger.error(f"Unexpected error processing actions: {str(e)}")
            raise

        return True
