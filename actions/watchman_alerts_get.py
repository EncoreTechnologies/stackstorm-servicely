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
from lib.action_base import BaseAction


class WatchmanAlertsGet(BaseAction):
    def __init__(self, config):
        super(WatchmanAlertsGet, self).__init__(config)

    def run(
        self,
        queue_name,
        subject,
        watchman_api_key,
        watchman_server,
        watchman_page_size,
        servicely_server,
        servicely_endpoint,
        servicely_token,
        execution_id=None,
        filter_criteria=None
    ):
        url = f"https://{watchman_server}/v2.5/computers"
        params = {
            'api_key': watchman_api_key,
            'per_page': watchman_page_size,
            'expand[]': 'plugin_results'
        }

        self.logger.info(
            f"Fetching alerts from Watchman: {watchman_server}"
        )

        try:
            all_computers = self.fetch_paginated_data(url, params)
            self.logger.info(
                f"Retrieved {len(all_computers)} computers from Watchman"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to fetch alerts from Watchman: {str(e)}"
            )
            raise

        if filter_criteria:
            self.logger.info(
                f"Applying {len(filter_criteria)} filter criteria blocks"
            )
            filtered_computers = self.filter_alerts(
                all_computers,
                filter_criteria
            )
            self.logger.info(
                f"Filtered to {len(filtered_computers)} computers with alerts"
            )
        else:
            filtered_computers = [
                c for c in all_computers
                if 'plugin_results' in c and c['plugin_results']
            ]
            self.logger.info(
                f"No filters, {len(filtered_computers)} computers with alerts"
            )

        try:
            chunks_posted = self.post_data_in_chunks(
                data=filtered_computers,
                queue_name=queue_name,
                subject=subject,
                server=servicely_server,
                endpoint=servicely_endpoint,
                token=servicely_token,
                execution_id=execution_id
            )

            self.logger.info(
                f"Completed: {len(filtered_computers)} computers "
                f"in {chunks_posted} requests"
            )

            return {
                'success': True,
                'computer_count': len(filtered_computers),
                'chunks_posted': chunks_posted,
                'queue': queue_name
            }

        except Exception as e:
            self.logger.error(
                f"Failed to post alerts to Servicely: {str(e)}"
            )
            try:
                error_payload = {
                    'success': False,
                    'error': str(e),
                    'computer_count': len(filtered_computers)
                }
                self.post_to_servicely_queue(
                    queue_name=queue_name,
                    subject=subject,
                    payload=error_payload,
                    server=servicely_server,
                    endpoint=servicely_endpoint,
                    token=servicely_token,
                    execution_id=execution_id,
                    state="error"
                )
            except Exception as post_error:
                self.logger.error(
                    f"Failed to post error to queue: {str(post_error)}"
                )
            raise

    def filter_alerts(self, computers, filter_criteria):
        sorted_criteria = sorted(
            filter_criteria,
            key=lambda x: int(x.get('order', 999))
        )

        filtered_computers = []

        for computer in computers:
            if 'plugin_results' not in computer:
                continue
            if not computer['plugin_results']:
                continue

            matching_plugin_results = []
            for plugin_result in computer['plugin_results']:
                for criteria_block in sorted_criteria:
                    if self.matches_criteria_block(
                        plugin_result,
                        criteria_block
                    ):
                        matching_plugin_results.append(plugin_result)
                        break

            if matching_plugin_results:
                computer_copy = computer.copy()
                computer_copy['plugin_results'] = matching_plugin_results
                filtered_computers.append(computer_copy)

        return filtered_computers

    def matches_criteria_block(self, plugin_result, criteria_block):
        criteria_list = criteria_block.get('criteria', [])

        for criterion in criteria_list:
            field = criterion.get('field')
            operator = criterion.get('operator')
            value = criterion.get('value', '')

            if field not in plugin_result:
                return False

            field_value = str(plugin_result[field])

            if operator == '=':
                if field_value.lower() != value.lower():
                    return False
            elif operator == '!=':
                if field_value.lower() == value.lower():
                    return False
            elif operator == '*':
                if value.lower() not in field_value.lower():
                    return False
            elif operator == 'In':
                valid_values = [v.strip().lower() for v in value.split(',')]
                if field_value.lower() not in valid_values:
                    return False
            else:
                self.logger.warning(
                    f"Unknown operator: {operator}"
                )
                return False

        return True
