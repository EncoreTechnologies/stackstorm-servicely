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


class WatchmanComputersGet(BaseAction):
    def __init__(self, config):
        super(WatchmanComputersGet, self).__init__(config)

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
        c_parent=None
    ):
        url = f"https://{watchman_server}/v2.5/computers"
        params = {
            'api_key': watchman_api_key,
            'per_page': watchman_page_size
        }

        self.logger.info(
            f"Fetching computers from Watchman: {watchman_server}"
        )

        try:
            result = self.fetch_and_post_paginated_data(
                url=url,
                params=params,
                queue_name=queue_name,
                subject=subject,
                server=servicely_server,
                endpoint=servicely_endpoint,
                token=servicely_token,
                execution_id=execution_id,
                c_parent=c_parent
            )

            self.logger.info(
                f"Completed: {result['total_items']} computers "
                f"in {result['pages_posted']} requests"
            )

            return {
                'success': True,
                'computer_count': result['total_items'],
                'pages_posted': result['pages_posted'],
                'queue': queue_name
            }

        except Exception as e:
            self.logger.error(
                f"Failed to process computers: {str(e)}"
            )
            try:
                error_payload = {
                    'success': False,
                    'error': str(e)
                }
                self.post_to_servicely_queue(
                    queue_name=queue_name,
                    subject=subject,
                    payload=error_payload,
                    server=servicely_server,
                    endpoint=servicely_endpoint,
                    token=servicely_token,
                    execution_id=execution_id,
                    state="error",
                    c_parent=c_parent
                )
            except Exception as post_error:
                self.logger.error(
                    f"Failed to post error to queue: {str(post_error)}"
                )
            raise
