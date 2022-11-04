#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
from typing import Dict

from airflow import AirflowException
from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger(__name__)


class BigQueryToBigQueryOperatorLineageMixin:
    """Mixin class for BigQueryToBigQueryOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        except AirflowException as airflow_exception:
            log.exception(f'Error on creating hook: {airflow_exception}')
            return

        sources = self.source_project_dataset_tables
        if isinstance(sources, str):
            sources = [sources]

        keys = ('project_id', 'dataset_id', 'table_id')
        inlets = []
        for table_name in sources:
            try:
                source_items = hook.split_tablename(
                    table_input=table_name, default_project_id=hook.project_id
                )
            except Exception:
                log.exception(f'Error on parsing table name: "{table_name}"')
                return
            inlets.append(BigQueryTable(**dict(zip(keys, source_items))))

        outlets = []
        try:
            destination_items = hook.split_tablename(
                table_input=self.destination_project_dataset_table, default_project_id=hook.project_id
            )
        except Exception:
            log.exception(f'Error on parsing table name: "{self.destination_project_dataset_table}"')
            return
        outlets.append(BigQueryTable(**dict(zip(keys, destination_items))))

        self.inlets.extend(inlets)
        self.outlets.extend(outlets)
