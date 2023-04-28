#
# Copyright 2020 Google LLC
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
"""Airflow local settings."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.composer.utils import is_serverless_composer

if TYPE_CHECKING:
    from kubernetes.client import models as k8s

USER_WORKLOADS_NAMESPACE = "composer-user-workloads"


def dag_policy(dag):
    """Applies per-DAG policy."""
    # Avoid circular imports by moving imports inside method.
    from airflow.composer.dag_rbac_per_folder import apply_dag_rbac_per_folder_policy
    from airflow.configuration import conf

    if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
        apply_dag_rbac_per_folder_policy(dag)


def pod_mutation_hook(pod: k8s.V1Pod):
    if is_serverless_composer():
        pod.metadata.namespace = USER_WORKLOADS_NAMESPACE
