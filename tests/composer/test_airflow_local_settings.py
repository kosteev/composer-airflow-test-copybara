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
from __future__ import annotations

import datetime
import os
import unittest
from unittest import mock

from kubernetes.client import models as k8s
from parameterized import parameterized

from airflow import settings
from airflow.composer.airflow_local_settings import dag_policy, pod_mutation_hook
from airflow.models import DAG
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ
from tests.test_utils.config import conf_vars


class TestAirflowLocalSettings(unittest.TestCase):
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_dag_rbac_per_folder_policy(self):
        role_a_dag = DAG(
            dag_id="role_a_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_a_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_a/dag.py")
        role_b_dag = DAG(
            dag_id="role_b_dag",
            start_date=datetime.datetime(2017, 1, 1),
            access_control={
                "role_b": {"test_permission"},
                "admin": {"admin_permission"},
            },
        )
        role_b_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_b/dag.py")
        root_dag = DAG(
            dag_id="root_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        root_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "dag.py")
        role_length_exceed_dag = DAG(
            dag_id="role_length_exceed_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_length_exceed_dag.fileloc = os.path.join(settings.DAGS_FOLDER, f"role_{'x' * 70}/dag.py")

        dag_policy(role_a_dag)
        dag_policy(role_b_dag)
        dag_policy(root_dag)
        dag_policy(role_length_exceed_dag)

        assert role_a_dag.access_control == {"role_a": {ACTION_CAN_EDIT, ACTION_CAN_READ}}
        assert role_b_dag.access_control == {
            "role_b": {"test_permission", ACTION_CAN_EDIT, ACTION_CAN_READ},
            "admin": {"admin_permission"},
        }
        assert root_dag.access_control is None
        assert role_length_exceed_dag.access_control is None

    @parameterized.expand(
        [
            (
                "1.20.12",
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default")),
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default")),
            ),
            (
                "2.4.21",
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default")),
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default")),
            ),
            (
                "2.5.0-preview.0",
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default")),
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="composer-user-workloads")),
            ),
        ]
    )
    def test_pod_mutation_hook(self, composer_version, pod, expected_mutated_pod):
        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            pod_mutation_hook(pod)

        assert pod == expected_mutated_pod
