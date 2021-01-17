# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_AWS_KEY
from tests.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context

ROLE_ANR = os.environ.get('GCP_AWS_ROLE_ANR', "arn:aws:iam::123456:role/role_arn")
AUDIENCE = os.environ.get('GCP_AWS_AUDIENCE', 'aws-federation.airflow.apache.org')


@pytest.mark.system("google.cloud")
@pytest.mark.credential_file(GCP_AWS_KEY)
class AwsBaseHookSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_AWS_KEY)
    def test_run_example_gcp_vision_autogenerated_id_dag(self):
        mock_connection = Connection(
            conn_type="aws",
            extra=json.dumps(
                {
                    "role_arn": ROLE_ANR,
                    "assume_role_method": "assume_role_with_web_identity",
                    "assume_role_with_web_identity_federation": 'google',
                    "assume_role_with_web_identity_federation_audience": AUDIENCE,
                }
            ),
        )

        with mock.patch.dict('os.environ', AIRFLOW_CONN_AWS_DEFAULT=mock_connection.get_uri()):
            hook = AwsBaseHook(client_type='s3')

            client = hook.get_conn()
            response = client.list_buckets()
            assert 'Buckets' in response
