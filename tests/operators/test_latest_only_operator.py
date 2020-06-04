#
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

import datetime
import unittest

from freezegun import freeze_time

from airflow import settings
from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)


def get_task_instances(task_id):
    session = settings.Session()
    return session \
        .query(TaskInstance) \
        .filter(TaskInstance.task_id == task_id) \
        .order_by(TaskInstance.execution_date) \
        .all()


class TestLatestOnlyOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()
        freezer = freeze_time(FROZEN_NOW)
        freezer.start()
        self.addCleanup(freezer.stop)

    def test_run(self):
        task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_skipping_non_latest(self):
        latest_task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        downstream_task = DummyOperator(
            task_id='downstream',
            dag=self.dag)
        downstream_task2 = DummyOperator(
            task_id='downstream_2',
            dag=self.dag)
        downstream_task3 = DummyOperator(
            task_id='downstream_3',
            trigger_rule=TriggerRule.NONE_FAILED,
            dag=self.dag)

        downstream_task.set_upstream(latest_task)
        downstream_task2.set_upstream(downstream_task)
        downstream_task3.set_upstream(downstream_task)

        self.dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            execution_date=timezone.datetime(2016, 1, 1, 12),
            state=State.RUNNING,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            execution_date=END_DATE,
            state=State.RUNNING,
        )

        latest_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task2.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task3.run(start_date=DEFAULT_DATE, end_date=END_DATE)

        latest_instances = get_task_instances('latest')
        exec_date_to_latest_state = {
            ti.execution_date: ti.state for ti in latest_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_latest_state)

        downstream_instances = get_task_instances('downstream')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'skipped',
            timezone.datetime(2016, 1, 1, 12): 'skipped',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_downstream_state)

        downstream_instances = get_task_instances('downstream_2')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): None,
            timezone.datetime(2016, 1, 1, 12): None,
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_downstream_state)

        downstream_instances = get_task_instances('downstream_3')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_downstream_state)

    def test_not_skipping_external(self):
        latest_task = LatestOnlyOperator(
            task_id='latest',
            dag=self.dag)
        downstream_task = DummyOperator(
            task_id='downstream',
            dag=self.dag)
        downstream_task2 = DummyOperator(
            task_id='downstream_2',
            dag=self.dag)

        downstream_task.set_upstream(latest_task)
        downstream_task2.set_upstream(downstream_task)

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=True,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=timezone.datetime(2016, 1, 1, 12),
            state=State.RUNNING,
            external_trigger=True,
        )

        self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=END_DATE,
            state=State.RUNNING,
            external_trigger=True,
        )

        latest_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task.run(start_date=DEFAULT_DATE, end_date=END_DATE)
        downstream_task2.run(start_date=DEFAULT_DATE, end_date=END_DATE)

        latest_instances = get_task_instances('latest')
        exec_date_to_latest_state = {
            ti.execution_date: ti.state for ti in latest_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_latest_state)

        downstream_instances = get_task_instances('downstream')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_downstream_state)

        downstream_instances = get_task_instances('downstream_2')
        exec_date_to_downstream_state = {
            ti.execution_date: ti.state for ti in downstream_instances}
        self.assertEqual({
            timezone.datetime(2016, 1, 1): 'success',
            timezone.datetime(2016, 1, 1, 12): 'success',
            timezone.datetime(2016, 1, 2): 'success'},
            exec_date_to_downstream_state)
