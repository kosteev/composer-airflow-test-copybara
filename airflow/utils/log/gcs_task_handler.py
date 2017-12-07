# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.file_task_handler import FileTaskHandler


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.
    """
    def __init__(self, base_log_folder, gcs_log_folder, filename_template):
        super(GCSTaskHandler, self).__init__(base_log_folder, filename_template)
        self.remote_base = gcs_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False

    def _build_hook(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
            return GoogleCloudStorageHook(
                google_cloud_storage_conn_id=remote_conn_id
            )
        except Exception as e:
            self.log.error(
                'Could not create a GoogleCloudStorageHook with connection id '
                '"{}". {}\n\nPlease make sure that airflow[gcp_api] is installed '
                'and the GCS connection exists.'.format(remote_conn_id, str(e))
            )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def set_context(self, ti):
        super(GCSTaskHandler, self).set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        self.log_relative_path = self._render_filename(ti, ti.try_number)

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super(GCSTaskHandler, self).close()

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.gcs_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        try:
            remote_log = self.gcs_read(remote_loc)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
        except Exception as e:
            log = '*** Unable to read remote log from {}\n*** {}\n\n'.format(
                remote_loc, str(e))
            self.log.error(log)
            log += super(GCSTaskHandler, self)._read(ti, try_number)

        return log

    def gcs_read(self, remote_log_location):
        """
        Returns the log found at the remote_log_location.
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        """
        bkt, blob = self.parse_gcs_url(remote_log_location)
        return self.hook.download(bkt, blob).decode()

    def gcs_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.
        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append:
            try:
                old_log = self.gcs_read(remote_log_location)
            except Exception as e:
                old_log = '*** Previous log discarded: {}\n\n'.format(str(e))
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile(mode='w+') as tmpfile:
                tmpfile.write(log)
                # Force the file to be flushed, since we're doing the
                # upload from within the file context (it hasn't been
                # closed).
                tmpfile.flush()
                self.hook.upload(bkt, blob, tmpfile.name)
        except Exception as e:
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)

    def parse_gcs_url(self, gsurl):
        """
        Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
        tuple containing the corresponding bucket and blob.
        """
        # Python 3
        try:
            from urllib.parse import urlparse
        # Python 2
        except ImportError:
            from urlparse import urlparse

        parsed_url = urlparse(gsurl)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket name')
        else:
            bucket = parsed_url.netloc
            blob = parsed_url.path.strip('/')
            return bucket, blob
