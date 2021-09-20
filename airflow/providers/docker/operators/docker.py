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
"""Implements Docker operator"""
import ast
import io
import pickle
import tarfile
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Union

from docker import APIClient, tls
from docker.errors import APIError
from docker.types import Mount

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.docker.hooks.docker import DockerHook


class DockerOperator(BaseOperator):
    """
    Execute a command inside a docker container.

    By default, a temporary directory is
    created on the host and mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container.
    In this case The path to the mounted directory can be accessed
    via the environment variable ``AIRFLOW_TMP_DIR``.

    If the volume cannot be mounted, warning is printed and an attempt is made to execute the docker
    command without the temporary folder mounted. This is to make it works by default with remote docker
    engine or when you run docker-in-docker solution and temporary directory is not shared with the
    docker engine. Warning is printed in logs in this case.

    If you know you run DockerOperator with remote engine or via docker-in-docker
    you should set ``mount_tmp_dir`` parameter to False. In this case, you can still use
    ``mounts`` parameter to mount already existing named volumes in your Docker Engine
    to achieve similar capability where you can store files exceeding default disk size
    of the container,

    If a login to a private registry is required prior to pulling the image, a
    Docker connection needs to be configured in Airflow and the connection ID
    be provided with the parameter ``docker_conn_id``.

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param command: Command to be run in the container. (templated)
    :type command: str or list
    :param container_name: Name of the container. Optional (templated)
    :type container_name: str or None
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param docker_url: URL of the host running the docker daemon.
        Default is unix://var/run/docker.sock
    :type docker_url: str
    :param environment: Environment variables to set in the container. (templated)
    :type environment: dict
    :param private_environment: Private environment variables to set in the container.
        These are not templated, and hidden from the website.
    :type private_environment: dict
    :param force_pull: Pull the docker image on every run. Default is False.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use.
        Either a float value, which represents the limit in bytes,
        or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
    :param host_tmp_dir: Specify the location of the temporary directory on the host which will
        be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
    :type host_tmp_dir: str
    :param network_mode: Network mode for the container.
    :type network_mode: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority
        to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate
        used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_hostname: Hostname to match against
        the docker server certificate or False to disable the check.
    :type tls_hostname: str or bool
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param mount_tmp_dir: Specify whether the temporary directory should be bind-mounted
        from the host to the container. Defaults to True
    :type mount_tmp_dir: bool
    :param tmp_dir: Mount point inside the container to
        a temporary directory created on the host by the operator.
        The path is also made available via the environment variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param mounts: List of volumes to mount into the container. Each item should
        be a :py:class:`docker.types.Mount` instance.
    :type mounts: list[docker.types.Mount]
    :param entrypoint: Overwrite the default ENTRYPOINT of the image
    :type entrypoint: str or list
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :type working_dir: str
    :param xcom_all: Push all the stdout or just the last line.
        The default is False (last line).
    :type xcom_all: bool
    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
    :type docker_conn_id: str
    :param dns: Docker custom DNS servers
    :type dns: list[str]
    :param dns_search: Docker custom DNS search domain
    :type dns_search: list[str]
    :param auto_remove: Auto-removal of the container on daemon side when the
        container's process exits.
        The default is False.
    :type auto_remove: bool
    :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
        greater than 0. If omitted uses system default.
    :type shm_size: int
    :param tty: Allocate pseudo-TTY to the container
        This needs to be set see logs of the Docker container.
    :type tty: bool
    :param privileged: Give extended privileges to this container.
    :type privileged: bool
    :param cap_add: Include container capabilities
    :type cap_add: list[str]
    :param retrieve_output: Should this docker image consistently attempt to pull from and output
        file before manually shutting down the image. Useful for cases where users want a pickle serialized
        output that is not posted to logs
    :type retrieve_output: bool
    :param retrieve_output_path: path for output file that will be retrieved and passed to xcom
    :type retrieve_output_path: Optional[str]
    """

    template_fields = ('command', 'environment', 'container_name')
    template_ext = (
        '.sh',
        '.bash',
    )

    def __init__(
        self,
        *,
        image: str,
        api_version: Optional[str] = None,
        command: Optional[Union[str, List[str]]] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        docker_url: str = 'unix://var/run/docker.sock',
        environment: Optional[Dict] = None,
        private_environment: Optional[Dict] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_hostname: Optional[Union[str, bool]] = None,
        tls_ssl_version: Optional[str] = None,
        mount_tmp_dir: bool = True,
        tmp_dir: str = '/tmp/airflow',
        user: Optional[Union[str, int]] = None,
        mounts: Optional[List[Mount]] = None,
        entrypoint: Optional[Union[str, List[str]]] = None,
        working_dir: Optional[str] = None,
        xcom_all: bool = False,
        docker_conn_id: Optional[str] = None,
        dns: Optional[List[str]] = None,
        dns_search: Optional[List[str]] = None,
        auto_remove: bool = False,
        shm_size: Optional[int] = None,
        tty: bool = False,
        privileged: bool = False,
        cap_add: Optional[Iterable[str]] = None,
        extra_hosts: Optional[Dict[str, str]] = None,
        retrieve_output: bool = False,
        retrieve_output_path: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.api_version = api_version
        self.auto_remove = auto_remove
        self.command = command
        self.container_name = container_name
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.docker_url = docker_url
        self.environment = environment or {}
        self._private_environment = private_environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.host_tmp_dir = host_tmp_dir
        self.network_mode = network_mode
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version
        self.mount_tmp_dir = mount_tmp_dir
        self.tmp_dir = tmp_dir
        self.user = user
        self.mounts = mounts or []
        self.entrypoint = entrypoint
        self.working_dir = working_dir
        self.xcom_all = xcom_all
        self.docker_conn_id = docker_conn_id
        self.shm_size = shm_size
        self.tty = tty
        self.privileged = privileged
        self.cap_add = cap_add
        self.extra_hosts = extra_hosts
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

        self.cli = None
        self.container = None
        self.retrieve_output = retrieve_output
        self.retrieve_output_path = retrieve_output_path

    def get_hook(self) -> DockerHook:
        """
        Retrieves hook for the operator.

        :return: The Docker Hook
        """
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=self.__get_tls_config(),
        )

    def _run_image(self) -> Optional[str]:
        """Run a Docker container with the provided image"""
        self.log.info('Starting docker container from image %s', self.image)
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")
        if self.mount_tmp_dir:
            with TemporaryDirectory(prefix='airflowtmp', dir=self.host_tmp_dir) as host_tmp_dir_generated:
                tmp_mount = Mount(self.tmp_dir, host_tmp_dir_generated, "bind")
                try:
                    return self._run_image_with_mounts(self.mounts + [tmp_mount], add_tmp_variable=True)
                except APIError as e:
                    if host_tmp_dir_generated in str(e):
                        self.log.warning(
                            "Using remote engine or docker-in-docker and mounting temporary "
                            "volume from host is not supported. Falling back to "
                            "`mount_tmp_dir=False` mode. You can set `mount_tmp_dir` parameter"
                            " to False to disable mounting and remove the warning"
                        )
                        return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)
                    raise
        else:
            return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)

    def _run_image_with_mounts(self, target_mounts, add_tmp_variable: bool) -> Optional[str]:
        if add_tmp_variable:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
        else:
            self.environment.pop('AIRFLOW_TMP_DIR', None)
        self.container = self.cli.create_container(
            command=self.format_command(self.command),
            name=self.container_name,
            environment={**self.environment, **self._private_environment},
            host_config=self.cli.create_host_config(
                auto_remove=False,
                mounts=target_mounts,
                network_mode=self.network_mode,
                shm_size=self.shm_size,
                dns=self.dns,
                dns_search=self.dns_search,
                cpu_shares=int(round(self.cpus * 1024)),
                mem_limit=self.mem_limit,
                cap_add=self.cap_add,
                extra_hosts=self.extra_hosts,
                privileged=self.privileged,
            ),
            image=self.image,
            user=self.user,
            entrypoint=self.format_command(self.entrypoint),
            working_dir=self.working_dir,
            tty=self.tty,
        )
        lines = self.cli.attach(container=self.container['Id'], stdout=True, stderr=True, stream=True)
        try:
            self.cli.start(self.container['Id'])

            line = ''
            res_lines = []
            return_value = None
            for line in lines:
                if hasattr(line, 'decode'):
                    # Note that lines returned can also be byte sequences so we have to handle decode here
                    line = line.decode('utf-8')
                line = line.strip()
                res_lines.append(line)
                self.log.info(line)
            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                res_lines = "\n".join(res_lines)
                raise AirflowException('docker container failed: ' + repr(result) + f"lines {res_lines}")
            if self.retrieve_output and not return_value:
                return_value = self._attempt_to_retrieve_result()
            ret = None
            if self.retrieve_output:
                ret = return_value
            elif self.do_xcom_push:
                ret = self._get_return_value_from_logs(res_lines, line)
            return ret
        finally:
            if self.auto_remove:
                self.cli.remove_container(self.container['Id'])

    def _attempt_to_retrieve_result(self):
        """
        Attempts to pull the result of the function from the expected file using docker's
        get_archive function.
        If the file is not yet ready, returns None
        :return:
        """

        def copy_from_docker(container_id, src):
            archived_result, stat = self.cli.get_archive(container_id, src)
            # no need to port to a file since we intend to deserialize
            file_standin = io.BytesIO(b"".join(archived_result))
            tar = tarfile.open(fileobj=file_standin)
            file = tar.extractfile(stat['name'])
            lib = getattr(self, 'pickling_library', pickle)
            return lib.loads(file.read())

        try:
            return_value = copy_from_docker(self.container['Id'], self.retrieve_output_path)
            return return_value
        except APIError:
            return None

    def _get_return_value_from_logs(self, res_lines, line):
        return res_lines if self.xcom_all else line

    def execute(self, context) -> Optional[str]:
        self.cli = self._get_cli()
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")

        # Pull the docker image if `force_pull` is set or image does not exist locally

        if self.force_pull or not self.cli.images(name=self.image):
            self.log.info('Pulling docker image %s', self.image)
            latest_status = {}
            for output in self.cli.pull(self.image, stream=True, decode=True):
                if isinstance(output, str):
                    self.log.info("%s", output)
                    continue
                if isinstance(output, dict) and 'status' in output:
                    output_status = output["status"]
                    if 'id' not in output:
                        self.log.info("%s", output_status)
                        continue

                    output_id = output["id"]
                    if latest_status.get(output_id) != output_status:
                        self.log.info("%s: %s", output_id, output_status)
                        latest_status[output_id] = output_status
        return self._run_image()

    def _get_cli(self) -> APIClient:
        if self.docker_conn_id:
            return self.get_hook().get_conn()
        else:
            tls_config = self.__get_tls_config()
            return APIClient(base_url=self.docker_url, version=self.api_version, tls=tls_config)

    @staticmethod
    def format_command(command: Union[str, List[str]]) -> Union[List[str], str]:
        """
        Retrieve command(s). if command string starts with [, it returns the command list)

        :param command: Docker command or entrypoint
        :type command: str | List[str]

        :return: the command (or commands)
        :rtype: str | List[str]
        """
        if isinstance(command, str) and command.strip().find('[') == 0:
            return ast.literal_eval(command)
        return command

    def on_kill(self) -> None:
        if self.cli is not None:
            self.log.info('Stopping docker container')
            self.cli.stop(self.container['Id'])

    def __get_tls_config(self) -> Optional[tls.TLSConfig]:
        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            # Ignore type error on SSL version here - it is deprecated and type annotation is wrong
            # it should be string
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,
                assert_hostname=self.tls_hostname,
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')
        return tls_config
