########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import tempfile
from contextlib import contextmanager

import yaml

from docl import files, resources
from docl.configuration import configuration
from docl.work import work
from docl.logs import logger
from docl.subprocess import quiet_docker


@contextmanager
def with_server(invalidate_cache, no_progress=False, network='bridge'):
    server = FileServer(work.dir, network=network)
    local_resources_url = server.start(invalidate_cache=invalidate_cache,
                                       no_progress=no_progress)
    try:
        yield local_resources_url
    finally:
        server.stop()


class FileServer(object):
    def __init__(self, directory, network, port=9797):
        self._directory = directory
        self._network = network
        self._port = port
        self._container_id = None

    def start(self, invalidate_cache=False, no_progress=False):
        if invalidate_cache or not work.cached_resources_tar_path.exists():
            _download_resources_tar(no_progress=no_progress)

        with tempfile.NamedTemporaryFile(delete=False) as nginx_conf:
            config_template = resources.get('nginx_conf.tmpl')
            config_template = config_template.replace('DOCL_NGINX_PORT',
                                                      '{0}'.format(self._port))
            nginx_conf.write(config_template)

        volume_desc = [
            '{0}:/etc/nginx/nginx.conf:ro'.format(nginx_conf.name),
            '{0}:/usr/share/nginx/html:ro'.format(self._directory)
        ]
        run_container = quiet_docker.run.bake(name='docl-fileserver',
                                              d=True,
                                              network=self._network)
        for volume in volume_desc:
            run_container = run_container.bake(v=volume)
        self._container_id = run_container('nginx:1.11').wait().rstrip()

        inspect_command = quiet_docker.inspect.bake(self._container_id)
        inspect_result = inspect_command()
        inspect_result.wait()
        inspect_data = json.loads(inspect_result.stdout)

        fileserver_address = inspect_data[0]['NetworkSettings']['Networks'][
            self._network]['IPAddress']
        local_resources_url = 'http://{}:{}/{}'.format(
            fileserver_address, self._port,
            work.cached_resources_tar_path.basename())
        logger.info('Resources tar available at {}'
                    .format(local_resources_url))
        return local_resources_url

    def stop(self):
        if self._container_id is None:
            raise RuntimeError('FileServer: tried to stop container before '
                               'starting it')
        quiet_docker.stop(self._container_id)
        quiet_docker.rm(self._container_id)


def get_host():
    host = configuration.docker_host
    if '://' in host:
        host = host.split('://')[1]
    if ':' in host:
        host = host.split(':')[0]
    return host


def _download_resources_tar(no_progress):
    resources_local_path = work.cached_resources_tar_path
    if resources_local_path.exists():
        resources_local_path.unlink()
    resources_url = _get_resources_url()
    logger.info('Downloading resources tar from {}. This might take a '
                'while'.format(resources_url))
    files.download(url=resources_url,
                   output_path=resources_local_path,
                   no_progress=no_progress)


def _get_resources_url():
    manager_blueprint_path = configuration.simple_manager_blueprint_path
    manager_inputs_path = (manager_blueprint_path.dirname() / 'inputs' /
                           'manager-inputs.yaml')
    manager_inputs = yaml.safe_load(manager_inputs_path.text())
    return manager_inputs[
        'inputs']['manager_resources_package']['default']
