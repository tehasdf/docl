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

import sched
import tempfile
import time
import threading

import argh
import sh
import yaml
from watchdog import events
from watchdog import observers
from path import path

from docl import constants
from docl import resources
from docl.configuration import configuration
from docl.work import work
from docl.subprocess import docker
from docl.subprocess import ssh_keygen
from docl.subprocess import cfy
from docl.logs import logger

app = argh.EntryPoint('cloudify-docker')
command = app


@command
@argh.arg('--simple-manager-blueprint-path', required=True)
def init(simple_manager_blueprint_path=None,
         docker_host='fd://',
         ssh_key_path='~/.ssh/.id_rsa',
         clean_image_docker_tag='cloudify/centos-manager:7',
         installed_image_docker_tag='cloudify/centos-manager-installed:7',
         source_root='~/dev/cloudify',
         workdir=None,
         reset=False):
    ssh_key_path = path(ssh_key_path).expanduser()
    simple_manager_blueprint_path = path(
        simple_manager_blueprint_path).expanduser()
    required_files = {
        simple_manager_blueprint_path: 'You must specify a path '
                                       'to a simple manager blueprint',
        ssh_key_path: 'You need to create a key (see man ssh-keygen) first',
    }
    for required_file, message in required_files.items():
        if not required_file.isfile():
            raise argh.CommandError(message)
    ssh_public_key = ssh_keygen('-y', '-f', ssh_key_path).strip()
    configuration.save(
        docker_host=docker_host,
        simple_manager_blueprint_path=simple_manager_blueprint_path.abspath(),
        ssh_key_path=ssh_key_path.abspath(),
        ssh_public_key=ssh_public_key,
        clean_image_docker_tag=clean_image_docker_tag,
        installed_image_docker_tag=installed_image_docker_tag,
        source_root=source_root,
        workdir=workdir,
        reset=reset)
    work.init()


@command
@argh.arg('-i', '--inputs', action='append')
def bootstrap(inputs=None):
    inputs = inputs or []
    container_id, container_ip = _create_base_container()
    _ssh_setup(container_id=container_id, container_ip=container_ip)
    _cfy_bootstrap(container_ip=container_ip, inputs=inputs)


@command
def save_image(container_id=None):
    container_id = container_id or work.last_container_id
    docker_tag = configuration.installed_image_docker_tag
    docker.stop(container_id)
    docker.commit(container_id, docker_tag)
    docker.rm('-f', container_id)


@command
def run(mount=False):
    docker_tag = configuration.installed_image_docker_tag
    volumes = _build_volumes() if mount else None
    _run_container(docker_tag=docker_tag, volume=volumes)


@command
def clean():
    docker_tag = configuration.installed_image_docker_tag
    containers = docker.ps(
        '-aq', '--filter', 'ancestor={}'.format(docker_tag)).split('\n')
    containers = [c.strip() for c in containers if c.strip()]
    if containers:
        docker.rm('-f', ' '.join(containers))


@command
def restart_services(container_id=None):
    container_id = container_id or work.last_container_id
    for service in constants.SERVICES:
        _restart_service(container_id, service)


@command
def watch(container_id=None):
    container_id = container_id or work.last_container_id
    services_to_restart = set()
    services_to_restart_lock = threading.Lock()

    class Handler(events.FileSystemEventHandler):
        def __init__(self, services):
            self.services = services

        def on_modified(self, event):
            if event.is_directory:
                with services_to_restart_lock:
                    services_to_restart.update(self.services)
    observer = observers.Observer()
    for package, services in constants.PACKAGE_DIR_SERVICES.items():
        src = '{}/{}/{}'.format(configuration.source_root,
                                constants.PACKAGE_DIR[package],
                                package)
        observer.schedule(Handler(services), path=src, recursive=True)
    observer.start()

    scheduler = sched.scheduler(time.time, time.sleep)

    def restart_changed_services():
        with services_to_restart_lock:
            current_services_to_restart = services_to_restart.copy()
            services_to_restart.clear()
        for service in current_services_to_restart:
            _restart_service(container_id, service)
        scheduler.enter(2, 1, restart_changed_services, ())
    restart_changed_services()

    try:
        scheduler.run()
    except KeyboardInterrupt:
        pass


def _restart_service(container_id, service):
    service_name = 'cloudify-{}'.format(service)
    logger.info('Restarting {}'.format(service_name))
    docker('exec', container_id, 'systemctl', 'restart', service_name)


def _build_volumes():
    volumes = []
    for env, packages in constants.ENV_PACKAGES.items():
        for package in packages:
            src = '{}/{}/{}'.format(configuration.source_root,
                                    constants.PACKAGE_DIR[package],
                                    package)
            dst = '/opt/{}/env/lib/python2.7/site-packages/{}'.format(env,
                                                                      package)
            volumes.append('{}:{}:ro'.format(src, dst))
    return volumes


def _create_base_container():
    docker_tag = configuration.clean_image_docker_tag
    docker.build('-t', configuration.clean_image_docker_tag, resources.DIR)
    container_id, container_ip = _run_container(docker_tag=docker_tag,
                                                expose=constants.EXPOSE,
                                                publish=constants.PUBLISH)
    docker('exec', container_id, 'systemctl', 'start', 'dbus')
    return container_id, container_ip


def _run_container(docker_tag, expose=None, publish=None, volume=None):
    expose = expose or []
    publish = publish or []
    volume = volume or []
    container_id = docker.run(*['--privileged', '--detach'] +
                               ['--hostname=cfy-manager'] +
                               ['--expose={}'.format(e) for e in expose] +
                               ['--publish={}'.format(p) for p in publish] +
                               ['--volume={}'.format(v) for v in volume] +
                               [docker_tag]).strip()
    container_ip = docker.inspect(
        '--format={{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',
        container_id,
    ).strip()
    work.save_last_container_id_and_ip(container_id=container_id,
                                       container_ip=container_ip)
    return container_id, container_ip


def _ssh_setup(container_id, container_ip):
    ssh_keygen('-R', container_ip)
    try:
        docker('exec', container_id, 'mkdir', '-m700', '/root/.ssh')
    except sh.ErrorReturnCode as e:
        if e.exit_code != 1:
            raise
    with tempfile.NamedTemporaryFile() as f:
        f.write(configuration.ssh_public_key)
        f.flush()
        docker.cp(f.name, '{}:/root/.ssh/authorized_keys'.format(container_id))


def _cfy_bootstrap(container_ip, inputs):
    with tempfile.NamedTemporaryFile() as f:
        f.write(yaml.safe_dump({
            'public_ip': container_ip,
            'private_ip': container_ip,
            'ssh_user': 'root',
            'ssh_key_filename': str(configuration.ssh_key_path),
        }))
        f.flush()
        inputs.insert(0, f.name)
        cfy.init('-r')
        cfy.bootstrap(
            blueprint_path=configuration.simple_manager_blueprint_path,
            *['--inputs={}'.format(i) for i in inputs])