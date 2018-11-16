import time
import random

from . import api
from .data import JobSpec, Dag


class Job:
    def __init__(self, client, id, attributes=None, _status=None):
        if attributes is None:
            attributes = {}

        self.client = client
        self.id = id
        self.attributes = attributes
        self._status = _status

    def is_complete(self):
        if self._status:
            state = self._status['state']
            if state in ('Complete', 'Cancelled'):
                return True
        return False

    def cached_status(self):
        assert self._status is not None
        return self._status

    def status(self):
        self._status = self.client._get_job(self.id)
        return self._status

    def wait(self):
        i = 0
        while True:
            self.status()  # update
            if self.is_complete():
                return self._status
            j = random.randrange(2 ** i)
            time.sleep(0.100 * j)
            # max 5.12s
            if i < 9:
                i = i + 1

    def cancel(self):
        self.client._cancel_job(self.id)

    def delete(self):
        self.client._delete_job(self.id)

        self.id = None
        self.attributes = None
        self._status = None

    def log(self):
        return self.client._get_job_log(self.id)


class Batch:
    def __init__(self, client, id):
        self.client = client
        self.id = id

    def create_job(self, image, command=None, args=None, env=None, ports=None,
                   resources=None, tolerations=None, volumes=None, security_context=None,
                   service_account_name=None, attributes=None, callback=None):
        return self.client._create_job(
            JobSpec.from_parameters(
                image, command, args, env, ports, resources, tolerations, volumes,
                security_context, service_account_name, attributes, callback),
            self.id)

    def status(self):
        return self.client._get_batch(self.id)

    def wait(self):
        i = 0
        while True:
            status = self.status()
            if status['jobs']['Created'] == 0:
                return status
            j = random.randrange(2 ** i)
            time.sleep(0.100 * j)
            # max 5.12s
            if i < 9:
                i = i + 1


class BatchClient:
    def __init__(self, url=None, api=api.DEFAULT_API):
        if not url:
            url = 'http://batch.default'
        self.url = url
        self.api = api

    def _create_job(self, spec, batch_id):
        j = self.api.create_job(self.url, spec, batch_id)
        return Job(self, j['id'], j.get('attributes'))

    def _get_job(self, id):
        return self.api.get_job(self.url, id)

    def _get_job_log(self, id):
        return self.api.get_job_log(self.url, id)

    def _delete_job(self, id):
        self.api.delete_job(self.url, id)

    def _cancel_job(self, id):
        self.api.cancel_job(self.url, id)

    def _get_batch(self, batch_id):
        return self.api.get_batch(self.url, batch_id)

    def _refresh_k8s_state(self):
        self.api.refresh_k8s_state(self.url)

    def list_jobs(self):
        jobs = self.api.list_jobs(self.url)
        return [Job(self, j['id'], j.get('attributes'), j) for j in jobs]

    def get_job(self, id):
        # make sure job exists
        j = self.api.get_job(self.url, id)
        return Job(self, j['id'], j.get('attributes'), j)

    def create_job(self,
                   image,
                   command=None,
                   args=None,
                   env=None,
                   ports=None,
                   resources=None,
                   tolerations=None,
                   volumes=None,
                   security_context=None,
                   service_account_name=None,
                   attributes=None,
                   callback=None):
        return self._create_job(
            JobSpec.from_parameters(
                image, command, args, env, ports, resources, tolerations, volumes,
                security_context, service_account_name, attributes, callback),
            None)

    def create_batch(self, attributes=None):
        batch = self.api.create_batch(self.url, attributes)
        return Batch(self, batch['id'])

    def create_dag(self, nodes):
        return self.api.create_dag(self.url, Dag(nodes))

    def get_dag(self, id):
        return Dag.from_json(self.api.get_dag(self.url, id))
