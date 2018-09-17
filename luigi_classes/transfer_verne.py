import datetime
import os
import filecmp

import luigi
from paramiko import SSHClient
from paramiko
from scp import SCPClient

from config_classes import VerneConfig
from xchem_db.models import *


class TransferDirectory(luigi.Task):
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_directory = luigi.Parameter()
    local_directory = luigi.Parameter()

    def run(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)
        sftp = ssh.open_sftp()
        try:
            sftp.stat(self.remote_directory)
        except FileNotFoundError:
            scp = SCPClient(ssh.get_transport())
            scp.put(self.local_directory, recursive=True, remote_path=self.remote_directory)
            scp.close()


# timestamp = datetime.datetime.now().strftime('%Y-%m%-dT%H')
# old_dir = # find previous directory on verne
# top_l_verne = '/data/fs-data/django_data'
# upload_to = os.path.join(top_l_verne, str(timestamp))
# compare files in lists with filecmp and update if necessary


class GetTransferDirectories(luigi.Task):
    remote_root = VerneConfig().remote_root
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m%-%dT%H'))

    def requires(self):
        proasis_out = ProasisOut.objects.all()
        paths = list(set([os.path.join(o.root, o.start) for o in proasis_out if o.root and o.start]))
        transfer_checks = []

        for p in paths:
            if os.path.isdir(p):
                transfer_checks.append(p)

        return [TransferDirectory(remote_directory=os.path.join(self.remote_root, self.timestamp,
                                                                '/'.join(p.split('/')[-3:])), local_directory=p)
                for p in transfer_checks]



