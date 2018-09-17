import datetime
import os

import luigi
from paramiko import SSHClient
from scp import SCPClient

# import setup_django
# setup_django.setup_django()

from .config_classes import VerneConfig
from xchem_db.models import *
from luigi_classes.pull_proasis import GetOutFiles


class TransferDirectory(luigi.Task):
    # hidden parameters in luigi.cfg
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # normal parameters
    remote_directory = luigi.Parameter()
    local_directory = luigi.Parameter()
    timestamp = luigi.Parameter()

    def requires(self):
        return GetOutFiles(date=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(str(self.local_directory + '_' + self.timestamp + '.transferred'))

    def run(self):
        # create SSH client with paramiko and connect with system host keys
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)
        sftp = ssh.open_sftp()

        # see if the remote directory exists
        try:
            sftp.stat(self.remote_directory)
        # if not, then recursivley add each file in the path
        except FileNotFoundError:
            f_path = ''
            for f in self.remote_directory.replace(self.remote_root, '').split('/'):
                f_path += str('/' + f)
                try:
                    sftp.stat(str(self.remote_root + f_path))
                except FileNotFoundError:
                    sftp.mkdir(str(self.remote_root + f_path))
            # set up scp protocol and recursively push the directories across
            scp = SCPClient(ssh.get_transport())
            scp.put(self.local_directory, recursive=True, remote_path=self.remote_directory)
            scp.close()

        # write local output file to signify transfer done
        with self.output().open('w') as f:
            f.write('')


class GetTransferDirectories(luigi.Task):
    remote_root = VerneConfig().remote_root
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H'))

    def output(self):
        return luigi.LocalTarget(str('logs/transfer_verne_' + self.timestamp + '.done'))

    def requires(self):
        # get all output directories in the database
        proasis_out = ProasisOut.objects.all()
        # get all paths that have values
        paths = list(set([os.path.join(o.root, o.start) for o in proasis_out if o.root and o.start]))
        transfer_checks = []
        # check that the paths exist
        for p in paths:
            if os.path.isdir(p):
                transfer_checks.append(p)
        # run a file transfer to verne for each directory
        return [TransferDirectory(remote_directory=os.path.join(self.remote_root, self.timestamp,
                                                                '/'.join(p.split('/')[-3:])), local_directory=p)
                for p in transfer_checks]

    def run(self):
        # write local output file to signify all transfers done
        with self.output().open('w') as f:
            f.write('')
