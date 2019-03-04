import os
import datetime

import luigi
from paramiko import SSHClient

from xchem_db.models import PanddaEvent, Target, Proposals, Crystal
from .config_classes import VerneConfig


def transfer_file(host_dict, file_dict):
    # create SSH client with paramiko and connect with system host keys
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(host_dict['hostname'], username=host_dict['username'])
    sftp = ssh.open_sftp()

    # see if the remote directory exists
    try:
        sftp.stat(file_dict['remote_directory'])
    # if not, then recursivley add each file in the path
    except FileNotFoundError:
        f_path = ''
        for f in file_dict['remote_directory'].replace(file_dict['remote_root'], '').split('/')[:-1]:
            f_path += str('/' + f)
            print(f_path)
            try:
                sftp.stat(str(file_dict['remote_root'] + f_path))
            except FileNotFoundError:
                sftp.mkdir(str(file_dict['remote_root'] + f_path))

    # set up scp protocol and recursively push the directories across
    # scp = SCPClient(ssh.get_transport())
    print(file_dict['local_file'])
    print(file_dict['remote_directory'])
    sftp.put(file_dict['local_file'], file_dict['remote_directory'])
    ssh.close()


class TransferFragspectTarget(luigi.Task):
    # hidden parameters in luigi.cfg
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # other params
    target = luigi.Parameter()
    timestamp = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        events = PanddaEvent.objects.filter(crystal__target__target_name=self.target)

        # timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H')
        remote_root = self.remote_root

        host_dict = {'hostname': self.hostname, 'username': self.username}

        for e in events:

            if e.pandda_event_map_native and e.refinement.bound_conf:
                name = '_'.join([e.crystal.crystal_name, str(e.site.site), str(e.event)])
                remote_map = name + '_pandda.map'
                remote_pdb = name + '_bound.pdb'

                transfer_file(host_dict=host_dict, file_dict={
                    'remote_directory': os.path.join(remote_root, self.timestamp, e.crystal.target.target_name.upper(),
                                                     name,remote_map),
                    'remote_root': remote_root,
                    'local_file': e.pandda_event_map_native
                })

                transfer_file(host_dict=host_dict, file_dict={
                    'remote_directory': os.path.join(remote_root, self.timestamp, e.crystal.target.target_name.upper(),
                                                     name, remote_pdb),
                    'remote_root': remote_root,
                    'local_file': e.refinement.bound_conf
                })


class TransferFragspectVisitProposal(luigi.Task):
    # hidden parameters in luigi.cfg
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # other params
    target = luigi.Parameter()
    timestamp = luigi.Parameter()
    tmp_dir = luigi.Parameter

    def requires(self):
        return TransferFragspectTarget(username=self.username, hostname=self.hostname, remote_root=self.remote_root,
                                       target=self.target)

    def output(self):
        pass

    def run(self):
        proposals = [c.visit.proposal.title for c in
                     Crystal.objects.filter(target__target_name=self.target).distinct('visit__proposal__title')]

        visits = [c.visit.visit[2:] for c in
                  Crystal.objects.filter(target__target_name=self.target).distinct('visit__visit')]

        proposal_file = os.path.join(self.tmp_dir, 'PROPOSALS')

        visit_file = os.path.join(self.tmp_dir, 'VISITS')

        with open(proposal_file, 'wb') as f:
            f.write(' '.join(proposals))

        with open(visit_file, 'wb') as f:
            f.write(' '.join(visits))

        remote_root = self.remote_root

        host_dict = {'hostname': self.hostname, 'username': self.username}

        transfer_file(host_dict=host_dict, file_dict={
            'remote_directory': os.path.join(remote_root, self.timestamp, self.target.upper(), 'PROPOSALS'),
            'remote_root': remote_root,
            'local_file': proposal_file
        })

        transfer_file(host_dict=host_dict, file_dict={
            'remote_directory': os.path.join(remote_root, self.timestamp, self.target.upper(), 'VISITS'),
            'remote_root': remote_root,
            'local_file': visit_file
        })

        os.remove(proposal_file)
        os.remove(visit_file)

        with open(self.output().path, 'wb') as f:
            f.write('')


class StartFragspectLoader(luigi.Task):
    # hidden parameters in luigi.cfg - file transfer
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # luigi.cfg - curl request to start loader
    user = VerneConfig().update_user
    token = VerneConfig().update_token
    rand_string = VerneConfig().rand_string

    # other params
    target = luigi.Parameter()
    timestamp = luigi.Parameter()
    tmp_dir = luigi.Parameter()

    # TODO: Add this to luigi.cfg
    target_list = VerneConfig().fragspect_list

    def requires(self):
        targets = open(self.target_list, 'rb').readlines()

    def output(self):
        pass

    def run(self):
        pass


