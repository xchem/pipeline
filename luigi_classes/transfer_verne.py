import os

import datetime
import luigi
from paramiko import SSHClient
from scp import SCPClient
from functions.misc_functions import get_mod_date

import setup_django

setup_django.setup_django()

from .config_classes import VerneConfig
from xchem_db.models import *
from luigi_classes.pull_proasis import GetOutFiles, CreateProposalVisitFiles


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
        return GetOutFiles(), CreateProposalVisitFiles()

    def output(self):
        print(self.local_directory)
        return luigi.LocalTarget(str(self.local_directory + '/verne.transferred'))

    def run(self):
        print(self.remote_directory)
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
                print(f_path)
                try:
                    sftp.stat(str(self.remote_root + f_path))
                except FileNotFoundError:
                    sftp.mkdir(str(self.remote_root + f_path))
                    #mode=775)

        # set up scp protocol and recursively push the directories across
        scp = SCPClient(ssh.get_transport())
        scp.put(self.local_directory, recursive=True, remote_path=self.remote_directory)
        scp.close()

        remote_target = os.path.join(self.remote_directory, self.local_directory.split('/')[-1])
        print(remote_target)

        local_file = os.path.join(os.getcwd(), 'NEW_DATA')
        if not local_file:
            with open(local_file, 'w') as f:
                f.write('')
        scp = SCPClient(ssh.get_transport())
        scp.put(os.path.join(os.getcwd(), 'NEW_DATA'), recursive=True, remote_path=remote_target)
        scp.close()

        # write local output file to signify transfer done
        with self.output().open('w') as f:
            f.write('')


class TransferVisitAndProposalFiles(luigi.Task):
    # hidden parameters in luigi.cfg
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # normal parameters
    remote_directory = luigi.Parameter()
    local_directory = luigi.Parameter()
    timestamp = luigi.Parameter()

    def requires(self):
        print(self.local_directory)
        print(self.remote_directory)
        return TransferDirectory(local_directory=self.local_directory, remote_directory=self.remote_directory,
                          timestamp=self.timestamp)

    def output(self):
        self.out_dir = '/'.join(
            TransferDirectory(
                              local_directory=self.local_directory, remote_directory=self.remote_directory,
                              timestamp=self.timestamp).output().path.split('/')[:-1])

        return luigi.LocalTarget(os.path.join(self.out_dir, str('visits_proposals.done')))

    def run(self):
        # create SSH client with paramiko and connect with system host keys
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)

        visit_proposal_file = [os.path.join(self.out_dir, 'VISITS'), os.path.join(self.out_dir, 'PROPOSALS')]
        for f in visit_proposal_file:
            if os.path.isfile(f):
                scp = SCPClient(ssh.get_transport())
                print('/'.join(self.remote_directory.split('/')[:-2]))
                scp.put(f, remote_path='/'.join(self.remote_directory.split('/')[:-2]))
                scp.close()
            else:
                break

        with self.output().open('w') as o:
            o.write('')


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
                                                                '/'.join(p.split('/')[-3:])), local_directory=p,
                                  timestamp=datetime.datetime.now().strftime('%Y-%m-%d'))
                for p in transfer_checks], [
                   TransferVisitAndProposalFiles(remote_directory=os.path.join(self.remote_root, self.timestamp,
                                                                               '/'.join(p.split('/')[-3:])),
                                                 local_directory=p,
                                                 timestamp=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
                   for p in transfer_checks]

    def run(self):
        # write local output file to signify all transfers done
        with self.output().open('w') as f:
            f.write('')


class GetTransferVisitProposal(luigi.Task):
    remote_root = VerneConfig().remote_root
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H'))

    def output(self):
        return luigi.LocalTarget(str('logs/transfer_verne_' + self.timestamp + '.visit_proposal.done'))

    def requires(self):
        # get all output directories in the database
        proasis_out = ProasisOut.objects.all()
        # get all paths that have values
        paths = list(set([os.path.join(o.root, o.start) for o in proasis_out if o.root and o.start]))
        transfer_checks = []
        # check that the paths exist
        for p in paths:
            if os.path.isdir(p):
                transfer_checks.append(p.split('/')[:-2])
        # run a file transfer to verne for each directory
        return [TransferVisitAndProposalFiles(remote_directory=os.path.join(self.remote_root, self.timestamp,
                                                                            '/'.join(p.split('/')[-3:])),
                                              local_directory=p,
                                              timestamp=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
                for p in transfer_checks]

    def run(self):
        # write local output file to signify all transfers done
        with self.output().open('w') as f:
            f.write('')


class TransferByTargetList(luigi.Task):
    remote_root = VerneConfig().remote_root
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
    target_list = VerneConfig().target_list

    def output(self):
        print(self.timestamp)
        return luigi.LocalTarget(str('verne_transfer_' + self.timestamp))

    def requires(self):
        transfer_paths = []
        if os.path.isfile(self.target_list):
            target_list = open(self.target_list, 'r')
            for target in target_list:
                tgt = target.rstrip()
                print(tgt)
                proasis_out = ProasisOut.objects.filter(crystal__target__target_name=tgt)
                for o in proasis_out:
                    if o.root and o.start:
                        pth = os.path.join(o.root, '/'.join(o.start.split('/')[:-2]))
                        if os.path.isdir(pth):
                            transfer_paths.append(pth)

        transfer_paths = list(set(transfer_paths))

        return [TransferVisitAndProposalFiles(remote_directory=os.path.join(self.remote_root, self.timestamp),
                                              local_directory=p,
                                              timestamp=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
                for p in transfer_paths]

    def run(self):
        outfiles = [p.path for p in self.input()]

        for f in outfiles:
            print(get_mod_date(f).strftime('%Y-%m-%dT%H'))
            print(self.timestamp)
            if get_mod_date(f).strftime('%Y-%m-%dT%H') == self.timestamp:
                with self.output().open('w') as f:
                    f.write('')
                pass

                # raise Exception('No data to be updated...')




class UpdateVerne(luigi.Task):
    user = VerneConfig().update_user
    token = VerneConfig().update_token
    rand_string = VerneConfig().rand_string
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
    remote_root = VerneConfig().remote_root
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    target_list = VerneConfig().target_list

    def requires(self):
        return TransferByTargetList()

    def output(self):
        return luigi.LocalTarget(str('verne_update_' + str(self.timestamp)))

    def run(self):

        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)
        verne_dirs = []
        v_sftp = ssh.open_sftp()
        v_sftp.chdir(self.remote_root)
        for i in v_sftp.listdir():
            lstatout = str(v_sftp.lstat(i)).split()[0]
            if 'd' in lstatout:
                verne_dirs.append(str(i))
        v_sftp.close()

        verne_dirs.sort()

        previous_dir = verne_dirs[-2]

        remote_target_list = os.path.join(self.remote_root, previous_dir, 'TARGET_LIST')
        scp = SCPClient(ssh.get_transport())
        scp.get(remote_target_list)
        scp.close()

        targets = str(open('TARGET_LIST', 'r').read()).rsplit()
        print(targets)
        local_targets = str(open(self.target_list, 'r').read()).rsplit()
        print(local_targets)
        targets.extend(local_targets)

        targets = list(set(targets))

        os.remove('TARGET_LIST')

        for target in targets:
            with open('TARGET_LIST', 'a') as f:
                f.write(str(str(target) + ' '))

        local_file = os.path.join(os.getcwd(), 'READY')
        if not local_file:
            with open(local_file, 'w') as f:
                f.write('')
        scp = SCPClient(ssh.get_transport())
        scp.put(os.path.join(os.getcwd(), 'READY'), recursive=True,
                remote_path=os.path.join(self.remote_root, self.timestamp))
        scp.put(os.path.join(os.getcwd(), 'TARGET_LIST'), recursive=True,
                remote_path=os.path.join(self.remote_root, self.timestamp))
        scp.close()
        ssh.exec_command(str('chmod -R 775 ' + os.path.join(self.remote_root, self.timestamp)))

        curl_string = str('curl -X POST "https://' + self.user +
                          ':' + self.rand_string +
                          '@jenkins-fragalysis-cicd.apps.xchem.diamond.ac.uk/job/Loader%20Image/build?token='
                          + self.token + '" -k')

        os.system(curl_string)

        with self.output().open('w') as f:
            f.write('')

