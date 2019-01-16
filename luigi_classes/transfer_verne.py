import os
import time

import datetime
import luigi
from paramiko import SSHClient
from scp import SCPClient

from rdkit import Chem
from rdkit.Chem import AllChem

import setup_django
from functions.misc_functions import get_mod_date

setup_django.setup_django()

from .config_classes import VerneConfig
from xchem_db.models import *
from luigi_classes.pull_proasis import GetOutFiles, CreateProposalVisitFiles


class GenerateLigandResults(luigi.Task):
    target = luigi.Parameter()
    directory = luigi.Parameter()
    sdf_file = luigi.Parameter(default='all_ligs.sdf')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.sdf_file))

    def run(self):
        # Get all project crystals from target name
        crystals = Crystal.objects.filter(target__target_name=self.target)

        # create empty object to hold molecules
        mols = []

        # for each crystal
        for c in crystals:
            # get relevant objects and set smiles string for ligand
            lab = Lab.objects.get(crystal_name=c)
            refinement = Refinement.objects.get(crystal_name=c)
            smiles = c.compound.smiles

            # create rdkit molecule object from smiles
            m = Chem.MolFromSmiles(smiles)

            # set molecule properties to be written to combined sdf
            m.SetProp('_Name', str(c.crystal_name))
            m.SetProp('Smiles', str(smiles))
            m.SetProp('SoakStatus', str(lab.soak_status))
            m.SetProp('MountStatus', str(lab.mounting_result))
            m.SetProp('RefinementStatus', str(refinement.status))
            m.SetProp('HarvestStatus', str(lab.harvest_status))
            m.SetProp('LibraryName', str(lab.library_name))
            m.SetProp('LibraryPlate', str(lab.library_plate))
            m.SetProp('SoakVolume', str(lab.soak_vol))
            m.SetProp('SoakTime', str(lab.soak_time))
            m.SetProp('SolventFraction', str(lab.solv_frac))
            m.SetProp('StockConcentration', str(lab.stock_conc))
            m.SetProp('RefinementOutcomeNum', str(refinement.outcome))

            # compute arbitrary 2D coordinates for each ligand
            AllChem.Compute2DCoords(m)

            # append current molecule to mol holder
            mols.append(m)

        # set sdf writer, and write all molecules out
        w = Chem.SDWriter(self.output().path)
        for m in mols:
            w.write(m)


class TransferDirectory(luigi.Task):
    # hidden parameters in luigi.cfg
    username = VerneConfig().username
    hostname = VerneConfig().hostname
    remote_root = VerneConfig().remote_root

    # normal parameters
    remote_directory = luigi.Parameter()
    local_directory = luigi.Parameter()
    timestamp = luigi.Parameter()
    target_file = luigi.Parameter()
    target_name = luigi.Parameter()

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
    target_file = luigi.Parameter()
    target_name = luigi.Parameter()
    open_target_list = VerneConfig().open_target_list

    def requires(self):
        print(self.local_directory)
        print(self.remote_directory)
        return TransferDirectory(local_directory=self.local_directory, remote_directory=self.remote_directory,
                          timestamp=self.timestamp, target_file=self.target_file, target_name=self.target_name)

    def output(self):
        self.out_dir = '/'.join(
            TransferDirectory(
                local_directory=self.local_directory, remote_directory=self.remote_directory,
                timestamp=self.timestamp, target_file=self.target_file,
                target_name=self.target_name).output().path.split('/')[:-1])

        return luigi.LocalTarget(os.path.join(self.out_dir, str('visits_proposals.done')))

    def run(self):
        # create SSH client with paramiko and connect with system host keys
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)

        # get paths for visit and proposal files
        visit_proposal_file = [os.path.join(self.out_dir, 'VISITS'), os.path.join(self.out_dir, 'PROPOSALS')]

        # construct a list of open targets from input text file
        open_targets = [x.rstrip().upper() for x in open(self.open_target_list, 'r').readlines()]

        # for each of the visit/proposal files
        for f in visit_proposal_file:
            remote_location = os.path.join(self.remote_directory, self.target_name.upper())
            if self.target_name.upper() in open_targets:
                # open the file and write 'OPEN' to it
                with open(f, 'w') as a:
                    a.write('OPEN')
                    # wait a second for the I/O to complete
                    time.sleep(1)
                sftp = ssh.open_sftp()
                print(str('REMOTE: ' + os.path.join(remote_location, f.split('/')[-1])))
                sftp.remove(os.path.join(remote_location, f.split('/')[-1]))
                sftp.close()
            # if the file exists (it should)
            if os.path.isfile(f):
                scp = SCPClient(ssh.get_transport())
                # put the file over to verne
                scp.put(f, remote_path=remote_location)
                # close the scp connection
                scp.close()
            else:
                raise Exception('No visit/proposal file!')

        with self.output().open('w') as o:
            o.write('')


class TransferByTargetList(luigi.Task):
    remote_root = VerneConfig().remote_root
    timestamp = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H'))
    target_list = VerneConfig().target_list
    target_file = 'TARGET_LIST'

    def output(self):
        print(self.timestamp)
        return luigi.LocalTarget(str('logs/verne_transfer_' + self.timestamp))

    def requires(self):
        if os.path.isfile(self.target_file):
            os.remove(self.target_file)
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
                            transfer_paths.append((pth, tgt))

        transfer_paths = list(set(transfer_paths))

        return [TransferVisitAndProposalFiles(remote_directory=os.path.join(self.remote_root, self.timestamp),
                                              local_directory=p[0],
                                              timestamp=datetime.datetime.now().strftime('%Y-%m-%dT%H'),
                                              target_file=self.target_file,
                                              target_name=p[1])
                for p in transfer_paths]

    def run(self):
        outfiles = [p.path for p in self.input()]

        for o in outfiles:
            print(int(datetime.datetime.strptime(self.timestamp, '%Y-%m-%dT%H').strftime('%Y%m%d%H%M')))
            print(int(datetime.datetime.strptime(get_mod_date(o), '%Y%m%d%H%M%S').strftime('%Y%m%d%H%M')))

            if int(datetime.datetime.strptime(self.timestamp, '%Y-%m-%dT%H').strftime('%Y%m%d%H%M')) - \
                    int(datetime.datetime.strptime(get_mod_date(o), '%Y%m%d%H%M%S').strftime('%Y%m%d%H%M')) <= 130:
                with self.output().open('w') as f:
                    f.write('')


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
        return luigi.LocalTarget(str('logs/verne_update_' + str(self.timestamp)))

    def run(self):

        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)

        if os.path.isfile(os.path.join(os.getcwd(), 'TARGET_LIST')):
            os.remove(os.path.join(os.getcwd(), 'TARGET_LIST'))

        os.system('touch ' + os.path.join(os.getcwd(), 'TARGET_LIST'))

        verne_dirs = []
        v_sftp = ssh.open_sftp()
        v_sftp.chdir(os.path.join(self.remote_root, self.timestamp))
        for i in v_sftp.listdir():
            lstatout = str(v_sftp.lstat(i)).split()[0]
            if 'd' in lstatout:
                verne_dirs.append(str(i))
        v_sftp.close()

        verne_dirs.sort()

        write_string = ' '.join(verne_dirs)

        with open(os.path.join(os.getcwd(), 'TARGET_LIST'), 'w') as f:
            f.write(write_string)

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

