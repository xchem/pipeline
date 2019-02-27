import setup_django

setup_django.setup_django()

from paramiko import SSHClient
from scp import SCPClient
import os
import time
import datetime

from xchem_db.models import PanddaEvent

# # hidden parameters in luigi.cfg
# username = VerneConfig().username
# hostname = VerneConfig().hostname
# remote_root = VerneConfig().remote_root
#
# # normal parameters
# remote_directory = luigi.Parameter()
# local_directory = luigi.Parameter()
# timestamp = luigi.Parameter()
# target_file = luigi.Parameter()
# target_name = luigi.Parameter()


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
        for f in file_dict['remote_directory'].replace(file_dict['remote_root'], '').split('/'):
            f_path += str('/' + f)
            print(f_path)
            try:
                sftp.stat(str(file_dict['remote_root']+ f_path))
            except FileNotFoundError:
                sftp.mkdir(str(file_dict['remote_root'] + f_path))

    # set up scp protocol and recursively push the directories across
    scp = SCPClient(ssh.get_transport())
    scp.put(file_dict['local_directory'], recursive=True, remote_path=file_dict['remote_directory'])
    scp.close()

events = PanddaEvent.objects.filter(crystal__target__target_name='NUDT7A_Crude')
timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H')
remote_root = '/data/fs-input/django_data'
host_dict = {'hostname': '192.168.160.15', 'username': 'centos'}
file_dict = {'remote_directory': os.path.join(remote_root, timestamp), 'remote_root': remote_root}

