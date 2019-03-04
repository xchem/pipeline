import setup_django

setup_django.setup_django()

from paramiko import SSHClient
from scp import SCPClient
import os
import datetime

from xchem_db.models import PanddaEvent

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
                sftp.stat(str(file_dict['remote_root']+ f_path))
            except FileNotFoundError:
                sftp.mkdir(str(file_dict['remote_root'] + f_path))

    # set up scp protocol and recursively push the directories across
    # scp = SCPClient(ssh.get_transport())
    print(file_dict['local_file'])
    print(file_dict['remote_directory'])
    sftp.put(file_dict['local_file'], file_dict['remote_directory'])
    ssh.close()


events = PanddaEvent.objects.filter(crystal__target__target_name='NUDT7A_Crude')

timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H')
remote_root = '/data/fs-input/django_data'

host_dict = {'hostname': '', 'username': ''}

for e in events:

    if e.pandda_event_map_native and e.refinement.bound_conf:
        name = '_'.join([e.crystal.crystal_name, str(e.site.site), str(e.event)])
        remote_map = name + '_pandda.map'
        remote_pdb = name + '_bound.pdb'
	
        transfer_file(host_dict=host_dict, file_dict={
            'remote_directory': os.path.join(remote_root, timestamp,  e.crystal.target.target_name.upper(), name, remote_map),
            'remote_root': remote_root,
            'local_file': e.pandda_event_map_native
        })

        transfer_file(host_dict=host_dict, file_dict={
            'remote_directory': os.path.join(remote_root, timestamp, e.crystal.target.target_name.upper(), name, remote_pdb),
            'remote_root': remote_root,
            'local_file': e.refinement.bound_conf
        })



