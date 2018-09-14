import luigi
from paramiko import SSHClient
from scp import SCPClient


class TransferDirectory(luigi.Task):
    username = luigi.Parameter()
    hostname = luigi.Parameter()
    remote_directory = luigi.Parameter()
    local_directory = luigi.Parameter()

    def run(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(self.hostname, username=self.username)
        scp = SCPClient(ssh.get_transport())

        scp.put(self.local_directory, recursive=True, remote_path=self.remote_directory)
        scp.close()

