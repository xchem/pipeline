import luigi
import subprocess
from functions import cluster_functions
import setup_django


class SubmitJob(luigi.Task):
    remote_sub_command = luigi.Parameter(default='ssh -t uzw12877@nx.diamond.ac.uk')
    job_directory = luigi.Parameter()
    job_script = luigi.Parameter()
    max_jobs = luigi.Parameter(default='100')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(str(self.job_script + '.running'))

    def run(self):

        ok_to_submit = cluster_functions.check_cluster(self.remote_sub_command, self.max_jobs)
        if not ok_to_submit:
            raise Exception('Too many jobs running on the cluster. Will try again later!')

        submission_string = ' '.join([
            self.remote_sub_command,
            '"',
            'cd',
            self.job_directory,
            '; module load global/cluster >>/dev/null 2>&1; qsub -q medium.q',
            self.job_script,
            '"'
        ])

        print(submission_string)

        submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = submission.communicate()

        out = out.decode('ascii')
        print('\n')
        print(out)
        print('\n')
        if err:
            err = err.decode('ascii')
            print('\n')
            print(err)
            print('\n')

        with self.output().open('w') as f:
            f.write('')
