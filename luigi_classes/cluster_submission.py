import luigi
import subprocess
import os
from functions import cluster_functions
import setup_django


# task to submit any job to the cluster
class SubmitJob(luigi.Task):
    remote_sub_command = luigi.Parameter(default='ssh -t uzw12877@nx.diamond.ac.uk')
    job_directory = luigi.Parameter()
    job_script = luigi.Parameter()
    max_jobs = luigi.Parameter(default='100')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, str(self.job_script + '.submitted')))

    def run(self):

        ok_to_submit = cluster_functions.check_cluster(self.remote_sub_command, self.max_jobs)
        if not ok_to_submit:
            raise Exception('Too many jobs running on the cluster. Will try again later!')

        out, err = cluster_functions.submit_job(job_directory=self.job_directory, job_script=self.job_script,
                                                remote_sub_command=self.remote_sub_command)

        if err:
            raise Exception(err)

        print(out)

        with self.output().open('wb') as f:
            f.write('')

# standard job
class WriteJob(luigi.Task):
    job_directory = luigi.Parameter()
    job_filename = luigi.Parameter()
    job_name = luigi.Parameter()
    job_executable = luigi.Parameter()
    job_options = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, self.job_filename))

    def run(self):
        os.chdir(self.job_directory)
        job_script = '''#!/bin/bash
cd %s
touch %s.running
%s %s > %s.log
rm %s.running
touch %s.done
''' % (self.job_directory, self.job_name, self.job_executable, self.job_options,
       self.job_name, self.job_name, self.job_name)

        with self.output().open('wb') as f:
            f.write(job_script)


class CheckJobOutput(luigi.Task):
    job_directory = luigi.Parameter()
    job_output_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, str(self.job_output_file + '.done')))

    def run(self):
        if os.path.isfile(os.path.join(self.job_directory, self.job_output_file)):
            with self.output().open('wb') as f:
                f.write('')
        else:
            raise Exception('Job output not found!')


# Task for writing jobs that include loading a conda environment and running a python script
class WriteCondaEnvJob(luigi.Task):
    job_directory = luigi.Parameter()
    job_filename = luigi.Parameter()
    anaconda_path = luigi.Parameter()
    additional_commands = luigi.Parameter()
    python_script = luigi.Parameter()
    parameters = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, self.job_filename))

    def run(self):
        os.chdir(self.job_directory)
        # order of job check becomes: check for job_script.done, check for job on cluster then re-run if neither present
        job_string = '''
                #!/bin/bash
                export PATH=%s
                %s
                conda activate %s
                cd %s
                touch %s.running
                python %s %s
                rm %s.running
                touch %s.done
                ''' % (self.anaconda_path,
                       self.additional_commands,
                       self.conda_environment,
                       self.job_directory,
                       str(self.output().path),
                       self.python_script,
                       self.parameters,
                       str(self.output().path),
                       str(self.output().path))

        with self.output().open('w') as f:
            f.write(job_string)




