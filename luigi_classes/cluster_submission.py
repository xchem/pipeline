import luigi
import subprocess
import os
import time
import glob
import smtplib
from email.mime.text import MIMEText
from functions import cluster_functions
import setup_django


# task to submit any job to the cluster
class SubmitJob(luigi.Task):
    remote_sub_command = luigi.Parameter(default='ssh -tt uzw12877@nx.diamond.ac.uk')
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

        # if err:
        #     raise Exception(err)

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


# class CheckJobOutput(luigi.Task):
#     job_directory = luigi.Parameter()
#     job_output_file = luigi.Parameter()
#
#     def output(self):
#         return luigi.LocalTarget(os.path.join(self.job_directory, str(self.job_output_file + '.done')))
#
#     def run(self):
#         if os.path.isfile(os.path.join(self.job_directory, self.job_output_file)):
#             with self.output().open('wb') as f:
#                 f.write('')
#         else:
#             raise Exception('Job output not found!')


# Task for writing jobs that include loading a conda environment and running a python script
class WriteCondaEnvJob(luigi.Task):
    job_directory = luigi.Parameter()
    job_filename = luigi.Parameter()
    anaconda_path = luigi.Parameter()
    additional_commands = luigi.Parameter()
    additional_commands_2 = luigi.Parameter()
    python_script = luigi.Parameter()
    parameters = luigi.Parameter()
    conda_environment = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, self.job_filename))

    def run(self):
        os.chdir(self.job_directory)
        # order of job check becomes: check for job_script.done, check for job on cluster then re-run if neither present
        job_string = '''#!/bin/bash
export PATH=%s
cd %s
%s
conda activate %s
touch %s.running
python %s %s
rm %s.running
touch %s.done
%s''' % (self.anaconda_path,
                    self.job_directory,
                       self.additional_commands,
                       self.conda_environment,
                       str(self.output().path),
                       self.python_script,
                       self.parameters,
                       str(self.output().path),
                       str(self.output().path),
        self.additional_commands_2)

        with self.output().open('w') as f:
            f.write(job_string)


class CheckJob(luigi.Task):
    remote_sub_command = luigi.Parameter(default='ssh -tt uzw12877@nx.diamond.ac.uk')
    max_jobs = luigi.Parameter(default='100')
    output_files = luigi.Parameter()
    job_file = luigi.Parameter()
    directory = luigi.Parameter()
    # data_directory = luigi.Parameter(default='')
    # extension = luigi.Parameter(default='')
    # a list of people to email when a job has finished
    emails = luigi.Parameter(default=['rachael.skyner@diamond.ac.uk',
                                      'richard.gillams@diamond.ac.uk'])

    def requires(self):
        pass

    def output(self):
        # a text version of the email sent is saved
        return luigi.LocalTarget(os.path.join(self.directory, str(self.job_file + '.message')))

    def run(self):

        def check_files(output_files, directory):
            files_exist = []
            for f in output_files:
                if os.path.isfile(os.path.join(directory, f)):
                    files_exist.append(1)
                else:
                    files_exist.append(0)
            return files_exist

        if 0 in check_files(self.output_files, self.directory):
            time.sleep(5)
            if 0 in check_files(self.output_files, self.directory):
                queue_jobs = []
                job = os.path.join(self.directory, self.job_file)
                output = glob.glob(str(job + '.o*'))
                print(output)

                submission_string = ' '.join([
                    self.remote_sub_command,
                    '"',
                    'qstat -r',
                    '"'
                ])

                submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
                out, err = submission.communicate()

                output_queue = (out.decode('ascii').split('\n'))
                print(output_queue)
                for line in output_queue:
                    if 'Full jobname' in line:
                        jobname = line.split()[-1]
                        queue_jobs.append(jobname)
                print(queue_jobs)

                if self.job_file not in queue_jobs:
                    ok_to_submit = cluster_functions.check_cluster(self.remote_sub_command, self.max_jobs)
                    if not ok_to_submit:
                        raise Exception('Too many jobs running on the cluster. Will try again later!')

                    out, err = cluster_functions.submit_job(job_directory=self.directory,
                                                            job_script=self.job_file,
                                                            remote_sub_command=self.remote_sub_command)

                    if err:
                        if 'Connection to' not in err:
                            raise Exception(err)

                    print(out)

                    print(
                        'The job had no output, and was not found to be running in the queue. The job has been '
                        'resubmitted. Will check again later!')

                if not queue_jobs:
                    raise Exception('output files not found for ' + str(
                        self.job_file) + '... something went wrong or job is still running')

        if 0 not in check_files(self.output_files, self.directory):
            # message text for the email
            message_text = r'''This is an automated message from the FragBack Pipeline.
A cluster job submitted by the pipeline has completed (the expected output files are present). 
You might want to check the output files listed below to see if the job has ACTUALLY completed successfully:

job directory: %s
job name: %s
job outputs: %s

Thanks, 
FragBack xoxo
''' % (self.directory, self.job_file, ', '.join([o for o in self.output_files]))

            # write the message to a txt file
            with open(self.output().path, 'w') as m:
                m.write(message_text)
            # open the message as read
            fp = open(self.output().path, 'r')
            # read with email package
            msg = MIMEText(fp.read())
            fp.close()
            # set email subject, to, from
            msg['Subject'] = str('FragBack: %s') % self.job_file
            msg['From'] = 'fragback-pipe'
            msg['To'] = ','.join(self.emails)
            # use localhost as email server
            s = smtplib.SMTP('localhost')
            # send the email to everyone
            s.sendmail(msg['From'], self.emails, msg.as_string())
            s.quit()
