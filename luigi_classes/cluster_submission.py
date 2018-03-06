import luigi
import subprocess
import os

class CheckCluster(luigi.Task):
    remote_sub_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('logs/cluster.number')

    def run(self):
        command = ' '.join([
            self.remote_sub_command,
            '"',
            'qstat -u uzw12877 | wc -l',
            '"'
            ])
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        if int(out) > 0:
            number = int(out) - 2
        if int(out)==0:
            number=int(out)
        with self.output().open('wb') as f:
            f.write(str(number))


class SubmitJob(luigi.Task):
    remote_sub_command=luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')
    job_directory = luigi.Parameter()
    job_script=luigi.Parameter()
    max_jobs = luigi.Parameter(default='100')

    def requires(self):
        return CheckCluster()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.job_directory, 'job.id'))

    def run(self):
        with self.input().open('r') as infile:
            number = int(infile.read())

        os.remove(self.input().path)

        if number > int(self.max_jobs):
            raise Exception('Max jobs (' + str(self.max_jobs) + ') exceeded. Please increase max_jobs or wait!')

        os.chdir(self.job_directory)

        submission_string = ' '.join([
            self.remote_sub_command,
            '"',
            'cd',
            self.job_directory,
            '; qsub',
            self.job_script,
            '"'
        ])

        submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = submission.communicate()

        job_number = out.split(' ')[2]

        with self.output().open('wb') as f:
            f.write(job_number)



