import subprocess


def submit_job(job_directory, job_script, remote_sub_command):
    submission_string = ' '.join([
        remote_sub_command,
        '"',
        'cd',
        job_directory,
        '; module load global/cluster >>/dev/null 2>&1; qsub -q medium.q ',
        job_script,
        '"'
    ])

    print(submission_string)

    submission = subprocess.Popen(submission_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = submission.communicate()

    out = out.decode('ascii')

    if err:
        err = err.decode('ascii')

    return out, err


def check_cluster(remote_sub_command, max_jobs):
    submit = True
    command = ' '.join([
        remote_sub_command,
        '"',
        'module load global/cluster >>/dev/null 2>&1; qstat -u uzw12877 | wc -l',
        '"'
    ])
    print(command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = int(out.decode('ascii').split()[-1])
    if err:
        err = err.decode('ascii')
        print(err)
    print(out)
    if int(out) > 0:
        number = int(out) - 2
    if int(out) == 0:
        number = int(out)

    if number > int(max_jobs):
        submit = False

    return submit
