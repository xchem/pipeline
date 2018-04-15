import luigi

def run_luigi_worker(task):
    # initialise a worker to run task
    w = luigi.worker.Worker()
    # schedule task
    w.add(task)
    # run task - returns True if job is run successfully by worker
    status = w.run()

    return status