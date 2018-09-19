import os
from setup_django import setup_django

setup_django()

import luigi

from xchem_db.models import *
from . import cluster_submission


class WriteHot(luigi.Task):
    apo_pdb = luigi.Parameter()
    directory = luigi.Parameter()

    def run(self):
        yield [cluster_submission.WriteHotJob(apo_pdb=a, directory=d) for (a, d) in zip(self.apo_pdb, self.directory)]


class SubmitHot(luigi.Task):
    output_paths=luigi.Parameter()
    apo_pdb = luigi.Parameter()
    directory = luigi.Parameter()

    def requires(self):
        return WriteHot(apo_pdb=self.apo_pdb, directory=self.directory)

    def run(self):
        yield [cluster_submission.SubmitJob(job_directory='/'.join(j.split('/')[:-1]),
                                            job_script=j.split('/')[-1]) for j in self.output_paths]


class WriteRunCheckHot(luigi.Task):

    def requires(self):
        out = ProasisOut.objects.exclude(apo=None)
        apo_pdb = []
        directory = []
        for o in out:

            apo_pdb.append(o.apo)
            directory.append(os.path.join(o.root, o.start))

        w_output_paths = [cluster_submission.WriteHotJob(apo_pdb=a, directory=d).output().path for (a, d) in zip(apo_pdb, directory)]

        yield [cluster_submission.WriteHotJob(apo_pdb=a, directory=d) for (a, d) in zip(apo_pdb, directory)]
        yield [cluster_submission.SubmitJob(job_directory='/'.join(j.split('/')[:-1]),
                                            job_script=j.split('/')[-1]) for j in w_output_paths]
        yield [cluster_submission.CheckJob(
            output_files=[j.replace('_apo_hotspots.sh', '_acceptor.ccp4.gz'),
                          j.replace('_apo_hotspots.sh', '_donor.ccp4.gz'),
                          j.replace('_apo_hotspots.sh', '_apolar.ccp4.gz')],
            job_file=j.split('/')[-1],
            directory='/'.join(j.split('/')[:-1])) for j in w_output_paths]
        yield [cluster_submission.RemoveJobFiles(
            output_files=[j.replace('_apo_hotspots.sh', '_acceptor.ccp4.gz'),
                          j.replace('_apo_hotspots.sh', '_donor.ccp4.gz'),
                          j.replace('_apo_hotspots.sh', '_apolar.ccp4.gz')],
            job_file=j.split('/')[-1], done_name='hotspots',
            directory='/'.join(j.split('/')[:-1])) for j in w_output_paths]
