import luigi
import os
from . import cluster_submission
import setup_django
from xchem_db.models import *


class WriteHotJob(luigi.Task):
    # defaults need defining in settings
    site_id = luigi.Parameter(default='2035')
    confirmation_code = luigi.Parameter(default='4DFB42')
    email = luigi.Parameter(default='rachael.skyner@dimond.ac.uk')

    # from proasis out
    apo_pdb = luigi.Parameter()
    directory = luigi.Parameter()
    anaconda_path = luigi.Parameter(default='/dls/science/groups/i04-1/software/anaconda/bin:$PATH')
    ccdc_settings = luigi.Parameter(default='/dls/science/groups/i04-1/software/mihaela/DiamondHotspots/ccdc_settings.sh')
    conda_environment = luigi.Parameter(default='hotspots')
    hotspot_script = luigi.Parameter(default='/dls/science/groups/i04-1/software/fragalysis/hotspots/hotspots.py')
    ccdc_location_batch = luigi.Parameter(default='/dls_sw/apps/ccdc/CSD_2017/bin/batch_register')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.apo_pdb.replace('.pdb', '_hotspots.sh')))

    def requires(self):
        additional_line = '''%s -current_machine -licence_dir $PWD -site_id %s -conf_code %s -email %s -auto_accept_licence
source %s
export CCDC_CSD_LICENCE_FILE=$PWD/csd_licence.dat
sleep 5s''' \
                          % (
                              self.ccdc_location_batch,
                              self.site_id,
                              self.confirmation_code,
                              self.email,
                              self.ccdc_settings
                          )
        additional_line_2 = "find . -name '*.ccp4' -print0 | " \
                            "while IFS= read -r -d $'\\0' file; do tar -czvf $file.tar.gz $file --remove-files; done"

        return cluster_submission.WriteCondaEnvJob(job_directory=self.directory,
                                                   job_filename=os.path.join(
                                                       self.directory, self.apo_pdb.replace('.pdb', '_hotspots.sh')),
                                                   anaconda_path=self.anaconda_path,
                                                   additional_commands=additional_line,
                                                   python_script=self.hotspot_script,
                                                   parameters=os.path.join(self.directory, self.apo_pdb),
                                                   conda_environment=self.conda_environment,
                                                   additional_commands_2=additional_line_2)


class WriteRunCheckHot(luigi.WrapperTask):

    def requires(self):
        out = ProasisOut.objects.exclude(apo=None)
        apo_pdb = []
        directory = []
        for o in out:

            apo_pdb.append(o.apo)
            directory.append(os.path.join(o.root, o.start))

        w_output_paths = [WriteHotJob(apo_pdb=a, directory=d).output().path for (a, d) in zip(apo_pdb, directory)]

        yield [WriteHotJob(apo_pdb=a, directory=d) for (a, d) in zip(apo_pdb, directory)]
        yield [cluster_submission.SubmitJob(job_directory='/'.join(j.split('/')[:-1]),
                                            job_script=j.split('/')[-1]) for j in w_output_paths]
        yield [cluster_submission.CheckJob(
            output_files=[j.replace('_apo_hotspots.sh', '_acceptor.ccp4.tar.gz'),
                          j.replace('_apo_hotspots.sh', '_donor.ccp4.tar.gz'),
                          j.replace('_apo_hotspots.sh', '_apolar.ccp4.tar.gz')],
            job_file=j.split('/')[-1],
            directory='/'.join(j.split('/')[:-1])) for j in w_output_paths]
