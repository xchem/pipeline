import luigi
import os
from . import cluster_submission
import setup_django
from db.models import ProasisHits


class WriteHotJob(luigi.Task):
    # defaults need defining in settings
    site_id = luigi.Parameter()
    confirmation_code = luigi.Parameter()
    email = luigi.Parameter()

    # from proasis out
    apo_pdb = luigi.Parameter()
    directory = luigi.Parameter()
    anaconda_path = luigi.Parameter()
    ccdc_settings = luigi.Parameter()
    conda_environment = luigi.Parameter()
    hotspot_script = luigi.Parameter()
    ccdc_location_batch = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.apo_pdb.replace('.pdb', '_hotspots.sh')))

    def run(self):

        job_string = '''
        #!/bin/bash
        export PATH=%s
        source %s
        %s -current_machine -site_id %s -conf_code %s -email %s -auto_accept_licence
        conda activate %s
        cd %s
        python %s %s
        ''' % (self.anaconda_path,
               self.ccdc_settings,
               self.ccdc_location_batch,
               self.site_id,
               self.confirmation_code,
               self.email,
               self.conda_environment,
               self.directory,
               self.hotspot_script,
               os.path.join(self.directory, self.apo_pdb))

        with self.output().open('w') as f:
            f.write(job_string)


class WriteRunCheckHot(luigi.Task):

    def requires(self):
        hits = ProasisHits.objects.exclude(strucid=None)


