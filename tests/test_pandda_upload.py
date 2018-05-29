import unittest
import os
import shutil
import datetime
import setup_django
from test_functions import run_luigi_worker
import functions.pandda_functions as pf
from luigi_classes import db_ops_django
from db.models import *
import pandas as pd


# class PanddaRun(models.Model):
#     input_dir = models.TextField(blank=True, null=True)
#     pandda_dir = models.TextField(blank=True, null=True)
#     pandda_log = models.TextField(blank=True, null=True)
#     pandda_version = models.TextField(blank=True, null=True)
#     sites_file = models.TextField(blank=True, null=True)
#     events_file = models.TextField(blank=True, null=True)
#
#     class Meta:
#         db_table = 'pandda_run'
#         # unique_together = ('crystal', 'pandda_log')
#
#
# class PanddaSite(models.Model):
#     crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
#     run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
#     site = models.IntegerField(blank=True, null=True)
#     site_aligned_centroid = models.TextField(blank=True, null=True)
#     site_native_centroid = models.TextField(blank=True, null=True)
#     pandda_model_pdb = models.TextField(blank=True, null=True)
#     pandda_input_mtz = models.TextField(blank=True, null=True)
#     pandda_input_pdb = models.TextField(blank=True, null=True)
#
#     class Meta:
#         db_table = 'pandda_site'
#         unique_together = ('crystal', 'run', 'site')
#
#
# class PanddaEvent(models.Model):
#     crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
#     site = models.ForeignKey(PanddaSite, on_delete=models.CASCADE)
#     run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
#     event = models.IntegerField(blank=True, null=True)
#     event_centroid = models.TextField(blank=True, null=True)
#     event_dist_from_site_centroid = models.TextField(blank=True, null=True)
#     lig_centroid = models.TextField(blank=True, null=True)
#     lig_dist_event = models.FloatField(blank=True, null=True)
#     lig_id = models.TextField(blank=True, null=True)
#     pandda_event_map_native = models.TextField(blank=True, null=True)
#
#     class Meta:
#         db_table = 'pandda_event'
#         unique_together = ('site', 'event', 'crystal', 'run')

class TestFindLogs(unittest.TestCase):
    # filepath where test data is
    filepath = 'tests/docking_files/database/'
    db_file_name = 'soakDBDataFile.sqlite'
    # tmp directory to test in
    tmp_dir = 'tmp/'
    date = datetime.date.today()

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()
        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)
        cls.db_full_path = os.path.join(cls.working_dir, cls.db_file_name)
        print('Working dir: ' + cls.working_dir)
        shutil.copytree(os.path.join(cls.top_dir, cls.filepath), cls.working_dir)
        os.chdir(cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)
        # delete rows created in soakdb table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()
        # # delete rows created in proposals table
        proposal_rows = Proposals.objects.all()
        proposal_rows.delete()
        # # delete rows created in crystals table
        crystal_rows = Crystal.objects.all()
        crystal_rows.delete()
        # delete pandda runs
        pandda_runs = PanddaRun.objects.all()
        pandda_runs.delete()

    def test_find_pandda_logs(self):
        print('\n')
        print('TEST: test_find_pandda_logs')
        print('\n')
        find_logs = run_luigi_worker(db_ops_django.FindPanddaLogs(
            search_path=
            '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/panddas_alice',
        soak_db_filepath=str(self.working_dir + '*')))

        self.assertTrue(find_logs)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir,
                                                    self.date.strftime('logs/pandda/pandda_logs_%Y%m%d.txt'))))

    def test_add_pandda_runs(self):
        print('\n')
        print('TEST: test_add_pandda_runs')
        print('\n')
        print('finding files from logs...')
        log_files = pf.find_log_files(
            '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/')
        log_files = log_files.split()
        print('log_files: ' + str(log_files))

        for log_file in log_files:

            pver, input_dir, output_dir, sites_file, events_file, err = pf.get_files_from_log(log_file)

            print(log_file)

            if err!='False' or err!=False:

                print('Adding pandda run from log: ' + str(log_file))
                print('pver: ' + str(pver))
                print('input_dir: ' + str(input_dir))
                print('output_dir: ' + str(output_dir))
                print('sites_file: ' + str(sites_file))
                print('events_file: ' + str(events_file))
                print('err: ' + str(err))

                add_run = run_luigi_worker(db_ops_django.AddPanddaRun(
                    log_file=log_file, pver=pver, input_dir=input_dir, output_dir=output_dir, sites_file=sites_file,
                    events_file=events_file))

                self.assertTrue(add_run)

                sites_frame = pd.DataFrame.from_csv(sites_file)

                print(sites_frame)

                for i in range(0,len(sites_frame['site_idx'])):
                    print(sites_frame['site_idx'][i])
                    print(sites_frame['centroid'][i])
                    print(sites_frame['native_centroid'][i])




