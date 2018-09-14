import os
import shutil
import unittest

import datetime

import functions.pandda_functions as pf
from luigi_classes.transfer_pandda import FindPanddaLogs, AddPanddaRun, AddPanddaSites
from test_functions import run_luigi_worker


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
        # soakdb_rows = SoakdbFiles.objects.all()
        # soakdb_rows.delete()
        # # # delete rows created in proposals table
        # proposal_rows = Proposals.objects.all()
        # proposal_rows.delete()
        # # # delete rows created in crystals table
        # crystal_rows = Crystal.objects.all()
        # crystal_rows.delete()
        # # delete pandda runs
        # pandda_runs = PanddaRun.objects.all()
        # pandda_runs.delete()

    def test_find_pandda_logs(self):
        find_logs = run_luigi_worker(FindPanddaLogs(
            search_path=
            '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/panddas_alice',
            soak_db_filepath=str(self.working_dir + '*')))

        self.assertTrue(find_logs)

    def test_add_pandda_runs(self):

        remove_files = []

        log_files = pf.find_log_files(
            '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/')
        log_files = log_files.split()

        for log_file in log_files:

            pver, input_dir, output_dir, sites_file, events_file, err = pf.get_files_from_log(log_file)

            if not err and sites_file and events_file and '0.1.' not in pver:
                remove_path = str('/'.join(log_file.split('/')[:-1]))
                remove_files.append(remove_path)

                add_run = run_luigi_worker(AddPanddaRun(
                    log_file=log_file, pver=pver, input_dir=input_dir, output_dir=output_dir, sites_file=sites_file,
                    events_file=events_file))

                self.assertTrue(add_run)

                add_sites = run_luigi_worker(AddPanddaSites(log_file=log_file, pver=pver,
                                                            input_dir=input_dir, output_dir=output_dir,
                                                            sites_file=sites_file,
                                                            events_file=events_file,
                                                            soakdb_filename=os.path.join(
                                                                self.working_dir, 'soakDBDataFile.sqlite'))
                                             )

                self.assertTrue(add_sites)

                if os.path.isfile(str(log_file + '.sites.done')):
                    os.remove(str(log_file + '.sites.done'))

    def test_all_for_nudt7_en(self):
        pass
