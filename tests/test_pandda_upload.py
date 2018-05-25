import unittest
import os
import shutil
import datetime
import setup_django
from test_functions import run_luigi_worker
from luigi_classes import db_ops_django
from db.models import *


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

    def test_find_pandda_logs(self):
        find_logs = run_luigi_worker(db_ops_django.FindPanddaLogs(
            search_path=
            '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/panddas_alice',
        soak_db_filepath=str(self.working_dir + '*')))

        self.assertTrue(find_logs)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir,
                                                    self.date.strftime('logs/pandda/pandda_logs_%Y%m%d.txt'))))
