from test_functions import run_luigi_worker
from luigi_classes import db_ops_django
import luigi
from test_functions import kill_job
import unittest
import os
import shutil
import datetime


class TestFindSoakDB(unittest.TestCase):
    # filepath where test data is
    filepath = 'tests/docking_files/database/'
    # tmp directory to test in
    tmp_dir = 'tmp/'
    date = luigi.DateParameter(default=datetime.date.today())

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()
        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)
        shutil.copytree(os.path.join(cls.top_dir, cls.filepath), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def test_findsoakdb(self):
        os.chdir(self.working_dir)
        find_file = run_luigi_worker(db_ops_django.FindSoakDBFiles(filepath=str(self.working_dir + '/*')))
        self.assertTrue(find_file)
        self.assertTrue(os.path.isfile(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')))


