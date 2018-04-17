import datetime
import os
import shutil
import unittest
import setup_django
from db.models import *
from luigi_classes import db_ops_django
from test_functions import run_luigi_worker


class TestDataTransfer(unittest.TestCase):
    # filepath where test data is
    filepath = 'tests/docking_files/database/'
    # tmp directory to test in
    tmp_dir = 'tmp/'
    date = datetime.date.today()

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()
        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)
        print('Working dir: ' + cls.working_dir)
        shutil.copytree(os.path.join(cls.top_dir, cls.filepath), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)
        # delete rows created in soakdb table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()
        # delete rows created in proposals table
        proposal_rows = Proposals.objects.all()
        proposal_rows.delete()

    def test_findsoakdb(self):
        os.chdir(self.working_dir)
        print(os.path.join(self.working_dir, self.filepath)+ '/*')
        find_file = run_luigi_worker(db_ops_django.FindSoakDBFiles(
            filepath=str(os.path.join(self.working_dir, self.filepath)+ '/*')))

        self.assertTrue(find_file)
        self.assertTrue(os.path.isfile(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')))

    def test_transfer_fedids_files(self):
        transfer = run_luigi_worker(db_ops_django.TransferAllFedIDsAndDatafiles(
            date=self.date, soak_db_filepath=str(os.path.join(self.working_dir, self.filepath) + '/*')))
        self.assertTrue(transfer)

    def test_check_files(self):
        check_files = run_luigi_worker(db_ops_django.CheckFiles(self.date, soak_db_filepath=str(os.path.join
                                                                                                (self.working_dir,
                                                                                                 self.filepath)
                                                                                                + '/*')))
        self.assertTrue(check_files)
