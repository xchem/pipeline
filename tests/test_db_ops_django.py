import os
import shutil
import unittest

import datetime

from luigi_classes.transfer_soakdb import FindSoakDBFiles, TransferAllFedIDsAndDatafiles, CheckFiles, \
    TransferNewDataFile
from test_functions import run_luigi_worker
from xchem_db.models import *


class TestTasks(unittest.TestCase):
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

    def test_findsoakdb(self):
        print('TESTING FINDSOAKDB: test_findsoakdb')
        os.chdir(self.working_dir)
        print(os.path.join(self.working_dir) + '*')
        find_file = run_luigi_worker(FindSoakDBFiles(
            filepath=str(self.working_dir + '*')))
        self.assertTrue(find_file)

    def test_transfer_fedids_files(self):
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        run_luigi_worker(FindSoakDBFiles(
            filepath=str(self.working_dir + '*')))
        print('TESTING FEDIDS: test_transfer_fedids_files')
        transfer = run_luigi_worker(TransferAllFedIDsAndDatafiles(
            date=self.date, soak_db_filepath=str(self.working_dir + '*')))
        self.assertTrue(transfer)

    def test_check_files(self):
        print('TESTING CHECK_FILES: test_check_files')
        check_files = run_luigi_worker(CheckFiles(soak_db_filepath=str(self.working_dir + '*')))
        self.assertTrue(check_files)

        # get the status value from soakdb entry for current file
        print('\n')
        print('status:')
        status = list(SoakdbFiles.objects.values_list('status', flat=True))
        print(status)
        print('\n')

    def test_transfers(self):
        print('TESTING TRANSFERS: test_transfers')
        find_file = run_luigi_worker(FindSoakDBFiles(
            filepath=str(self.working_dir + '*')))
        self.assertTrue(find_file)
        test_new_file = run_luigi_worker(TransferNewDataFile(data_file=self.db_full_path,
                                                                           soak_db_filepath=str(self.working_dir + '*')
                                                                           ))
        self.assertTrue(test_new_file)


        # get the status value from soakdb entry for current file
        status = list(SoakdbFiles.objects.values_list('status', flat=True))
        # check == 2 (not changed - ie. has been successfully added)
        self.assertEqual(int(status[0]), 2)

#         test_changed_file = run_luigi_worker(TransferChangedDataFile(data_file=self.db_full_path,
#                                                                            soak_db_filepath=str(self.working_dir + '*')
#                                                                                    ))
#
#         self.assertTrue(test_changed_file)
#
#         status = list(SoakdbFiles.objects.values_list('status', flat=True))
#         # check == 2 (not changed - ie. has been successfully added)
#         self.assertEqual(int(status[0]), 2)
#
#
# class TestTransferFunctions(unittest.TestCase):
#     # filepath where test data is
#     filepath = 'tests/docking_files/database/'
#     db_file_name = 'soakDBDataFile.sqlite'
#     # tmp directory to test in
#     tmp_dir = 'tmp/'
#     date = datetime.date.today()
#
#     @classmethod
#     def setUpClass(cls):
#         cls.top_dir = os.getcwd()
#         cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)
#         cls.db_full_path = os.path.join(cls.working_dir, cls.db_file_name)
#         print('Working dir: ' + cls.working_dir)
#         shutil.copytree(os.path.join(cls.top_dir, cls.filepath), cls.working_dir)
#
#     @classmethod
#     def tearDownClass(cls):
#         shutil.rmtree(cls.working_dir)
#         os.chdir(cls.top_dir)
#         # delete rows created in soakdb table
#         # soakdb_rows = SoakdbFiles.objects.all()
#         # soakdb_rows.delete()
#         # # delete rows created in proposals table
#         # proposal_rows = Proposals.objects.all()
#         # proposal_rows.delete()
#         # # delete rows created in crystals table
#         # crystal_rows = Crystal.objects.all()
#         # crystal_rows.delete()
#
#     def test_transfer_crystal(self):
#         print('TESTING CRYSTAL: test_transfer_crystal')
#
#         db_functions.transfer_table(translate_dict=db_functions.crystal_translations(),
#                                     filename=self.db_full_path, model=Crystal)
#
#         self.assertTrue(list(Crystal.objects.all()))
#
#         crystals = Crystal.objects.values_list('crystal_name', flat=True)
#         print(crystals)
#
#     def test_transfer_lab(self):
#         db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=self.db_full_path,
#                                     model=Lab)
#         self.assertTrue(list(Lab.objects.all()))
#
#     def test_transfer_refinement(self):
#         db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=self.db_full_path,
#                                     model=Refinement)
#         self.assertTrue(list(Refinement.objects.all()))
#
#
#     def test_transfer_dimple(self):
#         db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=self.db_full_path,
#                                     model=Dimple)
#         self.assertTrue(list(Dimple.objects.all()))
#
#     def test_transfer_data_processing(self):
#         db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
#                                     filename=self.db_full_path, model=DataProcessing)
#         self.assertTrue(list(DataProcessing.objects.all()))







