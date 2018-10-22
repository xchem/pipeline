import os
import unittest
import sqlite3
import json
import datetime

import pandas

from luigi_classes.transfer_soakdb import FindSoakDBFiles, TransferAllFedIDsAndDatafiles, CheckFiles, \
    TransferNewDataFile
from .test_functions import run_luigi_worker
from xchem_db.models import *

# task list:
# + FindSoakDBFiles
# + CheckFiles
# + TransferAllFedIDsAndDatafiles
# - TransferChangedDataFile
# - TransferNewDataFile
# - StartTransfers
# - CheckFileUpload
# - CheckUploadedFiles


class TestTransferSoakDBTasks(unittest.TestCase):
    # filepath where test data is (in docker container) and filenames for soakdb
    filepath = '/pipeline/tests/data/soakdb_files/'
    db_file_name = 'soakDBDataFile.sqlite'
    json_file_name = 'soakDBDataFile.json'

    # date for tasks
    date = datetime.datetime.now()

    # variables to check
    findsoakdb_outfile = date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')
    transfer_outfile = date.strftime('logs/transfer_logs/fedids_%Y%m%d%H.txt')
    checkfiles_outfile = date.strftime('logs/checked_files/files_%Y%m%d%H.checked')

    @classmethod
    def setUpClass(cls):
        # remove any previous soakDB file
        if os.path.isfile(os.path.join(cls.filepath, cls.db_file_name)):
            os.remove(os.path.join(cls.filepath, cls.db_file_name))

        # initialise db and json objects
        cls.db = os.path.join(cls.filepath, cls.db_file_name)
        json_file = json.load(open(os.path.join(cls.filepath, cls.json_file_name)))

        # write json to sqlite file
        conn = sqlite3.connect(cls.db)
        df = pandas.DataFrame.from_dict(json_file)
        df.to_sql("mainTable", conn, if_exists='replace')
        conn.close()

        # create log directories
        os.makedirs('/pipeline/logs/soakDBfiles')
        os.makedirs('/pipeline/logs/transfer_logs')

    @classmethod
    def tearDownClass(cls):
        pass

    # tasks: FindSoakDBFiles
    def test_findsoakdb(self):
        # Run the FindSoakDBFiles task
        find_file = run_luigi_worker(FindSoakDBFiles(filepath=self.filepath, date=self.date))
        # find the output file according to the task
        output_file = FindSoakDBFiles(filepath=self.filepath).output().path
        # read the output file from the task
        output_text = open(output_file, 'r').read().rstrip()

        # check the task has run (by worker)
        self.assertTrue(find_file)
        # check the output file is as expected
        self.assertEqual(output_file, self.findsoakdb_outfile)
        # check the text in the output file is as expected
        self.assertEqual(output_text, self.db)

        # delete files created by the test
        os.remove(output_file)

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles
    def test_transfer_fedids_files(self):

        # make sure there's nothing in the soakdb_files table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        # run the task to transfer all fedids and datafiles
        transfer = run_luigi_worker(TransferAllFedIDsAndDatafiles(date=self.date,
                                                                  soak_db_filepath=self.filepath))

        output_file = TransferAllFedIDsAndDatafiles(date=self.date, soak_db_filepath=self.filepath).output().path

        # check the find files task has run (by output)
        self.assertTrue(os.path.isfile(self.findsoakdb_outfile))
        # check the transfer task has run (by worker)
        self.assertTrue(transfer)
        # check that the transfer task output is as expected
        self.assertEqual(output_file, self.transfer_outfile)

        # make sure there's nothing in the soakdb_files table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        # remove output files
        os.remove(output_file)
        os.remove(self.findsoakdb_outfile)

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles -> CheckFiles
    # scenario: nothing run yet, so requires FindSoakDBFiles and TransferAllFedIDsAndDataFiles
    def test_check_files(self):
        # make sure there's nothing in the soakdb_files table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        check_files = run_luigi_worker(CheckFiles(date=self.date, soakdb_filepath=self.filepath))
        output_file = CheckFiles(date=self.date, soakdb_filepath=self.filepath).output().path
        self.assertTrue(check_files)

        # check the find files task has run (by output)
        self.assertTrue(os.path.isfile(self.findsoakdb_outfile))
        # check that the fedid/transfer task has run (by output)
        self.assertTrue(os.path.isfile(self.transfer_outfile))
        # check the transfer task has run (by worker)
        self.assertTrue(check_files)
        # check that the transfer task output is as expected
        self.assertEqual(output_file, self.checkfiles_outfile)
        # check that the status of the soakdb file has been set to 0
        self.assertEqual(SoakdbFiles.objects.get(filename=self.db).status, 0)

        # make sure there's nothing in the soakdb_files table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        # remove output files
        os.remove(output_file)
        os.remove(self.transfer_outfile)
        os.remove(self.findsoakdb_outfile)

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles -> CheckFiles
    # scenario: dump json into soakdb model to emulate existing record, check that status picked up as 1 (changed)
    def test_check_files_changed(self):
        # create mock entry in soakdb table to represent file with 0 modification date
        soak_db_dump = {'filename': self.db,
                        'modification_date': 0000000000000000,
                        'proposal': Proposals.objects.get_or_create(proposal='lb13385')[0],
                        }

        SoakdbFiles.objects.get_or_create(**soak_db_dump)

        check_files = run_luigi_worker(CheckFiles(date=self.date, soakdb_filepath=self.filepath))
        output_file = CheckFiles(date=self.date, soakdb_filepath=self.filepath).output().path
        self.assertTrue(check_files)

        # check the find files task has run (by output)
        self.assertTrue(os.path.isfile(self.findsoakdb_outfile))
        # check that the fedid/transfer task has run (by output)
        self.assertTrue(os.path.isfile(self.transfer_outfile))
        # check the transfer task has run (by worker)
        self.assertTrue(check_files)
        # check that the transfer task output is as expected
        self.assertEqual(output_file, self.checkfiles_outfile)
        # check that the status of the soakdb file has been set to 0
        self.assertEqual(SoakdbFiles.objects.get(filename=self.db).status, 2)

        # make sure there's nothing in the soakdb_files table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()

        # remove output files
        os.remove(output_file)
        os.remove(self.transfer_outfile)
        os.remove(self.findsoakdb_outfile)


    # def test_transfers(self):
    #     print('TESTING TRANSFERS: test_transfers')
    #     find_file = run_luigi_worker(FindSoakDBFiles(
    #         filepath=str(self.working_dir + '*')))
    #     self.assertTrue(find_file)
    #     test_new_file = run_luigi_worker(TransferNewDataFile(data_file=self.db_full_path,
    #                                                          soak_db_filepath=str(self.working_dir + '*')
    #                                                          ))
    #     self.assertTrue(test_new_file)
    #
    #     # get the status value from soakdb entry for current file
    #     status = list(SoakdbFiles.objects.values_list('status', flat=True))
    #     # check == 2 (not changed - ie. has been successfully added)
    #     self.assertEqual(int(status[0]), 2)

#         test_changed_file = run_luigi_worker(TransferChangedDataFile(data_file=self.db_full_path,
#                                                                           soak_db_filepath=str(self.working_dir + '*')
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







