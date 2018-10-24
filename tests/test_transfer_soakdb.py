# DO NOT RUN IN PRODUCTION ENVIRONMENT!

import os
import unittest
import sqlite3
import json
import datetime

import pandas

from luigi_classes.transfer_soakdb import FindSoakDBFiles, TransferAllFedIDsAndDatafiles, CheckFiles, \
    TransferNewDataFile, transfer_file
from .test_functions import run_luigi_worker
from xchem_db.models import *
from functions.misc_functions import get_mod_date
from functions.db_functions import soakdb_query

# function list:
# + misc_functions.get_mod_date
# - luigi_classes.transfer_soakdb.transfer_file
# + soakdb_query

# task list:
# + FindSoakDBFiles
# + CheckFiles
# + TransferAllFedIDsAndDatafiles
# - TransferChangedDataFile
# + TransferNewDataFile
# - StartTransfers
# - CheckFileUpload
# - CheckUploadedFiles


class TestTransferSoakDBDependencyFunctions(unittest.TestCase):

    # variables
    tmp_file = 'tmp.txt'
    # filepath where test data is (in docker container) and filenames for soakdb
    filepath = '/pipeline/tests/data/soakdb_files/'
    db_file_name = 'soakDBDataFile.sqlite'
    json_file_name = 'soakDBDataFile.json'

    @classmethod
    def setUpClass(cls):
        # remove any previous soakDB file
        if os.path.isfile(os.path.join(cls.filepath, cls.db_file_name)):
            os.remove(os.path.join(cls.filepath, cls.db_file_name))

        # initialise db and json objects
        cls.db = os.path.join(cls.filepath, cls.db_file_name)
        cls.json_file = json.load(open(os.path.join(cls.filepath, cls.json_file_name)))

        # write json to sqlite file
        conn = sqlite3.connect(cls.db)
        df = pandas.DataFrame.from_dict(cls.json_file)
        df.to_sql("mainTable", conn, if_exists='replace')
        conn.close()

    def tearDown(self):
        pass

    def test_mod_date(self):
        os.system('touch ' + self.tmp_file)
        modification_dates = [datetime.datetime.now().strftime('%Y%m%d%H%M%S'), get_mod_date(self.tmp_file)]
        print(modification_dates)

        self.assertTrue(abs(int(modification_dates[0]) - int(modification_dates[1])) <= 30)

    def test_soakdb_query(self):
        results = soakdb_query(self.db)
        results_list = [dict(ix) for ix in results]
        print(results_list)
        print(self.json_file)

        self.assertTrue(len(results_list)==1)
        self.assertTrue(len(self.json_file)==1)

        for key in results_list[0].keys():
            if key not in self.json_file[0].keys():
                print('key: "' + str(key) + '" not found')
            else:
                print('checking ' + str(key) + '...')
                self.assertEqual(results_list[0][key], self.json_file[0][key])

    # NB: requires a soakdb object exists for the data file
    # def test_transfer_file(self):
    #     soak_db_dump = {'filename': self.db,
    #                     'proposal': Proposals.objects.get_or_create(proposal='lb13385')[0],
    #                     'modification_date': 0
    #                     }
    #
    #     sdb = SoakdbFiles.objects.get_or_create(**soak_db_dump)
    #
    #     transfer_file(self.db)


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

        cls.modification_date = get_mod_date(os.path.join(cls.filepath, cls.db_file_name))
        print(str('mdate: ' + cls.modification_date))
        cls.newfile_outfile = str(os.path.join(cls.filepath, cls.db_file_name)
                              + '_' + str(cls.modification_date) + '.transferred')

        # create log directories
        os.makedirs('/pipeline/logs/soakDBfiles')
        os.makedirs('/pipeline/logs/transfer_logs')

    @classmethod
    def tearDownClass(cls):
        # create log directories
        os.removedirs('/pipeline/logs/soakDBfiles')
        os.removedirs('/pipeline/logs/transfer_logs')

    def tearDown(self):
        output_files = [self.findsoakdb_outfile, self.transfer_outfile, self.checkfiles_outfile]

        for f in output_files:
            if os.path.isfile(f):
                os.remove(f)

        models = [Target, Compounds, Reference, SoakdbFiles, Reference, Proposals, Crystal, DataProcessing,
                  Dimple, Lab, Refinement, PanddaAnalysis, PanddaRun, PanddaEvent, PanddaSite, PanddaStatisticalMap]

        for m in models:
                m.objects.all().delete()

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

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles
    def test_transfer_fedids_files(self):
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

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles -> CheckFiles
    # scenario: nothing run yet, so requires FindSoakDBFiles and TransferAllFedIDsAndDataFiles
    def test_check_files(self):
        check_files = run_luigi_worker(CheckFiles(date=self.date, soak_db_filepath=self.filepath))
        output_file = CheckFiles(date=self.date, soak_db_filepath=self.filepath).output().path
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

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles -> CheckFiles
    # scenario: dump json into soakdb model to emulate existing record, check that status picked up as 1 (changed)
    # NB: Checks that data has actually been transfered by looking for lab entry, so have to emulate that too
    def test_check_files_changed(self):
        # create mock entry in soakdb table to represent file with 0 modification date
        soak_db_dump = {'filename': self.db,
                        'proposal': Proposals.objects.get_or_create(proposal='lb13385')[0],
                        'modification_date': 0
                        }

        sdb = SoakdbFiles.objects.get_or_create(**soak_db_dump)

        transfer_file(self.db)

        sdb[0].status = None
        sdb[0].save()

        # emulate soakdb task
        os.system('touch ' + self.findsoakdb_outfile)

        with open(self.findsoakdb_outfile, 'w') as f:
            f.write(self.db)

        # emulate transfer task
        os.system('touch ' + self.transfer_outfile)

        check_files = run_luigi_worker(CheckFiles(date=self.date, soak_db_filepath=self.filepath))
        output_file = CheckFiles(date=self.date, soak_db_filepath=self.filepath).output().path
        self.assertTrue(check_files)

        # check the transfer task has run (by worker)
        self.assertTrue(check_files)
        # check that the transfer task output is as expected
        self.assertEqual(output_file, self.checkfiles_outfile)
        # check that the status of the soakdb file has been set to 1 (changed)
        self.assertEqual(SoakdbFiles.objects.get(filename=self.db).status, 1)

    # tasks: FindSoakDBFiles -> TransferAllFedIDsAndDatafiles -> CheckFiles -> TransferChangedDatafile
    def test_transfer_changed_datafile(self):
        # create mock entry in soakdb table to represent file with 0 modification date
        soak_db_dump = {'filename': self.db,
                        'proposal': Proposals.objects.get_or_create(proposal='lb13385')[0],
                        'modification_date': self.date.strftime("%Y%m%d%H%M%S")
                        }

        sdb = SoakdbFiles.objects.get_or_create(**soak_db_dump)

        transfer_file(self.db)

        sdb[0].status = 1
        sdb[0].save()

        # emulate soakdb task
        os.system('touch ' + self.findsoakdb_outfile)

        with open(self.findsoakdb_outfile, 'w') as f:
            f.write(self.db)

        # emulate transfer task
        os.system('touch ' + self.transfer_outfile)

        # emulate check files
        os.system('touch ' + self.checkfiles_outfile)

        transfer_new = run_luigi_worker(TransferNewDataFile(data_file=self.db, soak_db_filepath=self.filepath))
        output_file = TransferNewDataFile(data_file=self.db, soak_db_filepath=self.filepath).output().path
        self.assertTrue(transfer_new)

        # check the transfer task has run (by worker)
        self.assertTrue(transfer_new)
        # check that the transfer task output is as expected
        self.assertEqual(output_file, self.newfile_outfile)
        # check that the status of the soakdb file has been set to 2 (changed)
        self.assertEqual(SoakdbFiles.objects.get(filename=self.db).status, 2)
