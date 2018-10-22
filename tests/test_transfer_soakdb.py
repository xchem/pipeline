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

    def tearDown(self):
        output_files = [self.findsoakdb_outfile, self.transfer_outfile, self.checkfiles_outfile]

        for f in output_files:
            if os.path.isfile(f):
                os.remove(f)

        models = [Target, Compounds, Reference, SoakdbFiles, Reference, Proposals, Crystal, DataProcessing,
                  Dimple, Lab, Refinement, PanddaAnalysis, PanddaRun, PanddaEvent, PanddaSite, PanddaStatisticalMap]

        for m in models:
            try:
                m.objects.all().delete()
            except:
                continue

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
                        'modification_date': 0000000000000000,
                        'proposal': Proposals.objects.get_or_create(proposal='lb13385')[0],
                        }

        sdb = SoakdbFiles.objects.get_or_create(**soak_db_dump)

        transfer_file(self.db)

        sdb[0].status=None
        sdb[0].save()

        # emulate soakdb task
        os.system('touch ' + self.findsoakdb_outfile)

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
