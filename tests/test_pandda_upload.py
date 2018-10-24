import os
import shutil
import unittest

import datetime

import functions.pandda_functions as pf
from luigi_classes.transfer_pandda import *
from .test_functions import run_luigi_worker


class TestFindLogs(unittest.TestCase):
    date = datetime.datetime.now()
    search_path = '/pipeline/tests/data/*'
    sdb_filepath = '/pipeline/tests/data/database/'
    db_file_name = 'soakDBDataFile.sqlite'
    db = os.path.join(sdb_filepath, db_file_name)
    findsoakdb_outfile = date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')

    @classmethod
    def setUpClass(cls):
        # create log directories
        os.makedirs('/pipeline/logs/soakDBfiles')
        os.makedirs('/pipeline/logs/search_paths')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('/pipeline/logs')

    # tasks: FindSoakDBFiles -> FindSearchPaths
    def test_find_search_paths(self):
        # imitate FindSoakDB task
        # emulate soakdb task
        os.system('touch ' + self.findsoakdb_outfile)

        with open(self.findsoakdb_outfile, 'w') as f:
            f.write(self.db)

        with open(self.findsoakdb_outfile, 'r') as f:
            print(f.read())

        find_paths = run_luigi_worker(FindSearchPaths(soak_db_filepath=self.sdb_filepath,
                                                      date_time=self.date.strftime("%Y%m%d%H")))

        self.assertTrue(find_paths)
        self.assertTrue(
            '0,/pipeline/tests/data/database/soakDBDataFile.sqlite,/pipeline/tests/data/,/pipeline/tests/data/database/'
            in open(FindSearchPaths(soak_db_filepath=os.path.join(self.sdb_filepath, self.db_file_name),
                                    date_time=self.date.strftime("%Y%m%d%H")).output().path, 'r').read())
        data = FindSearchPaths(soak_db_filepath=os.path.join(self.sdb_filepath, self.db_file_name),
                                    date_time=self.date.strftime("%Y%m%d%H")).output().path, 'r').read()
        with open('tmp_find_paths_output.csv', 'w') as f:
            f.write(data)

        # with open(FindSearchPaths(soak_db_filepath=self.sdb_filepath,
        #                           date_time=self.date.strftime("%Y%m%d%H")).output().path, 'r') as f:
        #     print(f.read())


    # def test_find_pandda_logs(self):
    #     find_logs = run_luigi_worker(FindPanddaLogs(
    #         search_path=
    #         '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/panddas_alice',
    #         soak_db_filepath=str(self.working_dir + '*')))
    #
    #     self.assertTrue(find_logs)
    #
    # def test_add_pandda_runs(self):
    #
    #     remove_files = []
    #
    #     log_files = pf.find_log_files(
    #         '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/tests/docking_files/')
    #     log_files = log_files.split()
    #
    #     for log_file in log_files:
    #
    #         pver, input_dir, output_dir, sites_file, events_file, err = pf.get_files_from_log(log_file)
    #
    #         if not err and sites_file and events_file and '0.1.' not in pver:
    #             remove_path = str('/'.join(log_file.split('/')[:-1]))
    #             remove_files.append(remove_path)
    #
    #             add_run = run_luigi_worker(AddPanddaRun(
    #                 log_file=log_file, pver=pver, input_dir=input_dir, output_dir=output_dir, sites_file=sites_file,
    #                 events_file=events_file))
    #
    #             self.assertTrue(add_run)
    #
    #             add_sites = run_luigi_worker(AddPanddaSites(log_file=log_file, pver=pver,
    #                                                         input_dir=input_dir, output_dir=output_dir,
    #                                                         sites_file=sites_file,
    #                                                         events_file=events_file,
    #                                                         soakdb_filename=os.path.join(
    #                                                             self.working_dir, 'soakDBDataFile.sqlite'))
    #                                          )
    #
    #             self.assertTrue(add_sites)
    #
    #             if os.path.isfile(str(log_file + '.sites.done')):
    #                 os.remove(str(log_file + '.sites.done'))
    #
    # def test_all_for_nudt7_en(self):
    #     pass
