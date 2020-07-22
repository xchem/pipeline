from sqlite3 import OperationalError
from setup_django import setup_django

setup_django()

import datetime
import luigi

from functions.luigi_transfer_soakdb_functions import *
from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig


class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.datetime.now())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt')))

    def run(self):
        out = find_soak_db_files(filepath=self.filepath)

        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        print('Finding soakdb files via CheckFiles')
        soakdb = list(SoakdbFiles.objects.all())

        if not soakdb:
            return [TransferAllFedIDsAndDatafiles(soak_db_filepath=self.soak_db_filepath),
                    FindSoakDBFiles(filepath=self.soak_db_filepath)]
        else:
            return [FindSoakDBFiles(filepath=self.soak_db_filepath), FindSoakDBFiles(filepath=self.soak_db_filepath)]

    def output(self):
        return luigi.LocalTarget(os.path.join(
            DirectoriesConfig().log_directory,
            self.date.strftime('checked_files/files_%Y%m%d%H.checked')))

    def run(self):
        check_files(soak_db_filepath=self.input[1].path)
        # write output to signify job done
        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    resources = {'django': 1}
    # date parameter for daily run - needs to be changed
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    # needs a list of soakDB files from the same day
    def requires(self):
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    # output is just a log file
    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('transfer_logs/fedids_%Y%m%d%H.txt')))

    # transfers data to a central postgres db
    def run(self):
        transfer_all_fed_ids_and_datafiles(soak_db_filepath=self.input())
        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)

    def requires(self):
        return CheckFiles(soak_db_filepath=self.data_file)

    def output(self):
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        transfer_changed_datafile(data_file=self.data_file, hit_directory=self.hit_directory)

        with self.output().open('w') as f:
            f.write('')


class TransferNewDataFile(luigi.Task):
    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        transfer_file(self.data_file)

        with self.output().open('w') as f:
            f.write('')


class StartTransfers(luigi.Task):
    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def get_file_list(self, status_code):

        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [o.filename for o in status_query]

        return datafiles

    def requires(self):
        if not os.path.isfile(CheckFiles(soak_db_filepath=self.soak_db_filepath).output().path):
            return CheckFiles(soak_db_filepath=self.soak_db_filepath)
        else:
            new_list = self.get_file_list(0)
            changed_list = self.get_file_list(1)
            return [TransferNewDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                    for datafile in new_list], \
                   [TransferChangedDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                    for datafile in changed_list]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('transfer_logs/transfers_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CheckFileUpload(luigi.Task):
    resources = {'django': 1}
    filename = luigi.Parameter()
    model = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        mod_date = misc_functions.get_mod_date(self.filename)
        return luigi.LocalTarget(str(self.filename + '.' + mod_date + '.checked'))

    def run(self):
        check_file_upload(filename=self.filename, model=self.model)
        with self.output().open('w') as f:
            f.write('')


class CheckUploadedFiles(luigi.Task):
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        if not os.path.isfile(StartTransfers(date=self.date, soak_db_filepath=self.soak_db_filepath).output().path):
            return StartTransfers(date=self.date, soak_db_filepath=self.soak_db_filepath)
        else:
            soakdb_files = [obj.filename for obj in SoakdbFiles.objects.all()]
            m = [Lab, Dimple, DataProcessing, Refinement]
            zipped = []
            for filename in soakdb_files:
                for model in m:
                    try:
                        maint_exists = db_functions.check_table_sqlite(filename, 'mainTable')
                    except OperationalError:
                        if not os.path.isfile(filename):
                            f = SoakdbFiles.objects.get(filename=filename)
                            f.delete()
                            continue
                        else:
                            raise Exception(str(traceback.format_exc() + '; db_file=' + filename))
                    if maint_exists == 1:
                        zipped.append(tuple([filename, model]))

            return [CheckFileUpload(filename=filename, model=model) for (filename, model) in zipped]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_checked_%Y%m%d.txt')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')
