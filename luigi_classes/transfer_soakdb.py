from sqlite3 import OperationalError
from setup_django import setup_django

setup_django()

import datetime
import luigi

from functions.luigi_transfer_soakdb_functions import *
from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig


class FindSoakDBFiles(luigi.Task):
    """Luigi Class to find soakdb files within a given directory.
    Does not require any pre-requisite tasks to be run.

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.datetime.now())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def output(self):
        """Defines Expected output of :class:`FindSoakDBFiles`

        :return: Creates a log file: 'soakDBfiles/soakDB_%Y%m%d.txt' in Luigi config dir.
        :rtype: None
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt')))

    def run(self):
        """Defines what tasks should be run when :class:`FindSoakDBFiles` is performed.

        :return: Writes output of `find_soak_db_files` as described by :class:`FindSoakDBFiles.output()`
        :rtype: None
        """
        out = find_soak_db_files(filepath=self.filepath)

        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    """Luigi Class to check if a given soakdb file has been uploaded to XCD
    If file is in XCDB, requires :class:`FindSoakDBFiles` to be completed
    else requires :class:`FindSoakDBFiles` and :class:`TransferAllFedIDsAndDatafiles`

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        """Requirements for :class:`CheckFiles` to be run
        If file is in XCDB, requires :class:`FindSoakDBFiles` to be completed
        else requires :class:`FindSoakDBFiles` and :class:`TransferAllFedIDsAndDatafiles`

        """
        print('Finding soakdb files via CheckFiles')
        soakdb = list(SoakdbFiles.objects.all())

        if not soakdb:
            return [TransferAllFedIDsAndDatafiles(soak_db_filepath=self.soak_db_filepath),
                    FindSoakDBFiles(filepath=self.soak_db_filepath)]
        else:
            return [FindSoakDBFiles(filepath=self.soak_db_filepath), FindSoakDBFiles(filepath=self.soak_db_filepath)]

    def output(self):
        """Defines Expected output of :class:`CheckFiles`

        :return: Creates a log file: 'checked_files/files_%Y%m%d%H.checked' in Luigi config dir.
        :rtype: None
        """
        return luigi.LocalTarget(os.path.join(
            DirectoriesConfig().log_directory,
            self.date.strftime('checked_files/files_%Y%m%d%H.checked')))

    def run(self):
        """Defines what tasks should be run when :class:`CheckFiles` is performed.

        :return: Performed the `check_files` functions and writes '' to a logfile as described by :class:`CheckFiles.output()`
        :rtype: None
        """
        check_files(soak_db_filepath=self.input[1].path)
        # write output to signify job done
        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    """Luigi Class to transfers FedID and DataFiles into XCDB
    Requires :class:`FindSoakDBFiles` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    # date parameter for daily run - needs to be changed
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    # needs a list of soakDB files from the same day
    def requires(self):
        """Requirements for :class:`TransferAllFedIDsAndDatafiles` to be run
        Requires :class:`FindSoakDBFiles` to be completed

        """
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    # output is just a log file
    def output(self):
        """Defines Expected output of :class:`TransferAllFedIDsAndDatafiles`

        :return: Creates a log file: 'transfer_logs/fedids_%Y%m%d%H.txt' in Luigi config dir.
        :rtype: None
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('transfer_logs/fedids_%Y%m%d%H.txt')))

    # transfers data to a central postgres db
    def run(self):
        """Defines what tasks should be run when :class:`CheckFiles` is performed.

        :return: Performed the `transfer_all_fed_ids_and_datafiles` functions and writes 'TransferFeDIDs DONE'
        to a logfile as described by :class:`TransferAllFedIDsAndDatafiles.output()`
        :rtype: None
        """
        transfer_all_fed_ids_and_datafiles(soak_db_filepath=self.input())
        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    """Luigi Class to transfer changed files into XCDB
    Requires :class:`CheckFiles` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)

    def requires(self):
        """Requirements for :class:`TransferChangedDataFile` to be run
        Requires :class:`CheckFiles` to be completed

        """
        return CheckFiles(soak_db_filepath=self.data_file)

    def output(self):
        """Defines Expected output of :class:`TransferChangedDataFile`

        :return: Creates a log file: '[filename]_[modification_date].transferred' in Luigi config dir.
        :rtype: None
        """
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        """Defines what tasks should be run when :class:`TransferChangedDataFile` is performed.

        :return: Performed the `transfer_changed_datafile` functions and writes ''
        to a logfile as described by :class:`TransferChangedDataFile.output()`
        :rtype: None
        """
        transfer_changed_datafile(data_file=self.data_file, hit_directory=self.hit_directory)

        with self.output().open('w') as f:
            f.write('')


class TransferNewDataFile(luigi.Task):
    """Luigi Class to transfer changed files into XCDB
    Requires :class:`CheckFiles` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        """Requirements for :class:`TransferNewDataFile` to be run
        Requires :class:`CheckFiles` to be completed

        """
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        """Defines Expected output of :class:`TransferNewDataFile`

        :return: Creates a log file: '[filename]_[modification_date].transferred' in Luigi config dir.
        :rtype: None
        """
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        """Defines what tasks should be run when :class:`TransferNewDataFile` is performed.

        :return: Performed the `transfer_file` functions and writes ''
        to a logfile as described by :class:`TransferNewDataFile.output()`
        :rtype: None
        """
        transfer_file(self.data_file)

        with self.output().open('w') as f:
            f.write('')


class StartTransfers(luigi.Task):
    """Luigi Class to initiate transfer sequence into XCDB
    Requires :class:`CheckFiles` or both :class:`TransferNewDataFile` and :class:`TransferChangedDataFile` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def get_file_list(self, status_code):
        """Get a list of files to attempt to transfer.

        :param status_code: Not sure, ask Rachael
        :type status_code: str

        :return: list of filenames that are to be transferred to XCDB
        :rtype: list
        """
        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [o.filename for o in status_query]

        return datafiles

    def requires(self):
        """Requirements for :class:`StartTransfers` to be run
        Requires :class:`CheckFiles` to be completed or both :class:`TransferNewDataFile` and :class:`TransferChangedDataFile`

        """
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
        """Defines Expected output of :class:`StartTransfers`

        :return: Creates a log file: 'transfer_logs/transfers_[date].done' in Luigi config dir.
        :rtype: None
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('transfer_logs/transfers_' + str(self.date) + '.done')))

    def run(self):
        """Defines what tasks should be run when :class:`StartTransfers` is performed.

        :return: writes '' to a logfile as described by :class:`StartTransfers.output()`
        :rtype: None
        """
        with self.output().open('w') as f:
            f.write('')


class CheckFileUpload(luigi.Task):
    """Luigi Class to check if files uploaded

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    filename = luigi.Parameter()
    model = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        """Defines Expected output of :class:`CheckFileUpload`

        :return: Creates a log file: 'filename.data.checked' in Luigi config dir.
        :rtype: None
        """
        mod_date = misc_functions.get_mod_date(self.filename)
        return luigi.LocalTarget(str(self.filename + '.' + mod_date + '.checked'))

    def run(self):
        """Defines what tasks should be run when :class:`CheckFileUpload` is performed.

        :return: Performes `check_file_upload` and writes '' to a logfile as described by :class:`CheckFileUpload.output()`
        :rtype: None
        """
        check_file_upload(filename=self.filename, model=self.model)
        with self.output().open('w') as f:
            f.write('')


class CheckUploadedFiles(luigi.Task):
    """Luigi Class to check all uploaded Files
    Requires :class:`StartTransfers` or `CheckFileUpload` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        """Requirements for :class:`CheckUploadedFiles` to be run
        Requires :class:`StartTransfers` to be completed if file is missing or
        :class:`CheckFileUpload` if exists

        """
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
        """Defines Expected output of :class:`CheckUploadedFiles`

        :return: Creates a log file: 'soakDBfiles/soakDB_checked_%Y%m%d.txt' in Luigi config dir.
        :rtype: None
        """

        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_checked_%Y%m%d.txt')))

    def run(self):
        """Defines what tasks should be run when :class:`CheckUploadedFiles` is performed.

        :return: writes '' to a logfile as described by :class:`CheckFileUpload.output()`
        :rtype: None
        """
        with self.output().open('w') as f:
            f.write('')
