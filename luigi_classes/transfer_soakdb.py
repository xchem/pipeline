from sqlite3 import OperationalError
from setup_django import setup_django

setup_django()

import datetime
import luigi

from functions.luigi_transfer_soakdb_functions import *
from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig


class FindSoakDBFiles(luigi.Task):
    """ Find and return a list of all soakdb files within a specified directory

    This class requires no prerequisite tasks to be completed to run

    Args:
        date: A date that will be used to create the output file...
        filepath: The file/directory path to look for soakdb files.

    """

    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.datetime.now())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def output(self):
        """ Returns the specified output for this task (:class:`FindSoakDBFiles`)

        The naming convention for the output file follows : soakDBfiles/soakDB_%Y%m%d.txt given the date.
        and should be located in the specified log directory.

        Returns:
            luigi.localTarget
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt')))

    def run(self):
        """Performs `find_soak_db_files` and creates text file containing valid soakdb filepaths."""
        out = find_soak_db_files(filepath=self.filepath)

        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    """ Check if a given soakdb file has been uploaded to XCDB.

    If the file is in XCDB this task requires :class:`FindSoakDBFiles` to be completed
    If the file is NOT in XCDB then :class:`FindSoakDBFiles` and :class:`TransferAllFedIDsAndDatafiles` are required.

    Args:
        date: A date that will be used to create the output file
        soak_db_filepath: The filepath pointing to a given soakdb file.

    """

    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        """ CheckFiles requires :class:`FindSoakDBFiles` and :class:`TransferAllFedIDsAndDatafiles`

        CheckFiles expects a soak_db_filepath as a parameter given under default_path

        Returns:
            [(output of TransferAllFedIDs or FindSoakDBFiles), output of FindSoakDBFiles]
        """
        print('Finding soakdb files via CheckFiles')
        soakdb = list(SoakdbFiles.objects.all())

        if not soakdb:
            return [TransferAllFedIDsAndDatafiles(soak_db_filepath=self.soak_db_filepath),
                    FindSoakDBFiles(filepath=self.soak_db_filepath)]
        else:
            return [FindSoakDBFiles(filepath=self.soak_db_filepath), FindSoakDBFiles(filepath=self.soak_db_filepath)]

    def output(self):
        """ Returns the target output for :class:`CheckFiles` task.

        Naming convention for the output file is 'checked_files/files_%Y%m%d%H.checked'

        Returns:
            luigi.localTarget
        """
        return luigi.LocalTarget(os.path.join(
            DirectoriesConfig().log_directory,
            self.date.strftime('checked_files/files_%Y%m%d%H.checked')))

    def run(self):
        """ Performs `check_files` function and writes '' to the expected log file"""
        check_files(soak_db_filepath=self.input()[1].path)
        # write output to signify job done
        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    """ Transfer All FedID and Datafiles from within a soak-db file into XCDB?

    This task requires :class:`FindSoakDBfiles` to be completed

    Args:
        date:
        soak_db_filepath:
    """

    resources = {'django': 1}
    # date parameter for daily run - needs to be changed
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    # needs a list of soakDB files from the same day
    def requires(self):
        """TransferAllFedIDsAndDatafiles requires :class:`FindSoakDBFiles` to be completed

        TransferAllFedIDsAndDatafiles expects a soak_db_filepath as a parameter given under default_path

        Returns:
            [output of FindSoakDBFiles]
        """
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    # output is just a log file
    def output(self):
        """Returns the target output for :class:`TransferAllFedIDsAndDatafiles`

        Naming convention for the output file is 'transfer_logs/fedids_%Y%m%d%H.txt'

        Returns:
            luigi.localTarget
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('transfer_logs/fedids_%Y%m%d%H.txt')))

    # transfers data to a central postgres db
    def run(self):
        """ Performs `transfer_all_fed_ids_and_datafiles` function and writes 'TransferFeDIDs DONE' to the expected log file"""
        transfer_all_fed_ids_and_datafiles(soak_db_filelist=self.input().path)
        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    """Transfer soakdb files that have been modified since upload

    Requires :class:`CheckFiles` task to be completed

    Args:
        data_file:
        hit_directory:
    """

    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)

    def requires(self):
        """TransferChangedDataFile requires :class:`CheckFiles` to be completed.

        Expects the data_file given as a parameter

        Returns:
            Output of CheckFiles
        """
        return CheckFiles(soak_db_filepath=self.data_file)

    def output(self):
        """Returns the target output for :class:`TransferChangedDataFile`

        Naming convention for the output file is 'data+file+mod_date.transferred'

        Returns:
            luigi.localTarget
        """
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        """ Performs `transfer_changed_datafile` function and writes '' to the expected log file"""

        transfer_changed_datafile(data_file=self.data_file, hit_directory=self.hit_directory)

        with self.output().open('w') as f:
            f.write('')


class TransferNewDataFile(luigi.Task):
    """Transfer new soakdb files to XCDB.

    Requires :class:`CheckFiles` task to be completed

    Args:
        soak_db_filepath:
        data_file:
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
        """Returns the target output for :class:`TransferNewDataFile`

        Creates a log file: '[data_file]_[modification_date].transferred' in Luigi config dir.

        Returns:
            luigi.localTarget
        """
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        """ Performs `transfer_file` function and writes '' to the expected log file"""
        transfer_file(self.data_file)

        with self.output().open('w') as f:
            f.write('')


class StartTransfers(luigi.Task):
    """Initiate the transfer sequence of files into XCDB

    Requires :class:`CheckFiles` or both :class:`TransferNewDataFile` and :class:`TransferChangedDataFile` to be completed

    Args:
        date:
        soak_db_filepath:
    """

    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def get_file_list(self, status_code):
        """Get a list of files to attempt to transfer.

        Args:
            status_code: Not sure, ask Rachael

        Returns:
            list of filenames that are to be transferred to XCDB
        """
        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [o.filename for o in status_query]

        return datafiles

    def requires(self):
        """Requirements for :class:`StartTransfers` to be run

        Requires :class:`CheckFiles` to be completed or both :class:`TransferNewDataFile` and :class:`TransferChangedDataFile`

        Returns:
            Output of CheckFiles
            or
            Output of one of TransferNewDataFile or TransferChangedDataFile depending on state of soakdb file.
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
        """Returns the target output for :class:`StartTransfers`

        Creates a log file: 'transfer_logs/transfers_[date].done' in Luigi config dir.

        Returns:
            luigi.localTarget
        """
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('transfer_logs/transfers_' + str(self.date) + '.done')))

    def run(self):
        """Write to the output file, otherwise schedules required tasks."""
        with self.output().open('w') as f:
            f.write('')


class CheckFileUpload(luigi.Task):
    """Check if a file has uploaded correctly

    Has no requirements

    Args:
        filename:
        model:
    """

    resources = {'django': 1}
    filename = luigi.Parameter()
    model = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        """Returns the target output for :class:`CheckFileUpload`

        Creates a log file: 'filename.date.checked' in Luigi config dir.

        Returns:
            luigi.localTarget
        """
        mod_date = misc_functions.get_mod_date(self.filename)
        return luigi.LocalTarget(str(self.filename + '.' + mod_date + '.checked'))

    def run(self):
        """Performs the check_file_upload function on the given filename and model"""
        check_file_upload(filename=self.filename, model=self.model)
        with self.output().open('w') as f:
            f.write('')


class CheckUploadedFiles(luigi.Task):
    """Check whether or not all specified soakdb files have uploaded correctly

    Requires :class:`StartTransfers` or `CheckFileUpload` to be completed

    Args:
        date:
        soak_db_filepath:
    """

    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        """Requirements for :class:`CheckUploadedFiles` to be run

        Requires :class:`StartTransfers` to be completed if file is missing or
        :class:`CheckFileUpload` if exists

        Returns:
            Output of CheckFileUpload (if in XCDB) or StartTransfers (if not in XCDB)
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
        """Returns the target output for:class:`CheckUploadedFiles`

        Creates a log file: 'soakDBfiles/soakDB_checked_%Y%m%d.txt' in Luigi config dir.
        Returns:
            luigi.localTarget
        """

        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              self.date.strftime('soakDBfiles/soakDB_checked_%Y%m%d.txt')))

    def run(self):
        """Write to the output file, otherwise schedules required tasks."""
        with self.output().open('w') as f:
            f.write('')
