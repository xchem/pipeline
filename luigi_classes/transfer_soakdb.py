import luigi
import os
import setup_django
import subprocess
import datetime
from functions import db_functions, misc_functions, pandda_functions
from db.models import *
import pandas as pd
import traceback
from functions import misc_functions
from django.db import transaction


class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):

        # maybe change to *.sqlite to find renamed files? - this will probably pick up a tonne of backups
        command = str('''find ''' + self.filepath +  ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/tmp/*" -prune -o -path "*BACKUP*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print''')
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print(command)

        # run process to find sqlite files
        out, err = process.communicate()
        out = out.decode('ascii')
        print('OUTPUT:')
        print(out)

        # write filepaths to file as output
        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        print('Finding soakdb files via CheckFiles')
        soakdb = list(SoakdbFiles.objects.all())

        if not soakdb:
            return [TransferAllFedIDsAndDatafiles(soak_db_filepath=self.soak_db_filepath),
                    FindSoakDBFiles(filepath=self.soak_db_filepath)]
        else:
            return [FindSoakDBFiles(filepath=self.soak_db_filepath), FindSoakDBFiles(filepath=self.soak_db_filepath)]

    def output(self):
        return luigi.LocalTarget('logs/checked_files/files_' + str(self.date) + '.checked')

    @transaction.atomic
    def run(self):
        soakdb = SoakdbFiles.objects.all()
        print('SOAKDB:')
        for item in soakdb:
            print(item)

        # a list to hold filenames that have been checked
        checked = []

        # Status codes:-
        # 0 = new
        # 1 = changed
        # 2 = not changed

        print('INPUT NAME:')
        print(self.input()[1].path)

        with open(self.input()[1].path, 'r') as f:
            files = f.readlines()
            print('FILES:')
            print(files)

        for filename in files:
            # remove any newline characters
            filename_clean = filename.rstrip('\n')
            # find the relevant entry in the soakdbfiles table

            soakdb_query = list(SoakdbFiles.objects.select_for_update().filter(filename=filename_clean))

            print(len(soakdb_query))

            # raise an exception if the file is not in the soakdb table
            if len(soakdb_query) == 0:
                out, err, prop = db_functions.pop_soakdb(filename_clean)
                db_functions.pop_proposals(prop)

            # only one entry should exist per file
            if len(soakdb_query) == 1:
                # get the filename back from the query
                data_file = soakdb_query[0].filename
                # add the file to the list of those that have been checked
                checked.append(data_file)
                # get the modification date as stored in the db
                old_mod_date = soakdb_query[0].modification_date
                # get the current modification date of the file
                current_mod_date = misc_functions.get_mod_date(data_file)
                # get the id of the entry to write to
                id_number = soakdb_query[0].id

                print(old_mod_date)
                print(current_mod_date)

                # if the file has changed since the db was last updated for the entry, change status to indicate this
                if int(current_mod_date) > int(old_mod_date):
                    update_status = SoakdbFiles.objects.select_for_update().get(id=id_number)
                    update_status.status = 1
                    update_status.save()

                else:
                    update_status = SoakdbFiles.objects.select_for_update().get(id=id_number)
                    update_status.status = 0
                    update_status.save()

            # if there is more than one entry, raise an exception (should never happen - filename field is unique)
            if len(soakdb_query) > 1:
                raise Exception('More than one entry for file! Something has gone wrong!')

            # if the file is not in the database at all
            if filename_clean not in checked:
                # add the file to soakdb
                out, err, proposal = db_functions.pop_soakdb(filename_clean)
                # add the proposal to proposal
                db_functions.pop_proposals(proposal)
                # retrieve the new db entry
                soakdb_query = list(SoakdbFiles.objects.select_for_update().filter(filename=filename_clean))
                # get the id to update
                id_number = soakdb_query[0].id
                # update the relevant status to 0, indicating it as a new file
                update_status = SoakdbFiles.objects.select_for_update().get(id=id_number)
                update_status.status = 0
                update_status.save()


        # if the lab table is empty, no data has been transferred from the datafiles, so set status of everything to 0
        lab = list(Lab.objects.all())
        if not lab:
            # this is to set all file statuses to 0 (new file)
            soakdb = SoakdbFiles.objects.select_for_update().all()
            for filename in soakdb:
                filename.status = 0
                filename.save()

        # write output to signify job done
        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    # date parameter for daily run - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    # needs a list of soakDB files from the same day
    def requires(self):
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    # output is just a log file
    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/transfer_logs/fedids_%Y%m%d.txt'))

    # transfers data to a central postgres db
    def run(self):

        # use list from previous step as input to write to postgres
        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')

                # populate the soakdb table for each db file found by FindSoakDBFiles
                out, err, proposal = db_functions.pop_soakdb(database_file)
                print(out)
                print(err)
                print(proposal)

        # return a list of all proposals from db
        proposal_list = list(SoakdbFiles.objects.values_list('proposal', flat=True))

        # add fedid permissions via proposals table
        for proposal_number in set(proposal_list):
            db_functions.pop_proposals(proposal_number)

        # write output to show job done
        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    # no longer need this - assigned by django
    # file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles(soak_db_filepath=self.data_file)

    # def complete(self):
    #     soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
    #     if soakdb_query.status == 2:
    #         return True
    #     else:
    #         return False

    def output(self):
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        # delete all fields from soakdb filename
        maint_exists = db_functions.check_table_sqlite(self.data_file, 'mainTable')
        if maint_exists == 1:
            soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
            soakdb_query.delete()

            out, err, proposal = db_functions.pop_soakdb(self.data_file)
            db_functions.pop_proposals(proposal)

            db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=self.data_file,
                                        model=Crystal)
            db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=self.data_file,
                                        model=Lab)
            db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=self.data_file,
                                        model=Refinement)
            db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=self.data_file,
                                        model=Dimple)
            db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                        filename=self.data_file, model=DataProcessing)

        # retrieve the new db entry

        soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
        soakdb_query.status = 2
        soakdb_query.save()

        with self.output().open('w') as f:
            f.write('')


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    # no longer need this - assigned by django
    # file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    # def complete(self):
    #     soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
    #     if soakdb_query.status == 2:
    #         return True
    #     else:
    #         return False

    def output(self):
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        maint_exists = db_functions.check_table_sqlite(self.data_file, 'mainTable')
        if maint_exists==1:
            db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=self.data_file,
                                        model=Crystal)
            db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=self.data_file,
                                        model=Lab)
            db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=self.data_file,
                                        model=Refinement)
            db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=self.data_file,
                                        model=Dimple)
            db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                        filename=self.data_file, model=DataProcessing)

        soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
        soakdb_query.status = 2
        soakdb_query.save()

        with self.output().open('w') as f:
            f.write('')


class StartTransfers(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def get_file_list(self, status_code):

        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [object.filename for object in status_query]

        return datafiles

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget('logs/transfer_logs/transfers_' + str(self.date) + '.done')

    def run(self):
        new_list = self.get_file_list(0)
        changed_list = self.get_file_list(1)
        yield [TransferNewDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                for datafile in new_list], \
               [TransferChangedDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                for datafile in changed_list]

        with self.output().open('w') as f:
            f.write('')