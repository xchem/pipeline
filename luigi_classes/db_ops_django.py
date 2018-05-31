import luigi
import subprocess
import os
import datetime
from functions import db_functions, misc_functions, pandda_functions
from sqlalchemy import create_engine
import pandas
import sqlite3
import setup_django
from db.models import *
from django.db.models import Q


class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):

        # maybe change to *.sqlite to find renamed files? - this will probably pick up a tonne of backups
        command = str('''find ''' + self.filepath +  ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print''')
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
            soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))

            print(len(soakdb_query))

            # raise an exception if the file is not in the soakdb table
            if len(soakdb_query) == 0:
                raise Exception(str('No entry found for ' + str(filename_clean)))

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
                    update_status = SoakdbFiles.objects.get(id=id_number)
                    update_status.status = 1
                    update_status.save()

                else:
                    update_status = SoakdbFiles.objects.get(id=id_number)
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
                soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))
                # get the id to update
                id_number = soakdb_query[0].id
                # update the relevant status to 0, indicating it as a new file
                update_status = SoakdbFiles.objects.get(id=id_number)
                update_status.status = 0
                update_status.save()


        # if the lab table is empty, no data has been transferred from the datafiles, so set status of everything to 0
        lab = list(Lab.objects.all())
        if not lab:
            # this is to set all file statuses to 0 (new file)
            soakdb = SoakdbFiles.objects.all()
            soakdb.status = 0
            soakdb.update()

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

    def output(self):
        pass

    def run(self):
        # pass
        # delete all fields from soakdb filename
        soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
        soakdb_query.delete()

        db_functions.pop_soakdb(self.data_file)

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

        soakdb_query = list(SoakdbFiles.objects.filter(filename=self.data_file))
        # get the id to update
        id_number = soakdb_query[0].id
        # update the relevant status to 0, indicating it as a new file
        update_status = SoakdbFiles.objects.get(id=id_number)
        update_status.status = 2
        update_status.save()


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    # no longer need this - assigned by django
    # file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def run(self):
        #db_functions.transfer_table(translate_dict=db_functions)
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
        soakdb_query = list(SoakdbFiles.objects.filter(filename=self.data_file))
        # get the id to update
        id_number = soakdb_query[0].id
        # update the relevant status to 0, indicating it as a new file
        update_status = SoakdbFiles.objects.get(id=id_number)
        update_status.status = 2
        update_status.save()


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


class FindProjects(luigi.Task):

    def requires(self):
        pass
        # return CheckFiles(), StartTransfers()

    def output(self):
        return luigi.LocalTarget('logs/findprojects.done')

    def run(self):
        # all data necessary for uploading hits
        hits_dict = {'crystal_name': [], 'protein': [], 'smiles': [], 'bound_conf': [],
                                  'modification_date': [], 'strucid':[]}

        # all data necessary for uploading leads
        leads_dict = {'protein': [], 'pandda_path': [], 'reference_pdb': [], 'strucid':[]}

        # class ProasisHits(models.Model):
        #     bound_pdb = models.ForeignKey(Refinement, to_field='bound_conf', on_delete=models.CASCADE, unique=True)
        #     crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
        #     modification_date = models.TextField(blank=True, null=True)
        #     strucid = models.TextField(blank=True, null=True)
        #     ligand_list = models.IntegerField(blank=True, null=True)

        ref_or_above = Refinement.objects.filter(outcome__in=[3, 4, 5, 6])

        for entry in ref_or_above:
            print(entry.crystal_name.crystal_name)
            if entry.bound_conf:
                print(entry.bound_conf)
            else:
                print(entry.pdb_latest)


        # lab_table_select = Lab.objects.filter(crystal_name=ref_or_above.crystal_name)
        #
        # for row in rows:
        #
        #     c.execute('''SELECT smiles, protein FROM lab WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     lab_table = c.fetchall()
        #
        #     if len(str(row[0])) < 3:
        #         continue
        #
        #     if len(lab_table) > 1:
        #         print(('WARNING: ' + str(row[0]) + ' has multiple entries in the lab table'))
        #         # print lab_table
        #
        #     for entry in lab_table:
        #         if len(str(entry[1])) < 2 or 'None' in str(entry[1]):
        #             protein_name = str(row[0]).split('-')[0]
        #         else:
        #             protein_name = str(entry[1])
        #
        #
        #         crystal_data_dump_dict['protein'].append(protein_name)
        #         crystal_data_dump_dict['smiles'].append(entry[0])
        #         crystal_data_dump_dict['crystal_name'].append(row[0])
        #         crystal_data_dump_dict['bound_conf'].append(row[1])
        #         crystal_data_dump_dict['strucid'].append('')
        #
        #         try:
        #             modification_date = misc_functions.get_mod_date(str(row[1]))
        #
        #         except:
        #             modification_date = ''
        #
        #         crystal_data_dump_dict['modification_date'].append(modification_date)
        #
        #     c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     pandda_info = c.fetchall()
        #
        #     for pandda_entry in pandda_info:
        #         project_data_dump_dict['protein'].append(protein_name)
        #         project_data_dump_dict['pandda_path'].append(pandda_entry[0])
        #         project_data_dump_dict['reference_pdb'].append(pandda_entry[1])
        #         project_data_dump_dict['strucid'].append('')
        #
        # project_table = pandas.DataFrame.from_dict(project_data_dump_dict)
        # crystal_table = pandas.DataFrame.from_dict(crystal_data_dump_dict)
        #
        # protein_list = set(list(project_data_dump_dict['protein']))
        # print(protein_list)
        #
        # for protein in protein_list:
        #
        #     self.add_to_postgres(project_table, protein, ['reference_pdb'], project_data_dump_dict, 'proasis_leads')
        #
        #     self.add_to_postgres(crystal_table, protein, ['crystal_name', 'smiles', 'bound_conf'],
        #                          crystal_data_dump_dict, 'proasis_hits')
        #
        # with self.output().open('wb') as f:
        #     f.write('')


class FindPanddaLogs(luigi.Task):
    search_path = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return StartTransfers(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/pandda/pandda_logs_%Y%m%d.txt'))

    def run(self):
        print('RUNNING')
        log_files = pandda_functions.find_log_files(self.search_path)
        with self.output().open('w') as f:
            f.write(log_files)


class AddPanddaSites(luigi.Task):
    file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()
    def requires(self):
        return AddPanddaRun(file=self.file, output_dir=self.output_dir, input_dir=self.input_dir, pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file)

    def output(self):
        pass

    def run(self):
        run = PanddaRun.objects.get(pandda_log=self.file)

class AddPanddaEvents(luigi.Task):
    file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()

    def requires(self):
        return AddPanddaRun(file=self.file, output_dir=self.output_dir, input_dir=self.input_dir, pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file)

    def output(self):
        pass

    def run(self):
        pass


class AddPanddaRun(luigi.Task):
    log_file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()

    def requires(self):
        pass

    def complete(self):
        if PanddaRun.objects.filter(pandda_log=self.log_file).exists():
            return True
        else:
            return False

    def run(self):
        pandda_run = PanddaRun.objects.get_or_create(pandda_log=self.log_file)[0]
        print(pandda_run)
        pandda_run.pandda_dir = self.output_dir
        pandda_run.analysis_folder = PanddaAnalysis.get_or_create(pandda_dir=self.input_dir)[0]
        print(pandda_run.analysis_folder)
        pandda_run.pandda_version = self.pver
        pandda_run.sites_file = self.sites_file
        pandda_run.events_file = self.events_file
        pandda_run.save()


class AddPanddaTables(luigi.Task):
    search_path = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return FindPanddaLogs(search_path=self.search_path, soak_db_path=self.soak_db_filepath)

    def output(self):
        pass

    def run(self):
        # read the list of log files
        with self.input().open('r') as f:
            log_files = f.readlines().split()

        # dict to hold info for subsequent things
        results_dict = {'log_file': [], 'pver': [], 'input_dir': [], 'output_dir': [], 'sites_file': [],
                        'events_file': [], 'err': []}

        for file in log_files:

            results_dict['log_file'].append(file)
            # read information from the log file
            pver, input_dir, output_dir, sites_file, events_file, err = pandda_functions.get_files_from_log(file)
            if not err:
                yield AddPanddaRun(file=file, pver=pver, input_dir=input_dir, output_dir=output_dir,
                                   sites_file=sites_file, events_file=events_file)




