import luigi
import os
import re
import setup_django
import subprocess
import datetime
import shutil
import glob
from functions import db_functions, misc_functions, pandda_functions
from xchem_db.models import *
import pandas as pd
import traceback
from functions import misc_functions
from django.db import transaction
from functions import db_functions
from functions import proasis_api_funcs
from . import transfer_proasis


def transfer_file(data_file):
    maint_exists = db_functions.check_table_sqlite(data_file, 'mainTable')
    if maint_exists == 1:
        db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=data_file,
                                    model=Crystal)
        db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=data_file,
                                    model=Lab)
        db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=data_file,
                                    model=Refinement)
        db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=data_file,
                                    model=Dimple)
        db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                    filename=data_file, model=DataProcessing)

    soakdb_query = SoakdbFiles.objects.get(filename=data_file)
    soakdb_query.status = 2
    soakdb_query.save()


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
                print('LEN=0')
                out, err, prop = db_functions.pop_soakdb(filename_clean)
                db_functions.pop_proposals(prop)

            # only one entry should exist per file
            if len(soakdb_query) == 1:
                print('LEN=1')
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

            # else:
            #     update_status = SoakdbFiles.objects.select_for_update().get(id=id_number)
            #     update_status.status = 0
            #     update_status.save()

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
        return luigi.LocalTarget(self.date.strftime('logs/transfer_logs/fedids_%Y%m%d%H.txt'))

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
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return CheckFiles(soak_db_filepath=self.data_file)


    def output(self):
        modification_date = misc_functions.get_mod_date(self.data_file)
        return luigi.LocalTarget(str(self.data_file + '_' + str(modification_date) + '.transferred'))

    def run(self):
        # delete all fields from soakdb filename
        maint_exists = db_functions.check_table_sqlite(self.data_file, 'mainTable')
        if maint_exists == 1:
            soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)

            crystals = Crystal.objects.filter(visit=soakdb_query)

            for crystal in crystals:
                refinement = Refinement.objects.get(crystal_name=crystal)

                target_name = str(crystal.target.target_name).upper()
                crystal_name = str(crystal.crystal_name)
                proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name)

                if ProasisHits.objects.filter(crystal_name=crystal).exists():
                    proasis_hit = ProasisHits.objects.filter(crystal_name=crystal)
                    for hit in proasis_hit:
                        for path in glob.glob(os.path.join(os.getcwd(), 'logs/proasis/hits',
                                                           str(hit.crystal_name.crystal_name +
                                                               '_' + hit.modification_date + '*'))):
                            os.remove(path)
                        if os.path.isdir(proasis_crystal_directory):
                            shutil.rmtree(os.path.join(proasis_crystal_directory))

                        if ProasisOut.objects.filter(proasis=hit).exists:
                            for obj in ProasisOut.objects.filter(proasis=hit):
                                obj.delete()
                        hit.delete()

            soakdb_query.delete()

            out, err, proposal = db_functions.pop_soakdb(self.data_file)
            db_functions.pop_proposals(proposal)

        transfer_file(self.data_file)

        with self.output().open('w') as f:
            f.write('')


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

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
    date = luigi.DateParameter(default=datetime.date.today())
    # date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def get_file_list(self, status_code):

        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [object.filename for object in status_query]

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
        return luigi.LocalTarget('logs/transfer_logs/transfers_' + str(self.date.strftime("%Y%m%d%H")) + '.done')

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CheckFileUpload(luigi.Task):
    filename = luigi.Parameter()
    model = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        mod_date = misc_functions.get_mod_date(self.filename)
        return luigi.LocalTarget(str(self.filename + '.' + mod_date + '.checked'))

    def run(self):
        out_err_file = str('logs/' + str(self.filename.split('/')[3]) + '_' + str(self.filename.split('/')[4]) +
                           '_' + str(self.filename.split('/')[5]) + '_' +
                           str(misc_functions.get_mod_date(self.filename)) +
                           str(self.model).replace("<class '", '').replace("'>", '') + '.txt')

        print(out_err_file)

        results = db_functions.soakdb_query(self.filename)

        try:
            print('Number of rows from file = ' + str(len(results)))

            if len(Crystal.objects.filter(visit__filename=self.filename)) == len(results):
                status = True
            else:
                status = False

            print('Checking same number of rows in test_xchem: ' + str(status))
            if not status:
                raise Exception('FAIL: no of entries in test_xchem = ' + str(
                    len(Crystal.objects.filter(visit__filename=self.filename))))

            proteins = list(set([protein for protein in [protein['ProteinName'] for protein in results]]))

            print('Unique targets in soakdb file: ' + str(proteins))

            translations = {Lab: db_functions.lab_translations(),
                            Refinement: db_functions.refinement_translations(),
                            DataProcessing: db_functions.data_processing_translations(),
                            Dimple: db_functions.dimple_translations()}

            translation = translations[self.model]

            error_dict = {
                'crystal': [],
                'soakdb_field': [],
                'model_field': [],
                'soakdb_value': [],
                'model_value': []

            }
            for row in results:
                lab_object = self.model.objects.filter(crystal_name__crystal_name=row['CrystalName'],
                                                       crystal_name__visit__filename=str(self.filename),
                                                       crystal_name__compound__smiles=row['CompoundSMILES'])
                if len(lab_object)>1:
                    raise Exception('Multiple Crystals!')
                if len(lab_object)==0:
                    if self.model==Dimple and not row['DimplePathToPDB'] and not row['DimplePathToMTZ']:
                        pass
                    else:
                        raise Exception('No entry for ' + str(row['CrystalName'] + ' ' + row['DimplePathToPDB'] + ' '
                                                              + row['DimplePathToMTZ']))
                for key in translation.keys():
                    test_xchem_val = eval(str('lab_object[0].' + key))
                    soakdb_val = row[translation[key]]
                    if key == 'outcome':
                        pattern = re.compile('-?\d+')
                        try:
                            soakdb_val = int(pattern.findall(str(soakdb_val))[0])
                        except:
                            continue
                    if translation[key] == 'CrystalName':
                        test_xchem_val = lab_object[0].crystal_name.crystal_name
                    if translation[key] == 'DimpleReferencePDB' and soakdb_val:
                        test_xchem_val = lab_object[0].reference
                        if test_xchem_val != None:
                            test_xchem_val = lab_object[0].reference.reference_pdb
                    if soakdb_val == '' or soakdb_val == 'None' or not soakdb_val:
                        continue
                    if isinstance(test_xchem_val, float):
                        if float(test_xchem_val) == float(soakdb_val):
                            continue
                    if isinstance(test_xchem_val, int):
                        if int(soakdb_val) == int(test_xchem_val):
                            continue
                    if test_xchem_val != soakdb_val:
                        if soakdb_val in [None, 'None', '', '-', 'n/a', 'null', 'pending', 'NULL', '#NAME?', '#NOM?',
                                          'None\t',
                                          'Analysis Pending', 'in-situ']:
                            continue
                        else:
                            # try:
                            #     error_dict['crystal'].append(eval(str('lab_object[0].' + key + '.crystal_name')))
                            # except:
                            #     if key=='crystal_name':
                            #         error_dict['crystal'].append(eval(str('lab_object[0].' + key)))
                            #     else:
                            #         raise Exception(key)
                            error_dict['crystal'].append(str(lab_object[0].crystal_name.crystal_name))
                            error_dict['soakdb_field'].append(translation[key])
                            error_dict['model_field'].append(key)
                            error_dict['soakdb_value'].append(soakdb_val)
                            error_dict['model_value'].append(test_xchem_val)

            if error_dict['crystal']:
                pd.DataFrame.from_dict(error_dict).to_csv(out_err_file)

        except IndexError:
            if 'No item with that key' in traceback.format_exc():
                pass
            else:
                with open(out_err_file, 'w') as f:
                    f.write(traceback.format_exc())
                with open(out_err_file, 'a') as f:
                    f.write('\n' + str(key))
        except AttributeError:
            with open(out_err_file, 'w') as f:
                f.write(traceback.format_exc())
            with open(out_err_file, 'a') as f:
                f.write('\n' + str(lab_object))
        except:
            with open(out_err_file, 'w') as f:
                f.write(traceback.format_exc())

        with self.output().open('w') as f:
            f.write('')

class CheckUploadedFiles(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        if not os.path.isfile(StartTransfers(date=self.date, soak_db_filepath=self.soak_db_filepath).output().path):
            return StartTransfers(date=self.date, soak_db_filepath=self.soak_db_filepath)
        else:
            soakdb_files = [obj.filename for obj in SoakdbFiles.objects.all()]
            models = [Lab, Dimple, DataProcessing, Refinement]
            zipped = []
            for filename in soakdb_files:
                for model in models:
                    maint_exists = db_functions.check_table_sqlite(filename, 'mainTable')
                    if maint_exists == 1:
                        zipped.append(tuple([filename, model]))

            return [CheckFileUpload(filename=filename, model=model) for (filename, model) in zipped]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/soakDBfiles/soakDB_checked_%Y%m%d.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')

