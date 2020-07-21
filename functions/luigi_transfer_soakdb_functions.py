import glob
import shutil
import subprocess #?
import traceback

from functions.pandda_functions import find_log_files

from functions import db_functions, misc_functions
from luigi_classes.config_classes import DirectoriesConfig
from xchem_db.models import *
from dateutil.parser import parse

# Moving defined functions in transfer_soakdb.py to avoid cross-imports...
def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False

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

# Transfer_soakdb.py functions
def find_soak_db_files(filepath):
    command = str(
        '''find ''' + filepath + ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/tmp/*" -prune -o -path "*BACKUP*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*ack*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print'''
    )
    process = subprocess.Popen(args=command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print(command)
    out, err = process.communicate()
    out = out.decode('ascii')
    print('OUTPUT:')
    print(out)

    return str(out)


def check_files(soak_db_filepath):
    # Beginning of run(self)
    checked = []
    # Status codes:-
    # 0 = new
    # 1 = changed
    # 2 = not changed

    # self.input()[1].path = soak_db_filepath?
    print(f'INPUT NAME: {soak_db_filepath}')

    # Open file
    with open(soak_db_filepath, 'r') as f:
        files = f.readlines()
        print(f'FILES: {files}')

    for filename in files:
        filename_clean = filename.rstrip('\n')
        soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))
        print(len(soakdb_query))

        # Consider Switch instead of IFs?
        if len(soakdb_query) == 0:
            print('LEN=0')
            out, err, prop = db_functions.pop_soakdb(filename_clean)
            db_functions.pop_proposals(prop)

        if len(soakdb_query) == 1:
            print('LEN=1')
            # Get filename from query
            data_file = soakdb_query[0].filename
            # add file to list which have been checked
            checked.append(data_file)
            # Get last modification date as stored in soakdb
            old_mod_date = soakdb_query[0].modification_date
            # Get current modification date of file
            current_mod_date = misc_functions.get_mod_date(data_file)
            # get the id of entry to write to
            id_number = soakdb_query[0].id

            print(old_mod_date)
            if not old_mod_date:
                soakdb_query[0].modification_date = current_mod_date
                soakdb_query[0].save()
                old_mod_date = 0

            print(current_mod_date)

            # if the file has changed since the db was last updated for the entry, change status to indicate this
            try:
                if int(current_mod_date) > int(old_mod_date):
                    update_status = SoakdbFiles.objects.get(id=id_number)
                    update_status.status = 1
                    update_status.save()
            except ValueError:
                raise Exception(f"current_mod_date: {current_mod_date}, old_mod_date: {old_mod_date}")

        if len(soakdb_query) > 1:
            raise Exception('More than one entry for file! Something has gone wrong!')

        # If file isn't in XCDB
        if filename_clean not in checked:
            # Add to soakdb
            out, err, proposal = db_functions.pop_soakdb(filename_clean)
            db_functions.pop_proposals(proposal)
            soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))
            id_number = soakdb_query[0].id
            update_status = SoakdbFiles.objects.get(id=id_number)
            update_status.status=0
            update_status.save()

    lab = list(Lab.objects.all())
    if not lab:
        # Set all file statuses to 0
        soak_db = SoakdbFiles.objects.all()
        for filename in soak_db:
            filename.status = 0
            filename.save()

    return str('')


def transfer_all_fed_ids_and_datafiles(soak_db_filepath):
    with soak_db_filepath.open('r') as database_list:
        for database_file in database_list.readlines():
            database_file = database_file.replace('\n', '')

            out, err, proposal = db_functions.pop_soakdb(database_file)
            print(out)
            print(err)
            print(proposal)

    proposal_list = list(SoakdbFiles.objects.values_list('proposal', flat=True))

    for proposal_number in set(proposal_list):
        db_functions.pop_proposals(proposal_number)

    return 'DONE'


def transfer_changed_datafile(data_file, hit_directory):
    print(data_file)
    maint_exists = db_functions.check_table_sqlite(data_file, 'mainTable')

    if maint_exists == 1:
        soakdb_query = SoakdbFiles.objects.get(filename=data_file)
        print(soakdb_query)
        split_path = data_file.split('database')
        search_path = split_path[0]

        # remove pandda data transfer done file
        if os.path.isfile(os.path.join(search_path, 'transfer_pandda_data.done')):
            os.remove(os.path.join(search_path, 'transfer_pandda_data.done'))

        log_files = find_log_files(search_path).rsplit()
        print(log_files)
        for log in log_files:
            print(f"{log}.run.done")
            if os.path.isfile(f"{log}.run.done"):
                os.remove(f"{log}.run.done")
            if os.path.isfile(f"{log}.sites.done"):
                os.remove(f"{log}.sites.done")
            if os.path.isfile(f"{log}.events.done"):
                os.remove(f"{log}.events.done")

        #find_logs_out_files = glob.glob(str(search_path + '*.txt'))
        find_logs_out_files = glob.glob(f"{search_path}*.txt")

        for f in find_logs_out_files:
            if is_date(f.replace(search_path, '').replace('.txt', '')):
                os.remove(f)

        crystals = Crystal.objects.filter(visit=soakdb_query)

        for crystal in crystals:
            target_name = str(crystal.target.target_name).upper()
            crystal_name = str(crystal.crystal_name)
            # Do we even need this part if the proasis is being removed?
            proasis_crystal_directory = os.path.join(hit_directory, target_name.upper(), crystal_name)
            if ProasisHits.objects.filter(crystal_name=crystal).exists():
                proasis_hit = ProasisHits.objects.filter(crystal_name=crystal)
                for hit in proasis_hit:
                    for path in glob.glob(os.path.join(DirectoriesConfig().log_directory, 'proasis/hits',
                                                       str(hit.crystal_name.crystal_name +
                                                           '_' + hit.modification_date + '*'))):
                        os.remove(path)
                    if os.path.isdir(proasis_crystal_directory):
                        shutil.rmtree(os.path.join(proasis_crystal_directory), ignore_errors=True)

                    if ProasisOut.objects.filter(proasis=hit).exists:
                        for obj in ProasisOut.objects.filter(proasis=hit):
                            if obj.root:
                                delete_files = ['verne.transferred', 'PROPOSALS', 'VISITS', 'visits_proposals.done']
                                for f in delete_files:
                                    if os.path.isfile(os.path.join(obj.root, '/'.join(obj.start.split('/')[:-2]),
                                                                   f)):
                                        os.remove(os.path.join(obj.root, '/'.join(obj.start.split('/')[:-2]), f))
                                shutil.rmtree(os.path.join(obj.root, obj.start))
                            obj.delete()
                    hit.delete()

        soakdb_query.delete() # ?

        out,err,proposal = db_functions.pop_soakdb(data_file)
        db_functions.pop_proposals(proposal)

    else:
        print('Main Table does not exist!')

    transfer_file(data_file)

    return ''


# Calls transfer_file
#def transfer_new_datafile(luigi.Task):
#    return ''


#def start_transfers(luigi.Task):
#    return ''
#

def check_file_upload(filename, model):
    out_err_file = os.path.join(DirectoriesConfig().log_directory,
                                str(str(filename.split('/')[3]) +
                                    '_' + str(filename.split('/')[4]) +
                                    '_' + str(filename.split('/')[5]) + '_' +
                                    str(misc_functions.get_mod_date(filename)) +
                                    str(model).replace("<class '", '').replace("'>", '') + '.txt'))

    print(out_err_file)

    results = db_functions.soakdb_query(filename)

    try:
        print(f"Number of rows from file = {len(results)}")
        translations = {Lab: db_functions.lab_translations(),
                        Refinement: db_functions.refinement_translations(),
                        DataProcessing: db_functions.data_processing_translations(),
                        Dimple: db_functions.dimple_translations()}
        translation = translations[model]

        # different from what is in class...
        error_dict = dict(crystal=[], soakdb_field=[], model_field=[], soakdb_value=[], model_value=[])

        for row in results:
            lab_object = model.objects.filter(crystal_name__crystal_name=row['CrystalName'],
                                                crystal_name__visit__filename=str(filename),
                                                crystal_name__compound__smiles=row['CompoundSMILES'])
            if len(lab_object) > 1:
                raise Exception('Multiple Crystals!')
            if len(lab_object) == 0:
                if model == Dimple and not row row['DimplePathToPDB'] and not row['DimplePathToMTZ']:
                    pass
                else:
                    raise Exception(f"No entry for {row['CrystalName']}, {row['DimplePathToPDB']}, {row['DimplePathToMTZ']}")
            for key in translation.keys():
                test_xchem_val = eval(f"lab_objects[0].{key}")
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
                    if test_xchem_val is not None:
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

    return ''


#def check_uploaded_files(luigi.Task):
#    return ''
#

