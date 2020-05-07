import glob

from setup_django import setup_django

setup_django()

from functions import db_functions
from functions import misc_functions
from functions.pandda_functions import *
from xchem_db.models import *
from utils.refinement import RefinementObjectFiles

from dateutil.parser import parse
import argparse

from django.db import transaction

# functions called in steps
def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False

@transaction.atomic
def transfer_file(data_file):
    maint_exists = db_functions.check_table_sqlite(data_file, 'mainTable')
    if maint_exists == 1:
        print('transferring crystal data...')
        db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=data_file,
                                    model=Crystal)
        print('transferring lab data...')
        db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=data_file,
                                    model=Lab)
        print('transferring refinement data...')
        db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=data_file,
                                    model=Refinement)
        print('transferring dimple data...')
        db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=data_file,
                                    model=Dimple)
        print('transferring data processing data...')
        db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                    filename=data_file, model=DataProcessing)

    soakdb_query = SoakdbFiles.objects.get(filename=data_file)
    soakdb_query.status = 2
    soakdb_query.save()
# end of functions called in steps


@transaction.atomic
# step 1 - check the file to get it's status
def check_file(filename):
    status=2
    # remove any newline characters
    filename_clean = filename.rstrip('\n')
    # find the relevant entry in the soakdbfiles table

    soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))

    # raise an exception if the file is not in the soakdb table - not necessary here, I think
    # if len(soakdb_query) == 0:
    #     print('LEN=0')
    #     out, err, prop = db_functions.pop_soakdb(filename_clean)
    #     db_functions.pop_proposals(prop)

    # only one entry should exist per file
    if len(soakdb_query) == 1:
        # get the filename back from the query
        data_file = soakdb_query[0].filename
        # add the file to the list of those that have been checked
        # checked.append(data_file)
        # get the modification date as stored in the db
        old_mod_date = soakdb_query[0].modification_date
        # get the current modification date of the file
        current_mod_date = misc_functions.get_mod_date(data_file)
        # get the id of the entry to write to
        id_number = soakdb_query[0].id

        if not old_mod_date:
            soakdb_query[0].modification_date = current_mod_date
            soakdb_query[0].save()
            old_mod_date = 0

        # if the file has changed since the db was last updated for the entry, change status to indicate this
        try:
            if int(current_mod_date) > int(old_mod_date):
                update_status = SoakdbFiles.objects.get(id=id_number)
                update_status.status = 1
                status = 1
                update_status.save()
        except ValueError:
            raise Exception(str('current_mod_date: ' + str(current_mod_date)
                                + ', old_mod_date: ' + str(old_mod_date)))

        print(current_mod_date)
        print(old_mod_date)

    # if there is more than one entry, raise an exception (should never happen - filename field is unique)
    if len(soakdb_query) > 1:
        raise Exception('More than one entry for file! Something has gone wrong!')

    # if the file is not in the database at all
    if len(soakdb_query) == 0:
        print('This is a new soakDB file, just setting it up in the database!')
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
        status = 0

    # else:
    #     print('The file has not been updated, using existing XCDB data...')
    #     status = 2

    # if the lab table is empty, no data has been transferred from the datafiles, so set status of everything to 0


    lab = list(Lab.objects.all())
    if not lab:
        # this is to set all file statuses to 0 (new file)
        soakdb = SoakdbFiles.objects.all()
        for filename in soakdb:
            filename.status = 0
            filename.save()

    return status


@transaction.atomic
# Step 2 - identify whether the file is new or changed (from filename status in soakdb) and upload to db
def run_transfer(filename):
    status_query = SoakdbFiles.objects.get(filename=filename).status
    print('status query: ' + str(status_query) + '(0=new, 1=updated)')
    # if it is a changed file - do the delete things
    if status_query == 1:
        # maint_exists = db_functions.check_table_sqlite(filename, 'mainTable')
        #
        # if maint_exists == 1:
        soakdb_query = SoakdbFiles.objects.get(filename=filename)
        print(soakdb_query)
        # for pandda file finding
        split_path = filename.split('database')
        search_path = split_path[0]

        print('removing old logs...')
        # remove pandda data transfer done file
        if os.path.isfile(os.path.join(search_path, 'transfer_pandda_data.done')):
            os.remove(os.path.join(search_path, 'transfer_pandda_data.done'))

        log_files = find_log_files(search_path).rsplit()
        # print(log_files)

        for log in log_files:
            print(str(log + '.run.done'))
            if os.path.isfile(str(log + '.run.done')):
                os.remove(str(log + '.run.done'))
            if os.path.isfile(str(log + '.sites.done')):
                os.remove(str(log + '.sites.done'))
            if os.path.isfile(str(log + '.events.done')):
                os.remove(str(log + '.events.done'))

        find_logs_out_files = glob.glob(str(search_path + '*.txt'))

        for f in find_logs_out_files:
            if is_date(f.replace(search_path, '').replace('.txt', '')):
                os.remove(f)

        print('adding a new entry for file...')
        soakdb_query.delete()

    # the next step is always the same
    out, err, proposal = db_functions.pop_soakdb(filename)
    db_functions.pop_proposals(proposal)

    transfer_file(filename)

@transaction.atomic
# step 3 - create symlinks to bound-state pdbs
def create_links(filename, link_dir):
    to_search = Crystal.objects.filter(visit__filename=filename)
    crystals = Refinement.objects.filter(crystal_name__in=to_search, outcome__gte=4).filter(outcome__lte=6)
    for crystal in crystals:
        pth = os.path.join(link_dir,
                           crystal.crystal_name.target.target_name,
                           str(crystal.crystal_name.crystal_name + '.pdb'))

        smiles = crystal.crystal_name.compound.smiles
        prod_smiles = crystal.crystal_name.product

        try:
            if not os.path.exists(os.readlink(pth)):
                os.unlink(pth)
        except FileNotFoundError:
            pass
        if not os.path.isdir('/'.join(pth.split('/')[:-1])):
            os.makedirs('/'.join(pth.split('/')[:-1]))
        file_obj = RefinementObjectFiles(refinement_object=crystal)
        file_obj.find_bound_file()
        if file_obj.bound_conf:
            try:

                os.symlink(file_obj.bound_conf, pth)
                if prod_smiles:
                    smi = prod_smiles
                elif smiles:
                    smi = smiles
                #                 if smiles:
                smi_pth = pth.replace('.pdb', '_smiles.txt')
                with open(smi_pth, 'w') as f:
                    f.write(str(smi))
                f.close()

            except:
                raise Exception(file_obj.bound_conf)
        else:
            crystal.outcome = 3
            crystal.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--soakdb_file",
        default='',
        help="SoakDB file to be put into the database",
        required=True,
    )

    parser.add_argument(
        "-o",
        "--output_directory",
        default='',
        help="Output directory where bound-state pdb files will be",
        required=True,
    )

    args = vars(parser.parse_args())

    filename = args["soakdb_file"]
    link_dir = args["output_directory"]

    print('Will process: ' + filename)
    print('symlinks to the bound-state pdbs will be found in: ' + link_dir)

    print('Checking wether ' + filename + ' is a new or existing entry in XCDB...')
    status = check_file(filename)
    print(status)
    if status==0 or status==1:
        print('Transferring the data from the soakDB file into XCDB (this may take a while!)...')
        run_transfer(filename)
    print('Creating the symbolic links to the bound-state pdb files...')
    create_links(filename, link_dir)
    print('Telling the database this file is now up-to-date...')
    sdb = SoakdbFiles.objects.get(filename=filename)
    sdb.status = 2
    sdb.save()
    print('Done :)')
