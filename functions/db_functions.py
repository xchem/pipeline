import csv
import os
import sqlite3
import subprocess
import re
import setup_django
from xchem_db import models
from functions import misc_functions
from django.db import IntegrityError
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
import numpy
import pandas as pd


# To get all sql queries sent by Django from py shell
# import logging
# l = logging.getLogger('django.db.backends')
# l.setLevel(logging.DEBUG)
# l.addHandler(logging.StreamHandler())

def reference_translations():
    reference = {
        'reference':'DimpleReferencePDB'
    }

def lab_translations():
    lab = {
        'cryo_frac': 'CryoFraction',
        'cryo_status': 'CryoStatus',
        'cryo_stock_frac': 'CryoStockFraction',
        'cryo_transfer_vol': 'CryoTransferVolume',
        'crystal_name': 'CrystalName',
        'data_collection_visit': 'DataCollectionVisit',
        'expr_conc': 'CompoundConcentration',
        'harvest_status': 'HarvestStatus',
        'library_name': 'LibraryName',
        'library_plate': 'LibraryPlate',
        'mounting_result': 'MountingResult',
        'mounting_time': 'MountingTime',
        'soak_status': 'SoakStatus',
        'soak_time': 'SoakingTime',
        'soak_vol': 'SoakTransferVol',
        'solv_frac': 'SolventFraction',
        'stock_conc': 'CompoundStockConcentration',
        'visit': 'LabVisit'
    }

    return lab


def crystal_translations():
    crystal = {
        'crystal_name': 'CrystalName',
        'target': 'ProteinName',
        'compound': 'CompoundSMILES',
        'visit': '',
    }

    return crystal


def data_processing_translations():
    data_processing = {
        'image_path': 'DataProcessingPathToImageFiles',
        'program': 'DataProcessingProgram',
        'spacegroup': 'DataProcessingSpaceGroup',
        'unit_cell': 'DataProcessingUnitCell',
        'auto_assigned': 'DataProcessingAutoAssigned',
        'res_overall': 'DataProcessingResolutionOverall',
        'res_low': 'DataProcessingResolutionLow',
        'res_low_inner_shell': 'DataProcessingResolutionLowInnerShell',
        'res_high': 'DataProcessingResolutionHigh',
        'res_high_15_sigma': 'DataProcessingResolutionHigh15sigma',
        'res_high_outer_shell': 'DataProcessingResolutionHighOuterShell',
        'r_merge_overall': 'DataProcessingRmergeOverall',
        'r_merge_low': 'DataProcessingRmergeLow',
        'r_merge_high': 'DataProcessingRmergeHigh',
        'isig_overall': 'DataProcessingIsigOverall',
        'isig_low': 'DataProcessingIsigLow',
        'isig_high': 'DataProcessingIsigHigh',
        'completeness_overall': 'DataProcessingCompletenessOverall',
        'completeness_low': 'DataProcessingCompletenessLow',
        'completeness_high': 'DataProcessingCompletenessHigh',
        'multiplicity_overall': 'DataProcessingMultiplicityOverall',
        'multiplicity_low': 'DataProcessingMultiplicityLow',
        'multiplicity_high': 'DataProcessingMultiplicityHigh',
        'cchalf_overall': 'DataProcessingCChalfOverall',
        'cchalf_low': 'DataProcessingCChalfLow',
        'cchalf_high': 'DataProcessingCChalfHigh',
        'logfile_path': 'DataProcessingPathToLogfile',
        'mtz_path': 'DataProcessingPathToMTZfile',
        'log_name': 'DataProcessingLOGfileName',
        'mtz_name': 'DataProcessingMTZfileName',
        'original_directory': 'DataProcessingDirectoryOriginal',
        'unique_ref_overall': 'DataProcessingUniqueReflectionsOverall',
        'lattice': 'DataProcessingLattice',
        'point_group': 'DataProcessingPointGroup',
        'unit_cell_vol': 'DataProcessingUnitCellVolume',
        'score': 'DataProcessingScore',
        'status': 'DataProcessingStatus',
        'r_cryst': 'DataProcessingRcryst',
        'r_free': 'DataProcessingRfree',
        'dimple_pdb_path': 'DataProcessingPathToDimplePDBfile',
        'dimple_mtz_path': 'DataProcessingPathToDimpleMTZfile',
        'dimple_status': 'DataProcessingDimpleSuccessful',
        'crystal_name': 'CrystalName'
    }

    return data_processing


def dimple_translations():
    dimple = {
        'res_high': 'DimpleResolutionHigh',
        'r_free': 'DimpleRfree',
        'pdb_path': 'DimplePathToPDB',
        'mtz_path': 'DimplePathToMTZ',
        # 'reference_pdb': 'DimpleReferencePDB',
        'status': 'DimpleStatus',
        # 'pandda_run': 'DimplePANDDAwasRun',
        # 'pandda_hit': 'DimplePANDDAhit',
        # 'pandda_reject': 'DimplePANDDAreject',
        # 'pandda_path': 'DimplePANDDApath',
        'crystal_name': 'CrystalName',
        'reference': 'DimpleReferencePDB'
    }

    return dimple


def refinement_translations():
    refinement = {
        'res': 'RefinementResolution',
        'rcryst': 'RefinementRcryst',
        'r_free': 'RefinementRfree',
        'spacegroup': 'RefinementSpaceGroup',
        'lig_cc': 'RefinementLigandCC',
        'rmsd_bonds': 'RefinementRmsdBonds',
        'rmsd_angles': 'RefinementRmsdAngles',
        'outcome': 'RefinementOutcome',
        'mtz_free': 'RefinementMTZfree',
        'cif': 'RefinementCIF',
        'cif_status': 'RefinementCIFStatus',
        'cif_prog': 'RefinementCIFprogram',
        'pdb_latest': 'RefinementPDB_latest',
        'mtz_latest': 'RefinementMTZ_latest',
        'matrix_weight': 'RefinementMatrixWeight',
        'refinement_path': 'RefinementPathToRefinementFolder',
        'lig_confidence': 'RefinementLigandConfidence',
        'lig_bound_conf': 'RefinementLigandBoundConformation',
        'bound_conf': 'RefinementBoundConformation',
        'molprobity_score': 'RefinementMolProbityScore',
        'ramachandran_outliers': 'RefinementRamachandranOutliers',
        'ramachandran_favoured': 'RefinementRamachandranFavored',
        'status': 'RefinementStatus',
        'crystal_name': 'CrystalName'
    }

    return refinement

def distinct_crystals_sqlite(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # items that would be unique crystal entries
    c.execute("select CrystalName from mainTable where CrystalName NOT LIKE ? and CrystalName NOT LIKE ? and CrystalName !='' and CrystalName IS NOT NULL "
              "and CompoundSMILES not like ? and CompoundSMILES NOT LIKE ? and CompoundSMILES IS NOT NULL  and CompoundSMILES !='' "
              "and ProteinName not like ? and ProteinName NOT LIKE ? and ProteinName not NULL and ProteinName !=''", ('None', 'null','None', 'null', 'None', 'null'))

    results = c.fetchall()
    conn.close()
    crystal_names = [row['CrystalName'] for row in results]

    seen = {}
    dupes = []

    for x in crystal_names:
        if x not in seen:
            seen[x] = 1
        else:
            if seen[x] == 1:
                dupes.append(x)
            seen[x] += 1

    return dupes


def specific_crystal(filename, crystal):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("select * from mainTable where CrystalName = ?", (crystal,))

    results = c.fetchall()
    return results


def test_duplicate_method(filename):
    duplicates = distinct_crystals_sqlite(filename)
    all_results = [specific_crystal(filename, crystal) for crystal in duplicates]
    for results in all_results:
        keys = [row.keys() for row in results]
        if len(set(tuple(key_list) for key_list in keys)) == 1:
            key_list = keys[0]
            holder = []
            for result in results:
                tmp = []
                for key in key_list:
                    tmp.append(result[key])
                holder.append(tmp)
            unique = len(set(tuple(lst) for lst in holder))

            print(unique)
            print(len(results))

            if unique == len(results):
                timestamps=[]
                for result in results:
                    time = (int(result['LastUpdated'].replace('-','').replace(':','').replace(' ', '')))
                    timestamps.append(time)
                if len(list(set(timestamps))) > 1:
                    ind = numpy.argmax(timestamps)


def check_db_duplicates(filename):
    pop_soakdb(filename)
    duplicates_file = 'duplicates.csv'
    if os.path.isfile(os.path.join(os.getcwd(), duplicates_file)):
        duplicates_dict = pd.DataFrame.from_csv(duplicates_file).to_dict(orient='list')
    else:
        duplicates_dict = {'crystal': [],
                           'file_1': [],
                           'file_2': [],
                           'smiles': [],
                           'target': []}

    table = check_table_sqlite(filename, 'mainTable')
    if table == 0:
        return None

    # standard soakdb query for all data
    results = soakdb_query(filename)

    # check for existing crystal entries
    lst = [(row['CrystalName'], row['CompoundSMILES'], row['ProteinName']) for row in results]
    for tup in lst:
        vals = list(tup)
        obj, was_created = models.Crystal.objects.get_or_create(crystal_name=vals[0],
                                                                compound=models.Compounds.objects.get_or_create(
                                                                    smiles=vals[1])[0],
                                                                target=models.Target.objects.get_or_create(
                                                                    target_name=vals[2])[0],
                                                                visit=models.SoakdbFiles.objects.get_or_create(
                                                                    filename=filename)[0]
                                                                )
        if not was_created:
            if str(filename)==str(obj.visit.filename):
                continue
            print(duplicates_dict)
            duplicates_dict['crystal'].append(vals[0])
            duplicates_dict['smiles'].append(vals[1])
            duplicates_dict['target'].append(vals[2])
            duplicates_dict['file_1'].append(filename)
            duplicates_dict['file_2'].append(obj.visit.filename)

    pd.DataFrame.from_dict(duplicates_dict).to_csv(duplicates_file)



# @transaction.atomic
def transfer_table(translate_dict, filename, model):

    # standard soakdb query for all data
    results = soakdb_query(filename)

    # for each row found in soakdb
    for row in results:
        compound_smiles = row['CompoundSMILES']
        crystal_name = row['CrystalName']
        target = row['ProteinName']

        # set up blank dictionary to hold model values
        d = {}
        # get the keys and values of the query
        row_keys = row.keys()
        row_values = list(tuple(row))

        # swap the keys over for lookup, and give any missing keys a none value to skip them
        for i, x in enumerate(row_keys):
            if x in dict((v, k) for k, v in translate_dict.items()).keys():
                key = dict((v, k) for k, v in translate_dict.items())[x]

                if key not in d.keys():
                    d[key] = ''
                d[key] = row_values[i]

        # get the fields that must exist in the model (i.e. table)
        model_fields = [f.name for f in model._meta.local_fields]

        disallowed_floats = [None, 'None', '', '-', 'n/a', 'null', 'pending', 'NULL', '#NAME?', '#NOM?', 'None\t',
                             'Analysis Pending', 'in-situ']

        d = {k: v for k, v in d.items() if v not in disallowed_floats}

        if model != models.Reference and 'crystal_name' not in d.keys():
            continue

        if model == models.Crystal and 'target' not in d.keys():
            continue

        # check that file_id's can be written
        for key in model_fields:
            if key == 'visit' and model == models.Crystal:
                try:
                    d[key] = models.SoakdbFiles.objects.get(filename=filename)
                except ObjectDoesNotExist:
                    _, _, proposal = pop_soakdb(filename)
                    pop_proposals(proposal)
                    d[key] = models.SoakdbFiles.objects.get(filename=filename)

        for key in d.keys():

            # raise an exception if a rogue key is found - means translate_dict or model is wrong
            if key not in model_fields:
                raise Exception(str('KEY: ' + key + ' FROM MODELS not in ' + str(model_fields)))

            # find relevant entries for foreign keys and set as value - crystal names and proteins

            if key == 'crystal_name' and model != models.Crystal:
                d[key] = models.Crystal.objects.get(crystal_name=d[key], visit=models.SoakdbFiles.objects.get(
                    filename=filename), compound=models.Compounds.objects.get_or_create(smiles=compound_smiles)[0])

            if key == 'target':
                d[key] = models.Target.objects.get_or_create(target_name=d[key])[0]

            if key == 'compound':
                d[key] = models.Compounds.objects.get_or_create(smiles=d[key])[0]

            if key == 'reference':
                if d[key]:
                    d[key] = models.Reference.objects.get_or_create(reference_pdb=d[key])[0]

            if key == 'outcome':
                pattern = re.compile('-?\d+')
                value = pattern.findall(str(d[key]))
                if len(value) > 1:
                    raise Exception('multiple values found in outcome string')
                try:
                    d[key] = int(value[0])
                except:
                    continue

        # write out the row to the relevant model (table)
        try:
            with transaction.atomic():
                m = model.objects.create(**d)
                m.save

        except IntegrityError as e:
            print(d)
            print('WARNING: ' + str(e.__cause__))
            print(model_fields)
            crys_from_db = models.Crystal.objects.get(crystal_name=crystal_name, visit=models.SoakdbFiles.objects.get(
                filename=filename), compound=models.Compounds.objects.get_or_create(smiles=compound_smiles)[0])
            if crys_from_db.target == target:
                print('Crystal duplicated!')
                continue
        # uncomment to debug
        # except ValueError as e:
        #     print(d)
        #     print('WARNING: ' + str(e.__cause__))
        #     print(e)
        #     print(model_fields)
        #     continue


def soakdb_query(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("select distinct * from mainTable where CrystalName NOT LIKE ? and CrystalName NOT LIKE ? and CrystalName !='' and CrystalName IS NOT NULL "
              "and CompoundSMILES not like ? and CompoundSMILES NOT LIKE ? and CompoundSMILES IS NOT NULL  and CompoundSMILES !='' "
              "and ProteinName not like ? and ProteinName NOT LIKE ? and ProteinName not NULL and ProteinName !=''", ('None', 'null','None', 'null', 'None', 'null'))

    results = c.fetchall()
    conn.close()
    return results


def check_table_sqlite(filename, tablename):
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", (tablename,))
    results = c.fetchall()[0][0]
    conn.close()

    return results

@transaction.atomic
def pop_soakdb(database_file):
    # get proposal number from dls path
    print(database_file)
    try:
        visit = database_file.split('/')[5]
        proposal = visit.split('-')[0]
    except:
        proposal = 'lb13385'
        print('WARNING: USING DEFAULT PROPOSAL FOR TESTS')
    # get allowed users
    proc = subprocess.Popen(str('getent group ' + str(proposal)), stdout=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    # get modification date of file
    modification_date = misc_functions.get_mod_date(database_file)
    # add info to soakdbfiles table
    soakdb_entry = models.SoakdbFiles.objects.get_or_create(modification_date=modification_date, filename=database_file,
                                                            proposal=models.Proposals.objects.get_or_create(
                                                                proposal=proposal)[0], visit=visit)[0]
    soakdb_entry.save()
    return out, err, proposal

@transaction.atomic
def pop_proposals(proposal_number):
    # get proposal number from shell
    proc = subprocess.Popen(str('getent group ' + str(proposal_number)), stdout=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    # create list of fedids (or blank if none found)
    if len(out.decode('ascii')) == 0:
        append_list = ''
    else:
        append_list = out.decode('ascii').split(':')[3].replace('\n', '')
    # add proposal to proposals table with allowed fedids
    proposal_entry = models.Proposals.objects.select_for_update().get_or_create(proposal=proposal_number)[0]
    proposal_entry.fedids = str(append_list)
    proposal_entry.save()


def check_file_status(filename, bound_pdb):

    pdb_file_name = str(bound_pdb).split('/')[-1]

    if 'Refine' in str(bound_pdb).replace(pdb_file_name, ''):
        remove_string = str(str(bound_pdb).split('/')[-2] + '/' + pdb_file_name)
        map_directory = str(bound_pdb).replace(remove_string, '')
    else:
        map_directory = str(bound_pdb).replace(pdb_file_name, '')

    if os.path.isfile(str(map_directory + filename)):
        return True, str(map_directory + filename)
    else:
        return False, ''


def query_and_list(query, proposals_list, proposal_dict, strucid_list):
    conn, c = connectDB()
    c.execute(query)
    rows = c.fetchall()
    for row in rows:
        try:
            proposal = str(row[0]).split('/')[5].split('-')[0]
        except:
            continue
        if proposal not in proposals_list:
            proposals_list.append(proposal)
            proposal_dict.update({proposal: []})
        else:
            proposal_dict[proposal].append(str(row[1]))

        strucid_list.append(str(row[1]))

    conn.close()

    return proposals_list, proposal_dict, strucid_list

def get_strucid_list():

    proposals_list = []
    strucid_list = []
    proposal_dict = {}

    proposals_list, proposal_dict, strucid_list = query_and_list('SELECT bound_conf, strucid FROM proasis_hits',
                                                                 proposals_list, proposal_dict, strucid_list)

    proposals_list, proposal_dict, strucid_list = query_and_list('SELECT reference_pdb, strucid FROM proasis_leads',
                                                                 proposals_list, proposal_dict, strucid_list)

    strucids = list(set(strucid_list))

    return proposal_dict, strucids


def get_fedid_list():
    master_list = []
    conn, c = connectDB()
    c.execute('select fedids from proposals')
    rows = c.fetchall()
    for row in rows:
        if len(row[0]) > 1:
            fedid_list = str(row[0]).split(',')
            for item in fedid_list:
                master_list.append(item)

    final_list = list(set(master_list))
    conn.close()
    return final_list


def create_blacklist(fedid, proposal_dict, dir_path):
    search_string=str('%' + fedid + '%')
    proposal_list = []
    all_proposals = list(set(list(proposal_dict.keys())))
    strucid_list = []
    conn, c = connectDB()
    c.execute('select proposal from proposals where fedids like %s', (search_string,))
    rows = c.fetchall()
    for row in rows:
        proposal_list.append(str(row[0]))
    for proposal in proposal_list:
        if proposal in all_proposals:
            all_proposals.remove(proposal)
    for proposal in all_proposals:
        try:
            temp_vals = proposal_dict[proposal]
        except:
            continue
        for item in temp_vals:
            strucid_list.append(item)

    blacklist_file = str(dir_path + '/' + fedid + '.dat')
    with open(blacklist_file, 'wb') as writefile:
        wr = csv.writer(writefile)
        wr.writerow(strucid_list)
#
# def get_pandda_ligand(datafile, pandda_path, crystal):
#     conn = sqlite3.connect(datafile)
#     c = conn.cursor()
#     lig_list = []
#     for row in c.execute('''select PANDDA_site_ligand_resname, PANDDA_site_ligand_chain, PANDDA_site_ligand_sequence_number
#                   from panddaTable where PANDDAPath like ? and CrystalName like ?''', (pandda_path, crystal)):
#         resname = str(row[0])
#         chain = str(row[1])
#         sequence = str(row[2])
#         lig_list.append([resname, chain, sequence])
#     return lig_list
#
# def get_pandda_lig_list(bound_conf):
#     conn, c = connectDB()
#     c.execute('select file_id from refinement where bound_conf like %s', (bound_conf,))
#     file_id = str(c.fetchall()[0][0])
#     c.execute('select filename from soakdb_files where id = %s', (file_id,))
#     datafile = str(c.fetchall()[0][0])
#     c.execute('select crystal_name from proasis_hits where bound_conf like %s', (bound_conf,))
#     crystal = str(c.fetchall()[0][0])
#     c.execute('select pandda_path from dimple where file_id = %s and crystal_name like %s', (file_id, crystal))
#     pandda_directory = str(c.fetchall()[0][0])
#
#     lig_list = get_pandda_ligand(datafile,pandda_directory,crystal)
#
#     return lig_list
#

