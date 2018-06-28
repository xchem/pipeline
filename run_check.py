import setup_django
from db.models import *
from functions.db_functions import *

def check_table(model, results, translation):
    error_dict = {
        'crystal':[],
        'soakdb_field':[],
        'model_field':[],
        'soakdb_value':[],
        'model_value': []

    }
    for row in results:
        lab_object = model.objects.filter(crystal_name__crystal_name=row['CrystalName'])
        for key in translation.keys():
            test_xchem_val = eval(str('lab_object[0].' + key))
            soakdb_val = row[translation[key]]
            if translation[key] == 'CrystalName':
                test_xchem_val = eval(str('lab_object[0].' + key + '.crystal_name'))
            if translation[key] == 'DimpleReferencePDB' and soakdb_val:
                test_xchem_val = (eval(str('lab_object[0].' + key + '.reference_pdb')))
            if soakdb_val == '' or soakdb_val == 'None' or not soakdb_val:
                continue
            if isinstance(test_xchem_val, float):
                if float(test_xchem_val)==float(soakdb_val):
                    continue
            if isinstance(test_xchem_val, int):
                if int(soakdb_val)==int(test_xchem_val):
                    continue
            if test_xchem_val != soakdb_val:
                error_dict['crystal'].append(eval(str('lab_object[0].' + key + '.crystal_name')))
                error_dict['soakdb_field'].append(translation[key])
                error_dict['model_field'].append(key)
                error_dict['soakdb_value'].append(soakdb_val)
                error_dict['model_value'].append(test_xchem_val)

    return error_dict


database_file = '/dls/labxchem/data/2017/lb17884-1/processing/database/soakDBDataFile.sqlite'
print('Checking Database file ' + database_file)
print('Running soakdb_query...')
results = soakdb_query(database_file)
print('Number of rows from file = ' + str(len(results)))

if len(Crystal.objects.filter(file__filename=database_file)) == len(results):
    status = True
else:
    status = False

print('Checking same number of rows in test_xchem: ' + str(status))
if not status:
    print('FAIL: no of entries in test_xchem = ' + str(len(Crystal.objects.filter(file__filename=database_file))))

proteins = list(set([protein for protein in [protein['ProteinName'] for protein in results]]))

print('Unique targets in soakdb file: ' + str(proteins))


print('Checking Lab table...')
lab_errors = check_table(Lab, results, lab_translations())
print(lab_errors)

print('Checking Dimple table...')
dimple_errors = check_table(Dimple, results, dimple_translations())
print(dimple_errors)

print('Checking DataProcessing table...')
data_proc_errors = check_table(DataProcessing, results, data_processing_translations())
print(data_proc_errors)

print('Checking Refinement table...')
ref_errors = check_table(Refinement, results, refinement_translations())
print(ref_errors)

print('Checking Reference table...')
reference_errors = check_table(Reference, results, reference_translations())
print(reference_errors)


