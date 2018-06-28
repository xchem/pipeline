import setup_django
from db.models import *
from functions.db_functions import *

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

lab_trans = lab_translations()

for key in lab_trans.keys():
    print(lab_trans[key])
    for row in results:
        print(row[key])

#
#
# print(len(proteins))
#
# print(proteins)
#
# for row in results:
#     # set up blank dictionary to hold model values
#     d = {}
#     # get the keys and values of the query
#     row_keys = row.keys()
#     row_values = list(tuple(row))
#
# # print())
#
