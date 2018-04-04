from functions import db_functions as dbf
from functions import proasis_api_funcs as paf

conn, c = dbf.connectDB()

c.execute('select crystal_name from proasis_hits')
rows = c.fetchall()
crystal_list = []

for row in rows:
    crystal_list.append(str(row[0]))

crystal_list = list(set(crystal_list))
protein_list = []
for crystal in crystal_list:
    protein = crystal.split('-')
    protein_list.append(protein[0])

protein_list=list(set(protein_list))

count = 0
for protein in protein_list:
    print(protein)
    print('----------')
    current_list = []
    status_list=[]
    for crys in crystal_list:
        if protein in crys:
            current_list.append(crys)
    for crystal in list(set(current_list)):
        error_list = []

        c.execute("select strucid, bound_conf, modification_date from proasis_hits where crystal_name like %s and strucid NOT LIKE ''", (crystal,))
        rows = c.fetchall()
        bound_list = []
        strucid_list = []
        mod_date_list = []
        for row in rows:
            strucid = str(row[0])
            strucid_list.append(strucid)

            bound_conf = str(row[1])
            bound_list.append(bound_conf)

            modification_date = str(row[2])
            mod_date_list.append(modification_date)

        unique_bound = list(set(bound_list))
        unique_modification_date = list(set(mod_date_list))
        unique_strucids = list(set(strucid_list))

        if len(set([len(unique_modification_date),len(unique_bound),len(unique_strucids)]))==1:
            status_list.append(0)

        file_checks = {'bound_state':[], 'mod_date':[], 'pdb':[], 'mtz':[], '2fofc':[], 'fofc':[], 'ligs':[]}

        if sum([len(unique_modification_date),len(unique_bound),len(unique_strucids)])==0:
            print(crystal)
            print('--------------------------------')
            c.execute('select bound_conf, modification_date, exists_pdb, exists_mtz, exists_2fofc, exists_fofc, ligand_list from proasis_hits where crystal_name like %s', (crystal,))
            rows = c.fetchall()
            print(rows)
            print(strucid_list)
            for row in rows:
                file_checks['bound_state'].append(str(row[0]))
                file_checks['mod_date'].append(str(row[1]))
                file_checks['pdb'].append(str(row[2]))
                file_checks['mtz'].append(str(row[3]))
                file_checks['2fofc'].append(str(row[4]))
                file_checks['fofc'].append(str(row[5]))
                file_checks['ligs'].append(str(row[6]))

            # print(file_checks)

            for key in file_checks.keys():
                if '0' in file_checks[key]:
                    error_list.append(str('missing ' + str(key) + ' file!'))

                if 'None' in file_checks[key]:
                    error_list.append(str('None value found for ' + str(key)))

            if len(error_list)>0:
                print(error_list)
                print('\n')



        elif len(set([len(unique_modification_date),len(unique_bound),len(unique_strucids)]))>1:
            print(crystal)
            print('--------------------------------')
            status_list.append(1)
            print('STATUS: FUCKED')
            print(strucid_list)
            print(bound_list)
            print(mod_date_list)
            print('\n')

        # print('unique structures = ' + str(len(unique_bound)))
        # print('unique dates = ' + str(len(unique_modification_date)))
        # print('unique strucids = ' + str(len(unique_strucids)))


    print('Number of bad structures = ' + str(sum(status_list)) + '\n')


