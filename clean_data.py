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
        # print(crystal)
        # print('--------------------------------')
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
            print('STATUS: OK')
            status_list.append(0)

        else:
            status_list.append(1)
            # print('STATUS: FUCKED')
            # print(strucid_list)
            # print(bound_list)
            # print(mod_date_list)

        # print('unique structures = ' + str(len(unique_bound)))
        # print('unique dates = ' + str(len(unique_modification_date)))
        # print('unique strucids = ' + str(len(unique_strucids)))
        # print('\n')

    print('Number of bad structures = ' + str(sum(status_list)) + '\n')


