import proasis_api_funcs as paf
import db_functions as dbf
import pandas

conn, c = dbf.connectDB()

c.execute("select crystal_name, strucid from proasis_hits where strucid !=''")
rows = c.fetchall()

results_dict={'crystal':[], 'strucid':[], 'ligand':[]}

for row in rows:
    crystal = str(row[0])
    strucid = str(row[1])

    output_data, header = paf.run_edstats(strucid)
    if output_data:
        for ligand in output_data:

            lig_string = '-'.join([str(x) for x in ligand[0]])
            print lig_string
            results_dict['ligand'].append(lig_string)
            for j in range(0, len(header[24:36])):
                if header[24+j] not in results_dict.keys():
                    results_dict[header[24+j]] = []
                results_dict[header[24 + j]].append(str(ligand[1][24+j]))
            results_dict['crystal'].append(crystal)
            results_dict['strucid'].append(strucid)

data_frame = pandas.DataFrame.from_dict(results_dict)
data_frame.to_csv('edstats_proasis_ligands.csv')
print data_frame



