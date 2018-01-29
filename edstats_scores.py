import proasis_api_funcs as paf
import db_functions as dbf

conn, c = dbf.connectDB()

c.execute("select crystal_name, protein, smiles, strucid from proasis_hits where strucid !=''")
rows = c.fetchall()

results_dict={'crystal':[], 'protein':[], 'smiles':[], 'strucid':[]}

for row in rows:

    strucid = str(row[3])

    results_dict['crystal'].append(str(row[0]))
    results_dict['protein'].append(str(row[1]))
    results_dict['smiles'].append(str(row[2]))
    results_dict['strucid'].append(strucid)

mtz, pdb = paf.run_edstats(strucid)



