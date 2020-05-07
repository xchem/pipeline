import pandas
import os
import sqlite3

sdb = '/dls/labxchem/data/2020/lb18145-153/processing/database/soakDBDataFile.sqlite'

if os.path.isfile('mpro.xlsx'):
    os.remove('mpro.xlsx')

wget = 'wget -O mpro.xlsx "https://dlsltd-my.sharepoint.com/:x:/g/personal/daren_fearon_diamond_ac_uk/Eevaf_bECPtAmA6SV9zW5bkBbUFD9L3k3HYSAEnrzfuzlg?e=rSdpNe&CID=74a8e989-80c4-fcae-8b26-63785c1228ce&download=1"'
os.system(wget)

df = pandas.read_excel('mpro.xlsx', sheet_name='Summary')

mapping = {
    'Dataset':'crystal_name',
    'Compound SMILES': 'smiles',
    'ModifiedCompoundSMILES': 'new_smiles',
    'Moonshot ID': 'alternate_name',
    'Site': 'site_name',
    'PDB Entry': 'pdb_entry'
}

new_frame = df[mapping.keys()].rename(columns=mapping)

new_frame.to_csv('metadata.csv')

for _, row in new_frame.iterrows():
    if isinstance(row['new_smiles'], str):
        prod_smiles = row['new_smiles']
        crystal_name = row['crystal_name']

        # crystal = Crystal.objects.get(crystal_name=crystal_name)

        conn = sqlite3.connect(sdb)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("select * from mainTable where CrystalName = ?", (crystal_name,))

        results = c.fetchall()
        for result in results:
            sdb_prod_smiles = result['CompoundSMILESproduct']

            if str(sdb_prod_smiles)!= str(prod_smiles):
                c.execute("update mainTable set CompoundSMILESproduct=? where CrystalName=?;", (prod_smiles, crystal_name, ))
