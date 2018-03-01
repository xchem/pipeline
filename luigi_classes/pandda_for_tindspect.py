import luigi
import sqlite3
import os
from functions import db_functions
import pandas
import datetime

class CollatePanddaData(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(str('pandda_data/pandda_scrape_' + str(self.date) + '.csv'))

    def run(self):

        results = {'crystal_name':[], 'pandda_path':[], 'event_ccp4_map':[],
                   'initial_pandda_model':[], 'initial_pandda_map':[],
                   'lig_res':[], 'lig_chain':[], 'lig_seq':[], 'lig_alt_chain':[], 'smiles':[], 'compound_code':[]}

        conn, c = db_functions.connectDB()
        if os.path.isfile(self.output().path):
            existing_data = pandas.DataFrame.from_csv(self.output().path)
        else:
            id_list = []
            crystal_list = []
            pandda_list = []
            c.execute("SELECT file_id, crystal_name, pandda_path from dimple WHERE pandda_run='True'")
            rows = c.fetchall()
            for row in rows:
                id_list.append(str(row[0]))
                crystal_list.append(str(row[1]))
                pandda_list.append(str(row[2]))

            compiled_list = zip(id_list, crystal_list, pandda_list)

            file_ids = list(set([x for (x,_,_) in compiled_list]))

            for file_id in file_ids:
                c.execute('SELECT filename from soakdb_files WHERE id=%s', (file_id,))
                rows = c.fetchall()
                for row in rows:
                    filename = str(row[0])
                    c2 = sqlite3.connect(filename)
                    search_list = list(set([(id, crystal, path) for (id, crystal, path) in compiled_list if id==file_id]))
                    for search in search_list:
                        id = search[0]
                        name = search[1]
                        path = search[2]
                        c.execute("SELECT compound_code, smiles FROM lab WHERE file_id=%s and crystal_name=%s", (id, name))
                        rows = c.fetchall()
                        if len(rows)>1:
                            break
                        else:
                            for row in rows:
                                compound_code = str(row[0])
                                smiles = str(row[1])
                        for row in c2.execute('''SELECT PANDDA_site_event_map,
                                    PANDDA_site_initial_model,
                                    PANDDA_site_initial_mtz,
                                    PANDDA_site_ligand_resname,
                                    PANDDA_site_ligand_chain,
                                    PANDDA_site_ligand_sequence_number,
                                    PANDDA_site_ligand_altloc
                                    from panddaTable WHERE Crystalname = ? and PANDDApath = ? and 
                                    PANDDA_site_event_map not like ?''',
                                              (name, path, 'None')):

                            results['crystal_name'].append(name)
                            results['pandda_path'].append(path)
                            results['event_ccp4_map'].append(str(row[0]))
                            results['initial_pandda_model'].append(str(row[1]))
                            results['initial_pandda_map'].append(str(row[2]))
                            results['lig_res'].append(str(row[3]))
                            results['lig_chain'].append(str(row[4]))
                            results['lig_seq'].append(str(row[5]))
                            results['lig_alt_chain'].append(str(row[6]))
                            results['smiles'].append(smiles)
                            results['compound_code'].append(compound_code)

            frame = pandas.DataFrame.from_dict(results)
            frame.to_csv(self.output().path)