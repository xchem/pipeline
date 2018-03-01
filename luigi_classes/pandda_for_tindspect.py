import luigi
import sqlite3
import os
from functions import db_functions
import pandas

class CollatePanddaData(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('pandda_data/pandda_data.csv')

    def run(self):

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
                    search_list = list(set([(crystal, path) for (id, crystal, path) in compiled_list if id==file_id]))
                    for search in search_list:
                        name = search[0]
                        path = search[1]
                        for row in c2.execute('''SELECT PANDDA_site_event_map,
                                    PANDDA_site_initial_model,
                                    PANDDA_site_initial_mtz
                                    from panddaTable WHERE Crystalname = ? and PANDDApath = ? and 
                                    PANDDA_site_event_map not like ?''',
                                              (name, path, 'None')):
                            event_ccp4_map = str(row[0])
                            initial_pandda_model = str(row[1])
                            initial_pandda_map = str(row[2])