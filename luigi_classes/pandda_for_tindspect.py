import luigi
import sqlite3
import os
from functions import db_functions
import pandas
import datetime
import subprocess

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


class GenerateEventMtz(luigi.Task):
    pandda_input_mtz = luigi.Parameter()
    native_event_map = luigi.Parameter()

    def requires(self):
        return CollatePanddaData()

    def output(self):
        return luigi.LocalTarget(self.native_event_map.replace('.ccp4', '.mtz'))

    def run(self):
        resolution_high = 'n/a'
        resolution_line = 1000000
        mtzdmp = subprocess.Popen(['mtzdmp', self.pandda_input_mtz], stdout=subprocess.PIPE)
        for n, line in enumerate(iter(mtzdmp.stdout.readline, '')):
            if line.startswith(' *  Resolution Range :'):
                resolution_line = n + 2
            if n == resolution_line and len(line.split()) == 8:
                resolution_high = line.split()[5]

        mapmask_string = '''module load ccp4; mapmask         \\
mapin %s           \\
mapout %s << eof               
xyzlim cell
symmetry p1
MODE mapin
eof''' % (self.native_event_map, self.native_event_map.replace('native', 'p1'))

        #print command_string

        mapin = subprocess.Popen(mapmask_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = mapin.communicate()

        print out

        convert_string = '''module load phenix; phenix.map_to_structure_factors %s d_min=%s output_file_name=%s''' \
                         % (self.native_event_map.replace('native', 'p1'), resolution_high,
                            self.native_event_map.replace('.ccp4', '.mtz'))

class StartMapConversions(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        try:
            in_frame = pandas.DataFrame.from_csv(str('pandda_data/pandda_scrape_' + str(self.date) + '.csv'))
        except:
            return CollatePanddaData()
        input_mtz_list = in_frame['initial_pandda_map']
        native_event_list = in_frame['event_ccp4_map']
        return [GenerateEventMtz(pandda_input_mtz=input_mtz, native_event_map=input_ccp4) for (input_mtz, input_ccp4) in
                list(zip(input_mtz_list, native_event_list))]
