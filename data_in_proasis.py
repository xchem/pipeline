import luigi
import psycopg2
import database_operations
import pandas

class FindProjects(luigi.Task):

    def requires(self):
        return database_operations.TransferExperiment()

    def output(self):
        pass

    def run(self):
        # all data necessary for uploading hits
        crystal_data_dump_dict = {'crystal_name':[], 'protein':[], 'smiles':[], 'bound_conf':[]}

        # all data necessary for uploading leads
        project_data_dump_dict = {'crystal_name':[], 'protein':[], 'pandda_path':[], 'reference_pdb':[]}

        outcome_string = '(%3%|%4%|%5%|%6%)'

        conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
        c = conn.cursor()

        c.execute('''SELECT crystal_name, outcome, bound_conf FROM refinement WHERE outcome SIMILAR TO %s''', (str(outcome_string),))

        rows = c.fetchall()

        for row in rows:
            crystal_data_dump_dict['crystal_name'].append(row[0])
            crystal_data_dump_dict['bound_conf'].append(row[2])

            c.execute('''SELECT protein, smiles FROM lab WHERE crystal_name = %s''', (str(row[0]),))

            lab_table = c.fetchall()

            for entry in lab_table:
                crystal_data_dump_dict['protein'].append(entry[0])
                crystal_data_dump_dict['smiles'].append(entry[1])

                c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_name = %s''', (str(row[0]),))

                pandda_info = c.fetchall()

                for pandda_entry in pandda_info:
                    project_data_dump_dict['crystal_name'].append(row[0])
                    project_data_dump_dict['protein'].append(entry[0])
                    project_data_dump_dict['pandda_path'].append(pandda_entry[0])
                    project_data_dump_dict['reference_pdb'].append(pandda_entry[1])

