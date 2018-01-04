import luigi
import database_operations
import pandas
import misc_functions
import db_functions
from sqlalchemy import create_engine
import os, re, subprocess
import numpy as np

from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue


class FindProjects(luigi.Task):
    def add_to_postgres(self, table, protein, subset_list, data_dump_dict, title):
        xchem_engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')

        temp_frame = table.loc[table['protein'] == protein]
        temp_frame.reset_index(inplace=True)
        temp2 = temp_frame.drop_duplicates(subset=subset_list)

        try:
            nodups = db_functions.clean_df_db_dups(temp2, title, xchem_engine,
                                                   list(data_dump_dict.keys()))
            nodups.to_sql(title, xchem_engine, if_exists='append')
        except:
            temp2.to_sql(title, xchem_engine, if_exists='append')


    def requires(self):
        return database_operations.StartTransfers()

    def output(self):
        pass

    def run(self):
        # all data necessary for uploading hits
        crystal_data_dump_dict = {'crystal_name': [], 'protein': [], 'smiles': [], 'bound_conf': [],
                                  'modification_date': []}

        # all data necessary for uploading leads
        project_data_dump_dict = {'protein': [], 'pandda_path': [], 'reference_pdb': []}

        outcome_string = '(%3%|%4%|%5%|%6%)'

        conn, c = db_functions.connectDB()

        c.execute('''SELECT crystal_id, bound_conf FROM refinement WHERE outcome SIMILAR TO %s''',
                  (str(outcome_string),))

        rows = c.fetchall()

        print(str(len(rows)) + ' crystals were found to be in refinement or above')

        for row in rows:

            c.execute('''SELECT smiles, protein FROM lab WHERE crystal_id = %s''', (str(row[0]),))

            lab_table = c.fetchall()

            if len(str(row[0])) < 3:
                continue

            if len(lab_table) > 1:
                print('WARNING: ' + str(row[0]) + ' has multiple entries in the lab table')
                # print lab_table

            for entry in lab_table:
                if len(str(entry[1])) < 2 or 'None' in str(entry[1]):
                    protein_name = str(row[0]).split('-')[0]
                else:
                    protein_name = str(entry[1])

                if len(str(row[1])) < 5:
                    print ('No bound conf for ' + str(row[0]))
                    continue

                crystal_data_dump_dict['protein'].append(protein_name)
                crystal_data_dump_dict['smiles'].append(entry[0])
                crystal_data_dump_dict['crystal_name'].append(row[0])
                crystal_data_dump_dict['bound_conf'].append(row[1])

                try:
                    modification_date = misc_functions.get_mod_date(str(row[1]))
                except:
                    modification_date = ''

                crystal_data_dump_dict['modification_date'].append(modification_date)

            c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_id = %s''', (str(row[0]),))

            pandda_info = c.fetchall()

            for pandda_entry in pandda_info:
                # project_data_dump_dict['crystal_name'].append(row[0])
                project_data_dump_dict['protein'].append(protein_name)
                project_data_dump_dict['pandda_path'].append(pandda_entry[0])
                project_data_dump_dict['reference_pdb'].append(pandda_entry[1])

        project_table = pandas.DataFrame.from_dict(project_data_dump_dict)
        crystal_table = pandas.DataFrame.from_dict(crystal_data_dump_dict)

        protein_list = set(list(project_data_dump_dict['protein']))
        print protein_list

        for protein in protein_list:

            self.add_to_postgres(project_table, protein, ['reference_pdb'], project_data_dump_dict, 'proasis_leads')

            self.add_to_postgres(crystal_table, protein, ['crystal_name', 'smiles', 'bound_conf'],
                                 crystal_data_dump_dict, 'proais_hits')


class WriteWhitelists(luigi.Task):
    def requires(self):
        return database_operations.TransferFedIDs()

    def output(self):
        pass

    def run(self):
        pass


class WriteFedIDList(luigi.Task):
    def requires(self):
        return database_operations.TransferFedIDs()

    def output(self):
        pass

    def run(self):
        pass


class StartLeadTransfers(luigi.Task):
    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class LeadTransfer(luigi.Task):
    reference_structure = luigi.Parameter()
    pandda_directory = luigi.Parameter()
    name = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pandda_analyse_centroids = str(self.pandda_directory + '/analyses/pandda_analyse_sites.csv')
        if os.path.isfile(pandda_analyse_centroids):
            site_list = pandas.read_csv(str(pandda_analyse_centroids))['native_centroid']
            print(' Searching for residue atoms for ' + str(len(site_list)) + ' site centroids \n')
            print(' NOTE: 3 residue atoms are required for each site centroid \n')

            print site_list

            no = 0
            for centroid in site_list:
                # print('next centroid')
                structure = PDBParser(PERMISSIVE=0).get_structure(str(self.name), str(self.reference_structure))
                no += 1
                res_list = []

                # initial distance for nearest neighbor (NN) search is 20A
                neighbor_distance = 20

                centroid_coordinates = centroid.replace('(', '[')
                centroid_coordinates = centroid_coordinates.replace(')', ']')
                centroid_coordinates = eval(str(centroid_coordinates))

                # define centroid as an atom object for NN search
                centroid_atom = Atom.Atom('CEN', centroid_coordinates, 0, 0, 0, 0, 9999, 'C')
                atoms = list(structure.get_atoms())
                center = np.array(centroid_atom.get_coord())
                ns = NeighborSearch(atoms)

                # calculate NN list
                neighbors = ns.search(center, neighbor_distance)
                res_list = []

                # for each atom in the NN list
                for neighbor in neighbors:
                    try:
                        # get the residue that the neighbor belongs to
                        parent = Atom.Atom.get_parent(neighbor)
                        # if the residue is not a water etc. (amino acids have blank)
                        if parent.get_id()[0] == ' ':
                            # get the chain that the residue belongs to
                            chain = Residue.Residue.get_parent(parent)
                            # if statements for fussy proasis formatting
                        if len(str(parent.get_id()[1])) == 3:
                            space = ' '
                        if len(str(parent.get_id()[1])) == 2:
                            space = '  '
                        res = (str(parent.get_resname()) + ' ' + str(chain.get_id()) + space + str(parent.get_id()[1]))
                        res_list.append(res)

                    except:
                        break
            res_list = (list(set(res_list)))
            print res_list
            lig1 = str("'" + str(res_list[0]) + ' :' + str(res_list[1]) + ' :'
                       + str(res_list[2]) + " ' ")
            print lig1

            res_string = "-o '"

            for i in range(3, len(res_list) - 1):
                res_string += str(res_list[i] + ' ,')
                res_string += str(res_list[i + 1] + ' ')
            # print str(res_string[i+1])
            submit_to_proasis = str('/usr/local/Proasis2/utils/submitStructure.py -p ' + str(self.name) + ' -t ' + str(
                self.name + '_lead -d admin -f ' + str(self.reference_structure) + ' -l ' + str(lig1)) + str(
                res_string) + "' -x XRAY -n")
            # print(submit_to_proasis)
            process = subprocess.Popen(submit_to_proasis, stdout=subprocess.PIPE, shell=True)
            out, err = process.communicate()
            print(out)
            strucidstr = misc_functions.get_id_string(out)

            add_lead = str('/usr/local/Proasis2/utils/addnewlead.py -p ' + str(self.name) + ' -s ' + str(strucidstr))
            os.system(add_lead)
        else:
            print('file does not exist!')


class HitTransfer(luigi.Task):
    # bound state pdb file from refinement
    bound_pdb = luigi.Parameter()
    # the directory that files should be copied to on the proasis side
    hit_directory = luigi.Parameter()
    # the name of the crystal
    crystal = luigi.Parameter()
    # the name of the protein name (i.e. proasis project name)
    protein_name = luigi.Parameter()
    # smiles string for the ligand
    smiles = luigi.Parameter()

    def submit_proasis_job_string(self, substring):
        process = subprocess.Popen(substring, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        strucidstr = misc_functions.get_id_string(out)

        if strucidstr != '':
            print('Success... ' + str(self.crystal) + ' submitted. ProasisID: ' + str(strucidstr) + '\n *** \n')
        else:
            print('Error: ' + str(err))

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        print('Copying refine.bound.pdb...')
        os.system(str('cp ' + str(self.bound_pdb) + ' ' + str(self.hit_directory)))

        pdb_file_name = str(self.bound_pdb).split('/')[-1]

        proasis_bound_pdb = str(str(self.hit_directory) + '/' + pdb_file_name)

        # create 2D sdf files for all ligands from SMILES string
        misc_functions.create_sd_file(self.crystal, self.smiles,
                                      str(os.path.join(self.hit_directory, self.crystal + '.sdf')))

        print('detecting ligand for ' + str(self.crystal))
        pdb_file = open(proasis_bound_pdb, 'r')
        ligands = []
        lig_string = ''
        for line in pdb_file:
            if "LIG" in line:
                lig_string = re.search(r"LIG.......", line).group()
                ligands.append(str(lig_string))

        ligands = list(set(ligands))

        if len(ligands) == 1:
            print('submission string:\n')
            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig_string + "' -m " +
                                    str(os.path.join(str(self.hit_directory), str(self.crystal) + '.sdf')) +
                                    " -p " + str(self.protein_name) + " -t " + str(self.crystal) + " -x XRAY -N")

            self.submit_proasis_job_string(submit_to_proasis)


        elif len(ligands) > 1:
            lig1 = ligands[0]
            lign = " -o '"
            for i in range(1, len(ligands) - 1):
                lign += str(ligands[i] + ',')
            lign += str(ligands[len(ligands) - 1] + "'")

            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig1 + "' " + lign + " -m " +
                                    str(os.path.join(self.hit_directory, str(self.crystal) + '.sdf')) +
                                    " -p " + str(self.protein_name) + " -t " + str(self.crystal) + " -x XRAY -N")

            self.submit_proasis_job_string(submit_to_proasis)
