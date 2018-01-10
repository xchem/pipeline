import luigi
import database_operations
import pandas
import misc_functions
import db_functions

import os, re, subprocess
import numpy as np
import proasis_api_funcs

from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue


class StartLeadTransfers(luigi.Task):
    def get_list(self):
        path_list = []
        protein_list = []
        reference_list = []
        conn, c = db_functions.connectDB()
        c.execute(
            '''SELECT pandda_path, protein, reference_pdb FROM proasis_leads WHERE pandda_path !='' and pandda_path !='None' and reference_pdb !='' and reference_pdb !='None' ''')
        rows = c.fetchall()
        for row in rows:
            path_list.append(str(row[0]))
            protein_list.append(str(row[1]))
            reference_list.append(str(row[2]))

        list = zip(path_list, protein_list, reference_list)

        return list

    def requires(self):
        list = self.get_list()
        return [LeadTransfer(pandda_directory=path, name=protein, reference_structure=reference)
                for (path, protein, reference) in list]

    def output(self):
        pass

    def run(self):
        pass


class StartHitTransfers(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem')

    def get_list(self):
        bound_list = []
        hit_directory_list = []
        crystal_list = []
        protein_list = []
        smiles_list = []
        modification_list = []

        conn, c = db_functions.connectDB()
        c.execute(
            '''SELECT bound_conf, crystal_name, protein, smiles, modification_date FROM proasis_hits WHERE bound_conf !='' and bound_conf !='None' and modification_date !='' and modification_date !='None' ''')
        rows = c.fetchall()
        for row in rows:
            bound_list.append(str(row[0]))
            hit_directory_list.append(str(self.hit_directory + str(row[2])))
            crystal_list.append(str(row[1]))
            protein_list.append(str(row[2]))
            smiles_list.append(str(row[3]))
            modification_list.append(str(row[4]))

        list = zip(bound_list, hit_directory_list, crystal_list, protein_list, smiles_list, modification_list)

        return list

    def requires(self):
        list = self.get_list()
        return [HitTransfer(bound_pdb=pdb, hit_directory=directory, crystal=crystal_name, protein=protein_name,
                            smiles=smiles_string, mod_date=modification_string)
                for (pdb, directory, crystal_name, protein_name, smiles_string, modification_string) in list]

    def output(self):
        pass

    def run(self):
        pass


class LeadTransfer(luigi.Task):
    reference_structure = luigi.Parameter()
    pandda_directory = luigi.Parameter()
    name = luigi.Parameter()

    def requires(self):
        projects = []
        all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

        json_string_projects = proasis_api_funcs.get_json(all_projects_url)
        dict_projects = proasis_api_funcs.dict_from_string(json_string_projects)

        all_projects = dict_projects['ALLPROJECTS']
        for project in all_projects:
            projects.append(str(project['project']))

        print projects
        print self.name

        if str(self.name) not in projects:
            return AddProject(protein_name=self.name), database_operations.FindProjects()
        else:
            return database_operations.FindProjects()

    def output(self):
        pass

    def run(self):

        pandda_analyse_centroids = str(self.pandda_directory + '/analyses/pandda_analyse_sites.csv')
        if os.path.isfile(pandda_analyse_centroids):
            site_list = pandas.read_csv(str(pandda_analyse_centroids))['native_centroid']
            print(' Searching for residue atoms for ' + str(len(site_list)) + ' site centroids \n')
            print(' NOTE: 3 residue atoms are required for each site centroid \n')

            print site_list

        else:
            print('file does not exist!')

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


class AddProject(luigi.Task):
    protein_name = luigi.Parameter()

    def requires(self):
        pass
        # return database_operations.FindProjects()

    def output(self):
        return luigi.LocalTarget('project_added.txt')

    def run(self):
        add_project = str('/usr/local/Proasis2/utils/addnewproject.py -q OtherClasses -p ' + str(self.protein_name))
        process = subprocess.Popen(add_project, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        if len(out) > 1:
            with self.output().open('w') as f:
                f.write(out)


class HitTransfer(luigi.Task):
    # bound state pdb file from refinement
    bound_pdb = luigi.Parameter()
    # the directory that files should be copied to on the proasis side
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/TEST')
    # the name of the crystal
    crystal = luigi.Parameter()
    # the name of the protein name (i.e. proasis project name)
    protein_name = luigi.Parameter()
    # smiles string for the ligand
    smiles = luigi.Parameter()
    # modification date to check whether the structure needs updating or not
    mod_date = luigi.Parameter()

    def submit_proasis_job_string(self, substring):
        process = subprocess.Popen(substring, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        strucidstr = misc_functions.get_id_string(out)

        if strucidstr != '':
            print('Success... ' + str(self.crystal) + ' submitted. ProasisID: ' + str(strucidstr) + '\n *** \n')
            return strucidstr
        else:
            print('Error: ' + str(err))

    def check_modification_date(self, filename):
        if os.path.isfile(filename):
            proasis_file_date = misc_functions.get_mod_date(filename)
            modification_date = self.mod_date
            if proasis_file_date != modification_date:
                conn, c = db_functions.connectDB()
                c.execute('SELECT strucid FROM proasis_hits WHERE bound_conf = %s and modification_date = %s',
                          (self.bound_pdb, modification_date))
                rows = c.fetchall()
                for row in rows:
                    if len(str(row[0])) > 1:
                        proasis_api_funcs.delete_structure(str(row[0]))
                        c.execute(
                            'UPDATE proasis_hits SET strucid = NULL WHERE bound_conf = %s and modification_date = %s',
                            (self.bound_pdb, modification_date))

    def requires(self):
        projects = []
        all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

        json_string_projects = proasis_api_funcs.get_json(all_projects_url)
        dict_projects = proasis_api_funcs.dict_from_string(json_string_projects)

        all_projects = dict_projects['ALLPROJECTS']
        for project in all_projects:
            projects.append(str(project['project']))

        print projects
        print self.protein_name

        if str(self.protein_name) not in projects:
            return AddProject(protein_name=self.protein_name), database_operations.FindProjects()
        else:
            return database_operations.FindProjects()

    def output(self):
        pass

    def run(self):

        # set up directory paths for where files will be stored (for proasis)
        proasis_protein_directory = str(str(self.hit_directory) + '/')
        proasis_crystal_directory = str(str(self.hit_directory) + '/' + str(self.protein_name) + '/')

        # find the name of the file, and create filepath name in proasis directories
        pdb_file_name = str(self.bound_pdb).split('/')[-1]
        proasis_bound_pdb = str(proasis_crystal_directory + pdb_file_name)

        self.check_modification_date(proasis_bound_pdb)

        # copy refinement pdb specified in datasource to proasis directories
        print('Copying refinement pdb...')

        # if the proasis project (protein) dir does not exist, create it
        if not os.path.isdir(proasis_protein_directory):
            print('not a directory')
            os.system(str('mkdir ' + proasis_protein_directory))

        # if the crystal directory does not exist, create it
        if not os.path.isdir(proasis_crystal_directory):
            print('not a directory')
            os.system(str('mkdir ' + proasis_crystal_directory))

        # copy the file to the proasis directories
        os.system(str('cp ' + str(self.bound_pdb) + ' ' + proasis_crystal_directory))

        # if the bound pdb is in a refinement folder, change the path to find the map files
        if 'Refine' in self.bound_pdb.replace(pdb_file_name, ''):
            remove_string = str(str(self.bound_pdb).split('/')[-2] + '/' + pdb_file_name)
            map_directory = str(self.bound_pdb).replace(remove_string, '')
        else:
            map_directory = str(self.bound_pdb).replace(pdb_file_name, '')

        # copy the 2fofc and fofc maps over to the proasis directories
        if os.path.isfile(str(map_directory + '/2fofc.map')):
            os.system(str('cp ' + str(map_directory + '/2fofc.map ' + proasis_crystal_directory)))

        if os.path.isfile(str(map_directory + '/fofc.map')):
            os.system(str('cp ' + str(map_directory + '/fofc.map ' + proasis_crystal_directory)))

        print(map_directory)

        # create 2D sdf files for all ligands from SMILES string
        misc_functions.create_sd_file(self.crystal, self.smiles,
                                      str(os.path.join(proasis_crystal_directory, self.crystal + '.sdf')))

        # look for ligands in the pdb file
        print('detecting ligand for ' + str(self.crystal))
        pdb_file = open(proasis_bound_pdb, 'r')
        ligands = []
        lig_string = ''
        for line in pdb_file:
            if "LIG" in line:
                lig_string = re.search(r"LIG.......", line).group()
                ligands.append(str(lig_string))

        # find all unique ligands
        ligands = list(set(ligands))

        # create the submission string for proasis
        if len(ligands) == 1:
            print('submission string:\n')
            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig_string + "' -m " +
                                    str(os.path.join(proasis_crystal_directory, str(self.crystal) + '.sdf')) +
                                    " -p " + str(self.protein_name) + " -t " + str(self.crystal) + " -x XRAY -N")

            # submit the structure to proasis
            strucid = self.submit_proasis_job_string(submit_to_proasis)

        # same as above, but for structures containing more than one ligand
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

            strucid = self.submit_proasis_job_string(submit_to_proasis)

        submit_2fofc = str('/usr/local/Proasis2/utils/addnewfile.py -i 2fofc_c -f '
                           + proasis_crystal_directory + '/2fofc.map -s ' + strucid)
        submit_fofc = str('/usr/local/Proasis2/utils/addnewfile.py -i fofc_c -f '
                          + proasis_crystal_directory + '/fofc.map -s ' + strucid)

        os.system(submit_2fofc)
        os.system(submit_fofc)

        # add strucid to database
        conn, c = db_functions.connectDB()
        c.execute('UPDATE proasis_hits SET strucid = %s where bound_conf = %s and modification_date = %s',
                  (strucid, self.bound_pdb, self.mod_date))


class WriteBlackLists(luigi.Task):
    def requires(self):
        pass
    def output(self):
        pass
    def run(self):
