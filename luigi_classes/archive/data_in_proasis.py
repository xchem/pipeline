import csv
import os
import re
import subprocess
import shutil

import luigi
import numpy as np
import pandas
from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue

import functions.db_functions as db_functions
import functions.misc_functions as misc_functions
import functions.proasis_api_funcs as proasis_api_funcs


class LeadTransfer(luigi.Task):
    reference_structure = luigi.Parameter()
    pandda_directory = luigi.Parameter()
    name = luigi.Parameter()
    proasis_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return AddProject(protein_name=str(self.name).upper())

    def output(self):
        mod_date = misc_functions.get_mod_date(self.reference_structure)
        return luigi.LocalTarget('logs/leads/' + str(self.name).upper() + '_' + mod_date + '.added')

    def run(self):
        #try:
            self.name = str(self.name).upper()
            pandda_analyse_centroids = str(self.pandda_directory + '/analyses/pandda_analyse_sites.csv')
            if os.path.isfile(pandda_analyse_centroids):
                site_list = pandas.read_csv(str(pandda_analyse_centroids))['native_centroid']
                print((' Searching for residue atoms for ' + str(len(site_list)) + ' site centroids \n'))
                print(' NOTE: 3 residue atoms are required for each site centroid \n')

                print(site_list)

            else:
                raise Exception('file does not exist!')

            no = 0
            for centroid in site_list:
                # print('next centroid')
                structure = PDBParser(PERMISSIVE=0).get_structure(str(self.name), str(self.reference_structure))
                no += 1

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
                        if 'HOH' not in str(parent.get_resname()):
                            res = (
                                str(parent.get_resname()) + ' ' + str(chain.get_id()) + space + str(parent.get_id()[1]))
                            res_list.append(res)

                    except:
                        continue
            res_list = (list(set(res_list)))
            print(res_list)
            lig1 = str("'" + str(res_list[0]) + ' :' + str(res_list[1]) + ' :'
                       + str(res_list[2]) + " ' ")
            print(lig1)

            # some faff to get rid of waters and add remaining ligands in multiples of 3 - proasis is fussy
            alt_lig_option = " -o '"
            res_string = ""
            full_res_string = ''
            count = 0

            for i in range(3, len(res_list)):
                count += 1

            multiple = int(round(count / 3) * 3)
            count = 0
            for i in range(3, multiple):
                if count == 0:
                    res_string += alt_lig_option
                if count <= 1:
                    res_string += str(res_list[i] + ' ,')
                    count += 1
                elif count == 2:
                    res_string += str(res_list[i] + " '")
                    full_res_string.join(res_string)
                    count = 0

            # copy reference structure to proasis directories
            ref_structure_file_name = str(self.reference_structure).split('/')[-1]
            proasis_project_directory = str(str(self.proasis_directory) + '/' + str(self.name))
            proasis_reference_directory = str(str(proasis_project_directory) + '/reference/')
            proasis_reference_structure = str(proasis_reference_directory + '/' + str(ref_structure_file_name))

            if not os.path.isdir(str(proasis_project_directory)):
                os.mkdir(proasis_project_directory)
            if not os.path.isdir(proasis_reference_directory):
                os.mkdir(proasis_reference_directory)
            shutil.copy2(self.reference_structure, proasis_reference_structure)

            submit_to_proasis = str('/usr/local/Proasis2/utils/submitStructure.py -p ' + str(self.name) + ' -t ' + str(
                self.name) + '_lead -d admin -f ' + str(proasis_reference_structure) + ' -l ' + str(
                lig1) + "-x XRAY -n")
            print(submit_to_proasis)
            process = subprocess.Popen(submit_to_proasis, stdout=subprocess.PIPE, shell=True)
            out, err = process.communicate()
            out = out.decode('ascii')
            if err:
                err = err.decode('ascii')
            print(out)
            if err:
                raise Exception('There was a problem submitting this lead: ' + str(err))

            strucidstr = misc_functions.get_id_string(out)

            if len(strucidstr)=='':
                raise Exception('No strucid was detected: ' + str(out) + ' ; ' + str(err))

            add_lead = str('/usr/local/Proasis2/utils/addnewlead.py -p ' + str(self.name) + ' -s ' + str(strucidstr))
            process = subprocess.Popen(add_lead, stdout=subprocess.PIPE, shell=True)
            out, err = process.communicate()
            out = out.decode('ascii')
            if err:
                err = err.decode('ascii')
            print(out)
            if err:
                raise Exception('There was a problem submitting this lead: ' + str(err))

            conn, c = db_functions.connectDB()

            c.execute(
                'UPDATE proasis_leads SET strucid = %s WHERE reference_pdb = %s and pandda_path = %s',
                (strucidstr, self.reference_structure, self.pandda_directory))

            conn.commit()

            with self.output().open('wb') as f:
                f.write('')


# class AddProject(luigi.Task):
#     protein_name = luigi.Parameter()
#
#     def output(self):
#         self.protein_name = str(self.protein_name).upper()
#         return luigi.LocalTarget(str('logs/projects/' + str(self.protein_name) + '.added'))
#
#     def run(self):
#         try:
#             self.protein_name = str(self.protein_name).upper()
#             add_project = str(
#                 '/usr/local/Proasis2/utils/addnewproject.py -c admin -q OtherClasses -p ' + str(self.protein_name))
#             process = subprocess.Popen(add_project, stdout=subprocess.PIPE, shell=True)
#             out, err = process.communicate()
#             out = out.decode('ascii')
#             if err:
#                 err = err.decode('ascii')
#             if len(out) > 1:
#                 with self.output().open('w') as f:
#                     f.write(str(out))
#             else:
#                 with self.output().open('w') as f:
#                     f.write(str(err))
#         except:
#             with self.output().open('w') as f:
#                 f.write('FAIL')


class FindLigands(luigi.Task):
    bound_conf = luigi.Parameter()

    def requires(self):
        conn, c = db_functions.connectDB()
        c.execute('select protein from proasis_hits where bound_conf like %s', (self.bound_conf,))
        rows = c.fetchall()
        protein_list = []
        for row in rows:
            protein_list.append(str(row[0]).upper())
        return [AddProject(protein_name=protein) for protein in list(set(protein_list))]

    def output(self):
        out_name = '_'.join(self.bound_conf.split('/')[-4:-1])
        return luigi.LocalTarget('logs/find_ligands/' + out_name + '.done')

    def run(self):
        try:
            ligand_list = list(set(db_functions.get_pandda_lig_list(self.bound_conf)))
        except:
            try:
                pdb_file = open(self.bound_conf, 'r')
                ligand_list = []
                for line in pdb_file:
                    if "LIG" in line:
                        try:
                            lig_string = re.search(r"LIG.......", line).group()
                            ligand_list.append(list(filter(bool, list(lig_string.split(' ')))))
                        except:
                            continue
            except:
                ligand_list = None

        if ligand_list:
            unique_ligands = [list(x) for x in set(tuple(x) for x in ligand_list)]
        else:
            unique_ligands = None

        conn, c = db_functions.connectDB()
        c.execute('UPDATE proasis_hits SET ligand_list=%s WHERE bound_conf=%s',(str(unique_ligands), self.bound_conf))
        conn.commit()
        print(unique_ligands)

        with self.output().open('wb') as f:
            f.write('')


class CopyFilesForProasis(luigi.Task):
    protein_name = luigi.Parameter()
    hit_directory = luigi.Parameter()
    crystal = luigi.Parameter()
    bound_pdb = luigi.Parameter()
    mod_date = luigi.Parameter()

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
        return FindLigands(bound_conf=self.bound_pdb)

    def output(self):
        return luigi.LocalTarget(str('logs/proasis_file_transfer/' + self.crystal + '_' + self.mod_date + '.done'))

    def run(self):
        # set up directory paths for where files will be stored (for proasis)
        self.protein_name = str(self.protein_name).upper()
        proasis_protein_directory = str(str(self.hit_directory) + '/' + str(self.protein_name) + '/')
        proasis_crystal_directory = str(str(self.hit_directory) + '/' + str(self.protein_name) + '/'
                                        + str(self.crystal) + '/')

        # if the proasis project (protein) dir does not exist, create it
        if not os.path.isdir(proasis_protein_directory):
            print('not a directory')
            os.mkdir(proasis_protein_directory)

        # if the crystal directory does not exist, create it
        if not os.path.isdir(proasis_crystal_directory):
            print('not a directory')
            os.mkdir(proasis_crystal_directory)

        # find the name of the file, and create filepath name in proasis directories
        pdb_file_name = str(self.bound_pdb).split('/')[-1]
        proasis_bound_pdb = str(proasis_crystal_directory + pdb_file_name)

        self.check_modification_date(proasis_bound_pdb)

        #if the bound pdb is in a refinement folder, change the path to find the map files
        if 'Refine' in str(self.bound_pdb).replace(pdb_file_name, ''):
            remove_string = str(str(self.bound_pdb).split('/')[-2] + '/' + pdb_file_name)
            map_directory = str(self.bound_pdb).replace(remove_string, '')
        else:
            map_directory = str(self.bound_pdb).replace(pdb_file_name, '')

        # copy the 2fofc and fofc maps over to the proasis directories
        if os.path.isfile(str(map_directory + '/2fofc.map')):
            shutil.copy2(os.path.join(map_directory,'2fofc.map'), proasis_crystal_directory)
        else:
            raise Exception('2f0fc map not found! Will not upload to proasis!')

        if os.path.isfile(str(map_directory + '/fofc.map')):
            shutil.copy2(os.path.join(map_directory, 'fofc.map'), proasis_crystal_directory)
        else:
            raise Exception('f0fc map not found! Will not upload to proasis!')

        if os.path.isfile(str(map_directory + '/refine.mtz')):
            shutil.copy2(os.path.join(map_directory, 'refine.mtz'), proasis_crystal_directory)
        else:
            raise Exception('mtz file not found! Will not upload to proasis!')

        #copy refinement pdb specified in datasource to proasis directories
        print('Copying refinement pdb...')

        # copy the file to the proasis directories
        shutil.copy2(self.bound_pdb, proasis_crystal_directory)

        with self.output().open('wb') as f:
            f.write('')


class GenerateSdfFile(luigi.Task):
    # for sdf generation
    crystal = luigi.Parameter()
    smiles = luigi.Parameter()

    # for CopyFilesForProasis() requirement
    protein_name = luigi.Parameter()
    hit_directory = luigi.Parameter()
    bound_pdb = luigi.Parameter()

    def requires(self):
        pass


    def output(self):
        self.protein_name = str(self.protein_name).upper()
        proasis_crystal_directory = os.path.join(self.hit_directory, self.protein_name, self.crystal)

        return luigi.LocalTarget(str(os.path.join(proasis_crystal_directory, str(self.crystal + '.sdf'))))

    def run(self):
        self.protein_name = str(self.protein_name).upper()
        proasis_crystal_directory = os.path.join(self.hit_directory, self.protein_name, self.crystal)
        misc_functions.create_sd_file(self.crystal, self.smiles,
                                      str(os.path.join(proasis_crystal_directory, str(self.crystal + '.sdf'))))


class CheckFilesForUpload(luigi.Task):
    # bound state pdb file from refinement
    bound_pdb = luigi.Parameter()
    mod_date = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('logs/upload_checks/' + str(self.bound_pdb.split('/')[-1] + '_' + self.mod_date + '.done'))

    def run(self):
        conn, c = db_functions.connectDB()
        exists = db_functions.column_exists('proasis_hits', 'exists_pdb')
        if not exists:
            execute_string = str("ALTER TABLE proasis_hits ADD COLUMN exists_pdb text;")
            c.execute(execute_string)
            conn.commit()
        if os.path.isfile(self.bound_pdb):
            db_functions.check_file_status('mtz', 'refine.mtz', self.bound_pdb)
            db_functions.check_file_status('2fofc', '2fofc.map', self.bound_pdb)
            db_functions.check_file_status('fofc', 'fofc.map', self.bound_pdb)
            status = 1
        else:
            status = 0

        c.execute('UPDATE proasis_hits SET exists_pdb=%s WHERE bound_conf=%s', (status, self.bound_pdb))
        conn.commit()

        c.execute("SELECT strucid, exists_pdb, exists_2fofc, exists_fofc, exists_mtz FROM proasis_hits WHERE bound_conf=%s and strucid !=''", (self.bound_pdb,))
        rows = c.fetchall()
        for row in rows:
            if '0' in [str(row[1]), str(row[2]), str(row[3]), str(row[4])]:
                proasis_api_funcs.delete_structure(str(row[0]))
                c.execute("UPDATE proasis_hits SET strucid='' WHERE bound_conf=%s", (self.bound_pdb,))
                conn.commit()

        with self.output().open('wb') as f:
            f.write('')



class HitTransfer(luigi.Task):
    # bound state pdb file from refinement
    bound_pdb = luigi.Parameter()
    # the directory that files should be copied to on the proasis side
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    # the name of the crystal
    crystal = luigi.Parameter()
    # the name of the protein name (i.e. proasis project name)
    protein_name = luigi.Parameter()
    # smiles string for the ligand
    smiles = luigi.Parameter()
    # modification date to check whether the structure needs updating or not
    mod_date = luigi.Parameter()

    ligands = luigi.Parameter()

    def requires(self):
        self.protein_name = str(self.protein_name).upper()
        return AddProject(protein_name=self.protein_name), CopyFilesForProasis(protein_name=self.protein_name, hit_directory=self.hit_directory,
                                   crystal=self.crystal,
                                   bound_pdb=self.bound_pdb, mod_date=self.mod_date), FindLigands(
            bound_conf=self.bound_pdb), GenerateSdfFile(crystal=self.crystal, smiles=self.smiles,
                                                        protein_name=self.protein_name,
                                                        hit_directory=self.hit_directory, bound_pdb=self.bound_pdb)

    def output(self):
        return luigi.LocalTarget('logs/hits/' + str(self.crystal) + '_' + self.mod_date + '.added')

    def run(self):
        self.protein_name = str(self.protein_name).upper()
        self.ligands = eval(self.ligands)

        # proasis_protein_directory = str(str(self.hit_directory) + '/' + str(self.protein_name) + '/')
        proasis_crystal_directory = str(str(self.hit_directory) + '/' + str(self.protein_name) + '/'
                                        + str(self.crystal) + '/')

        pdb_file_name = str(self.bound_pdb).split('/')[-1]
        proasis_bound_pdb = str(proasis_crystal_directory + pdb_file_name)

        # Check whether there is already an entry for crystal in masterdb
        conn, c = db_functions.connectDB()

        c.execute("SELECT strucid, bound_conf FROM proasis_hits WHERE crystal_name LIKE %s", (self.crystal,))
        rows = c.fetchall()

        current_visit = str(self.bound_pdb).split('/')[4]
        title = str(self.crystal)

        if len(self.ligands) == 1:
            lig_string = str(proasis_api_funcs.get_lig_strings(self.ligands)[0])
            print('submission string:\n')
            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig_string + "' -m " +
                                    str(os.path.join(proasis_crystal_directory, str(self.crystal) + '.sdf')) +
                                    " -p " + str(self.protein_name) + " -t " + title + " -x XRAY -N")

            # submit the structure to proasis
            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)


        # same as above, but for structures containing more than one ligand
        elif len(self.ligands) > 1:
            ligands_list = proasis_api_funcs.get_lig_strings(self.ligands)
            print(ligands_list)
            lig1 = ligands_list[0]
            lign = " -o '"
            for i in range(1, len(ligands_list) - 1):
                lign += str(ligands_list[i] + ',')
            lign += str(ligands_list[len(ligands_list) - 1] + "'")

            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig1 + "' " + lign + " -m " +
                                    str(os.path.join(proasis_crystal_directory, str(self.crystal) + '.sdf')) +
                                    " -p " + str(self.protein_name) + " -t " + title + " -x XRAY -N")
            print(submit_to_proasis)

            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)

        elif len(self.ligands)==0:
            raise Exception('No ligands were found!')

        if strucid != '':

            out, err = proasis_api_funcs.add_proasis_file(file_type='2fofc_c', filename=os.path.join(proasis_crystal_directory, '2fofc.map'),
                                  strucid=strucid, title=str(self.crystal + '_2fofc'))


            print(out)
            if err:
                raise Exception(out)

            out, err = proasis_api_funcs.add_proasis_file(file_type='fofc_c', filename=os.path.join(proasis_crystal_directory, 'fofc.map'),
                                  strucid=strucid, title=str(self.crystal + '_fofc'))

            print(out)
            if err:
                raise Exception(out)

            out, err = proasis_api_funcs.add_proasis_file(file_type='mtz', filename=os.path.join(proasis_crystal_directory, 'refine.mtz'),
                                  strucid=strucid, title=str(self.crystal + '_mtz'))

            print(out)

            if err:
                raise Exception(out)

        else:
            raise Exception('proasis failed to upload structure: ' + out)

        # add strucid to database
        print('Updating master DB')
        conn, c = db_functions.connectDB()
        c.execute("UPDATE proasis_hits SET strucid=%s where bound_conf=%s and modification_date=%s",
                  (str(strucid), str(self.bound_pdb), str(self.mod_date)))

        conn.commit()
        print(c.query)
        print(c.statusmessage)

        if 'UPDATE 0' in c.statusmessage:
            proasis_api_funcs.delete_structure(strucid)
            raise Exception('db was not updated! structure removed from proasis!')

        with self.output().open('wb') as f:
            f.write('')


class WriteBlackLists(luigi.Task):
    def requires(self):
        return FindSoakDBFiles(), StartLeadTransfers(), StartHitTransfers()

    def output(self):
        return luigi.LocalTarget('logs/blacklists.done')

    def run(self):
        proposal_dict, strucids = db_functions.get_strucid_list()
        fedid_list = db_functions.get_fedid_list()

        directory_path = '/usr/local/Proasis2/Data/BLACKLIST'

        for fedid in fedid_list:
            db_functions.create_blacklist(fedid, proposal_dict, directory_path)

        with open('/usr/local/Proasis2/Data/BLACKLIST/other_user.dat', 'w') as f:
            wr = csv.writer(f)
            wr.writerow(strucids)

        with open(str(directory_path + '/uzw12877.dat'),'w') as f:
            f.write('')

        with self.output().open('wb') as f:
            f.write('')
