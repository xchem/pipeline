import luigi
import luigi_classes.transfer_soakdb as transfer_soakdb
import datetime
import subprocess
from db.models import *
from functions import misc_functions, db_functions
from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue
import numpy as np
import setup_django


class InitDBEntries(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_db_%Y%m%d%H.txt'))

    def run(self):
        fail_count = 0
        refinement = Refinement.objects.filter(outcome__gte=3)
        print(len(refinement))
        for obj in refinement:
            if obj.bound_conf !='':
                bound_conf = obj.bound_conf
            elif obj.pdb_latest != '':
                bound_conf = obj.pdb_latest
            else:
                fail_count += 1
                continue

            mtz = db_functions.check_file_status('refine.mtz', bound_conf)
            if not mtz[0]:
                fail_count += 1
                continue

            two_fofc = db_functions.check_file_status('2fofc.map', bound_conf)
            if not two_fofc[0]:
                fail_count += 1
                continue

            fofc = db_functions.check_file_status('fofc.map', bound_conf)
            if not fofc[0]:
                fail_count += 1
                continue

            mod_date = misc_functions.get_mod_date(obj.bound_conf)
            if mod_date:
                proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj, crystal_name=obj.crystal_name,
                                                          pdb_file=obj.bound_conf, modification_date=mod_date,
                                                          mtz=mtz[1], two_fofc=two_fofc[1], fofc=fofc[1])

                dimple = Dimple.objects.filter(crystal_name=obj.crystal_name)
                print(dimple.count())
                if dimple.count()==1:
                    if dimple[0].reference and dimple[0].reference.reference_pdb:
                        proasis_lead_entry = ProasisLeads.objects.get_or_create(reference_pdb=dimple[0].reference)

        print(fail_count)

        with self.output().open('w') as f:
            f.write('')


class AddProject(luigi.Task):
    protein_name = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return InitDBEntries(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        self.protein_name = str(self.protein_name).upper()
        return luigi.LocalTarget(str('logs/proasis/' + str(self.protein_name) + '.added'))

    def run(self):
        self.protein_name = str(self.protein_name).upper()
        add_project = str(
            '/usr/local/Proasis2/utils/addnewproject.py -c admin -q OtherClasses -p ' + str(self.protein_name))
        process = subprocess.Popen(add_project, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        out = out.decode('ascii')
        if err:
            err = err.decode('ascii')
            raise Exception(err)
        if len(out) > 1:
            with self.output().open('w') as f:
                f.write(str(out))
                print(out)


class AddProjects(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        proteins = Target.objects.all()
        return [AddProject(protein_name=protein.target_name, date=self.date, soak_db_filepath=self.soak_db_filepath)
                for protein in proteins]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_projects_%Y%m%d%H.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')

class AddLead(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    site_centroids = luigi.Parameter()
    reference_structure = luigi.Parameter()
    target = luigi.Parameter()

    def requires(self):
        return AddProjects(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        pass

    def run(self):
        for centroid in self.site_centroids:
            # print('next centroid')
            structure = PDBParser(PERMISSIVE=0).get_structure(str(self.target).upper(), str(self.reference_structure))

            # initial distance for nearest neighbor (NN) search is 20A
            neighbor_distance = 20

            centroid_coordinates = list(centroid)

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
        lig1 = str("'" + str(res_list[0]) + ' :' + str(res_list[1]) + ' :'
                   + str(res_list[2]) + " ' ")

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

        submit_to_proasis = str('/usr/local/Proasis2/utils/submitStructure.py -p ' + str(self.target).upper() + ' -t ' +
                                str(self.target).upper() + '_lead -d admin -f ' + str(self.reference_structure) + ' -l '
                                + str(lig1) +  ' ' + full_res_string + "-x XRAY -n")
        print(submit_to_proasis)
        # process = subprocess.Popen(submit_to_proasis, stdout=subprocess.PIPE, shell=True)
        # out, err = process.communicate()
        # out = out.decode('ascii')
        # if err:
        #     err = err.decode('ascii')
        # print(out)
        # if err:
        #     raise Exception('There was a problem submitting this lead: ' + str(err))
        #
        # strucidstr = misc_functions.get_id_string(out)
        #
        # if len(strucidstr) == '':
        #     raise Exception('No strucid was detected: ' + str(out) + ' ; ' + str(err))
        #
        # add_lead = str('/usr/local/Proasis2/utils/addnewlead.py -p ' + str(self.name) + ' -s ' + str(strucidstr))
        # process = subprocess.Popen(add_lead, stdout=subprocess.PIPE, shell=True)
        # out, err = process.communicate()
        # out = out.decode('ascii')
        # if err:
        #     err = err.decode('ascii')
        # print(out)
        # if err:
        #     raise Exception('There was a problem submitting this lead: ' + str(err))
        #
        # conn, c = db_functions.connectDB()
        #
        # c.execute(
        #     'UPDATE proasis_leads SET strucid = %s WHERE reference_pdb = %s and pandda_path = %s',
        #     (strucidstr, self.reference_structure, self.pandda_directory))
        #
        # conn.commit()
        #
        # with self.output().open('wb') as f:
        #     f.write('')

class UploadLeads(luigi.Task):

    def requires(self):
        leads = ProasisLeads.objects.filter(strucid=None)
        out_dict = {'reference': [], 'sites': [], 'targets': []}
        for lead in leads:
            targets = []
            dimple = Dimple.objects.filter(reference=lead.reference_pdb)
            crystals = [dimp.crystal_name for dimp in dimple]
            lead_crystals = ProasisHits.objects.filter(crystal_name__in=crystals)
            targets = list(set([hit.crystal_name.target.target_name for hit in lead_crystals]))
            site_list = []
            for crys in lead_crystals:
                events = PanddaEvent.objects.filter(crystal=crys.crystal_name)
                sites = list(set([(round(event.site.site_native_centroid_x, 2),
                                   round(event.site.site_native_centroid_y, 2),
                                   round(event.site.site_native_centroid_z, 2)) for event in events]))
                if events and sites:
                    for site in sites:
                        site_list.append(site)

                if site_list:
                    if len(targets)==1:
                        out_dict['targets'].append(targets[0])
                        out_dict['reference'].append(lead.reference_pdb.reference_pdb)
                        out_dict['sites'].append(list(set(site_list)))

        run_zip = zip(out_dict['reference'], out_dict['sites'], out_dict['targets'])

        return [AddLead(reference_structure=ref, site_centroids=s, target=tar) for (ref, s, tar) in run_zip]

    def output(self):
        pass

    def run(self):
        pass


class UploadHit(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass