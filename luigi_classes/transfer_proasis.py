import luigi
from . import transfer_soakdb
import datetime
import subprocess
import os
import shutil
import re
import glob
import csv
from xchem_db.models import *
from functions import misc_functions, db_functions, proasis_api_funcs
from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue
import numpy as np
import setup_django


class InitDBEntries(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_db_%Y%m%d%H.txt'))

    def run(self):
        fail_count = 0
        refinement = Refinement.objects.filter(outcome__gte=3)
        print(len(refinement))
        for obj in refinement:
            bound_conf = ''
            files = []
            mtz = ''
            two_fofc = ''
            fofc = ''
            mod_date = ''
            proasis_hit_entry=''
            entry=''
            if obj.bound_conf:
                if os.path.isfile(obj.bound_conf):
                    bound_conf = obj.bound_conf
            elif obj.pdb_latest:
                if os.path.isfile(obj.pdb_latest):
                    if 'Refine' in obj.pdb_latest:
                        search_path = '/'.join(obj.pdb_latest.split('/')[:-1])
                        files = glob.glob(str(search_path + '/refine*bound*.pdb'))
                        if len(files) == 1:
                            bound_conf = files[0]
                    else:
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

            if bound_conf:
                try:
                    pdb_file = open(bound_conf, 'r')
                    ligand_list = []
                    for line in pdb_file:
                        if "LIG" in line:
                            try:
                                lig_string = re.search(r".LIG.......", line).group()
                                ligand_list.append(list(filter(bool, list(lig_string.split(' ')))))
                            except:
                                continue
                except:
                    ligand_list = None

            if not ligand_list:
                continue

            unique_ligands = [list(x) for x in set(tuple(x) for x in ligand_list)]

            mod_date = misc_functions.get_mod_date(bound_conf)
            if mod_date:
                if ProasisHits.objects.filter(refinement=obj, crystal_name=obj.crystal_name).exists():
                    entry = ProasisHits.objects.get(refinement=obj, crystal_name=obj.crystal_name)
                    if entry.modification_date < mod_date or not entry.strucid:
                        if entry.strucid:
                            proasis_api_funcs.delete_structure(entry.strucid)
                            entry.strucid = None
                            entry.save()
                            if self.hit_directory in entry.pdb_file:
                                os.remove(entry.pdb_file)
                            if self.hit_directory in entry.mtz:
                                os.remove(entry.mtz)
                            if self.hit_directory in entry.two_fofc:
                                os.remove(entry.two_fofc)
                            if self.hit_directory in entry.fofc:
                                os.remove(entry.fofc)

                        entry.pdb_file = bound_conf
                        entry.modification_date = mod_date
                        entry.mtz = mtz[1]
                        entry.two_fofc = two_fofc[1]
                        entry.fofc = fofc[1]
                        entry.ligand_list = unique_ligands
                        entry.save()

                else:
                    proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj, crystal_name=obj.crystal_name,
                                                                          pdb_file=bound_conf,
                                                                          modification_date=mod_date,
                                                                          mtz=mtz[1], two_fofc=two_fofc[1],
                                                                          fofc=fofc[1], ligand_list=unique_ligands)

                dimple = Dimple.objects.filter(crystal_name=obj.crystal_name)
                if dimple.count() == 1:
                    if dimple[0].reference and dimple[0].reference.reference_pdb:
                        if os.path.isfile(dimple[0].reference.reference_pdb):
                            proasis_lead_entry = ProasisLeads.objects.get_or_create(reference_pdb=dimple[0].reference)
                        else:
                            if ProasisLeads.objects.filter(reference_pdb=dimple[0].reference).exists():
                                print('removing...')
                                proasis_lead_entry = ProasisLeads.objects.get(reference_pdb=dimple[0].reference)
                                proasis_lead_entry.delete()

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
            neighbor_distance = 10

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

                    if len(str(parent.get_id()[1])) >= 3:
                        space = ' '
                    if len(str(parent.get_id()[1])) == 2:
                        space = '  '
                    if len(str(parent.get_id()[1])) == 1:
                        space = '   '
                    if 'HOH' not in str(parent.get_resname()):
                        res = (
                                str(parent.get_resname()) + ' ' + str(chain.get_id()) + space + str(parent.get_id()[1]))
                        res_list.append(res)

                except:
                    continue

        res_list = (list(set(res_list)))
        print(res_list)
        colon = ' :'
        if len(str(res_list[0])) >= 10:
            colon = ':'
        lig1 = str("'" + str(res_list[0]) + colon)
        if len(str(res_list[1])) >= 10:
            colon = ':'
        lig1 += str(str(res_list[1]) + colon)
        close = " ' "
        if len(str(res_list[2])) >= 10:
            close = "' "
        lig1 += str(str(res_list[2]) + close)

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
            print(len(str(res_list[i])))
            if count == 0:
                res_string += alt_lig_option
            if count <= 1:
                if len(str(res_list[i])) >= 10:
                    res_string += str(res_list[i] + ',')
                else:
                    res_string += str(res_list[i] + ' ,')
                count += 1
            elif count == 2:
                if len(str(res_list[i])) >= 10:
                    res_string += str(res_list[i] + "'")
                else:
                    res_string += str(res_list[i] + " '")
                full_res_string = full_res_string + res_string
                count = 0

        # string to submit structure as lead
        submit_to_proasis = str('/usr/local/Proasis2/utils/submitStructure.py -p ' + str(self.target).upper() + ' -t ' +
                                str(self.target).upper() + '_lead -d admin -f ' + str(self.reference_structure) + ' -l '
                                + str(lig1) + ' ' + full_res_string + " -x XRAY -n")

        # for debug
        print(submit_to_proasis)

        # submit and read output - raise exception if neccessary
        process = subprocess.Popen(submit_to_proasis, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        out = out.decode('ascii')
        if err:
            err = err.decode('ascii')

        # find strucid from submission output
        strucidstr = misc_functions.get_id_string(out)

        # if no strucid in output, raise an exception and print error
        if len(strucidstr) == 0:
            raise Exception('No strucid was detected: ' + str(out) + ' ; ' + str(err))

        # add the new structure as a lead
        add_lead = str('/usr/local/Proasis2/utils/addnewlead.py -p ' + str(self.target).upper() +
                       ' -s ' + str(strucidstr))

        process = subprocess.Popen(add_lead, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        out = out.decode('ascii')

        # report any errors
        if err:
            err = err.decode('ascii')
        print(out)
        if err:
            raise Exception('There was a problem submitting this lead: ' + str(err))

        # update lead object strucid field
        lead = ProasisLeads.objects.get(reference_pdb=Reference.objects.get(reference_pdb=self.reference_structure))
        lead.strucid = strucidstr
        lead.save()


class UploadLeads(luigi.Task):
    debug = luigi.Parameter(default=False)
    date = luigi.DateParameter(default=datetime.date.today())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

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
                    if len(targets) == 1:
                        out_dict['targets'].append(targets[0])
                        out_dict['reference'].append(lead.reference_pdb.reference_pdb)
                        out_dict['sites'].append(list(set(site_list)))

        run_zip = zip(out_dict['reference'], out_dict['sites'], out_dict['targets'])
        print(run_zip)

        if self.debug:
            runner = list(run_zip)[0]
            return AddLead(reference_structure=runner[0], site_centroids=runner[1], target=runner[2])
        else:
            return [AddLead(reference_structure=ref, site_centroids=s, target=tar) for (ref, s, tar) in run_zip]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/hits/proasis_leads_%Y%m%d%H.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CopyFile(luigi.Task):
    proasis_hit = luigi.Parameter()
    crystal = luigi.Parameter()
    update_field = luigi.Parameter()
    filename = luigi.Parameter()
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        pass

    def output(self):
        target_name = str(self.crystal.target.target_name).upper()
        crystal_name = str(self.crystal.crystal_name)
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')
        filepath = os.path.join(proasis_crystal_directory, str(self.filename.split('/')[-1]))
        return luigi.LocalTarget(filepath)

    def run(self):

        # get target and crystal name from crystal object
        target_name = str(self.crystal.target.target_name).upper()
        crystal_name = str(self.crystal.crystal_name)

        # set directory for files to be copied to and create if necessary
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')
        print(proasis_crystal_directory)
        if not os.path.isdir(proasis_crystal_directory):
            os.makedirs(proasis_crystal_directory)

        os.symlink(self.filename, os.path.join(proasis_crystal_directory, str(self.filename.split('/')[-1])))

        if self.update_field == 'pdb':
            self.proasis_hit.pdb_file = self.output().path
            self.proasis_hit.save()
        if self.update_field == 'two_fofc':
            self.proasis_hit.two_fofc = self.output().path
            self.proasis_hit.save()
        if self.update_field == 'fofc':
            self.proasis_hit.fofc = self.output().path
            self.proasis_hit.save()
        if self.update_field == 'mtz':
            self.proasis_hit.mtz = self.output().path
            self.proasis_hit.save()
        print(self.proasis_hit.pdb_file)


class CopyInputFiles(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal = Crystal.objects.get(pk=self.crystal_id)

        return CopyFile(proasis_hit=proasis_hit, crystal=crystal, update_field='pdb',
                        filename=str(proasis_hit.pdb_file)), \
               CopyFile(proasis_hit=proasis_hit, crystal=crystal, update_field='two_fofc',
                        filename=str(proasis_hit.two_fofc)), \
               CopyFile(proasis_hit=proasis_hit, crystal=crystal, update_field='mtz',
                        filename=str(proasis_hit.mtz)), \
               CopyFile(proasis_hit=proasis_hit, crystal=crystal, update_field='fofc',
                        filename=str(proasis_hit.fofc))

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        return luigi.LocalTarget(str(proasis_hit.pdb_file) + '.proasis.in')

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class GetPanddaMaps(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CopyInputFiles(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                              hit_directory=self.hit_directory)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id))
        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' + mod_date + '.pandda')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal = Crystal.objects.get(pk=self.crystal_id)

        target_name = str(crystal.target.target_name).upper()
        crystal_name = str(crystal.crystal_name)
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        pandda_events = PanddaEvent.objects.filter(crystal=crystal).distinct('pandda_event_map_native')

        for event in pandda_events:
            if not os.path.isdir(proasis_crystal_directory):
                os.makedirs(proasis_crystal_directory)
            shutil.copy(str(event.pandda_event_map_native), proasis_crystal_directory)
            tar_string = '''export GZIP=-9; cd %s; tar -czvf %s.tar.gz %s --remove-files''' % \
                         (proasis_crystal_directory,
                          event.pandda_event_map_native.split('/')[-1],
                          event.pandda_event_map_native.split('/')[-1])

            os.system(tar_string)
            shutil.copy(str(event.pandda_model_pdb), proasis_crystal_directory)

            entry = ProasisPandda.objects.get_or_create(hit=proasis_hit, event=event, crystal=crystal)
            entry[0].event_map_native = os.path.join(proasis_crystal_directory,
                                                     str(str(event.pandda_event_map_native).split('/')[-1] + '.tar.gz'))
            entry[0].model_pdb = os.path.join(proasis_crystal_directory,
                                              str(str(event.pandda_model_pdb).split('/')[-1]))
            entry[0].save()

        with self.output().open('w') as f:
            f.write('')


class GenerateSdf(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GetPanddaMaps(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                             hit_directory=self.hit_directory)

        # GetLigandList(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
        #                  hit_directory=self.hit_directory)

    def output(self):
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        return luigi.LocalTarget(os.path.join(proasis_crystal_directory, str(crystal_name + '.sdf')))

    def run(self):
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name
        if not os.path.isdir(os.path.join(self.hit_directory, target_name, crystal_name, 'input/')):
            os.makedirs(os.path.join(self.hit_directory, target_name, crystal_name, 'input/'))
        # crystal = Crystal.objects.get(pk=self.crystal_id)
        # crystal_name = crystal.crystal_name
        smiles = crystal.compound.smiles
        misc_functions.create_sd_file(crystal_name, smiles, self.output().path)
        proasis_hit = ProasisHits.objects.get(crystal_name=crystal,
                                              refinement=Refinement.objects.get(pk=self.refinement_id))
        proasis_hit.sdf = self.output().path
        proasis_hit.save()


class UploadHit(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GenerateSdf(crystal_id=self.crystal_id, refinement_id=self.refinement_id, hit_directory=self.hit_directory)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id))
        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' + mod_date + '.structure')))

    def run(self):
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id))
        print('LIGANDS: ' + str(proasis_hit.ligand_list))
        unique_ligands = eval(proasis_hit.ligand_list)
        proasis_bound_pdb = proasis_hit.pdb_file

        if len(unique_ligands) == 1:
            lig_string = str(proasis_api_funcs.get_lig_strings(unique_ligands)[0][1:])
            print('submission string:\n')
            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig_string + "' -m " +
                                    str(os.path.join(proasis_crystal_directory, str(crystal_name) + '.sdf')) +
                                    " -p " + str(target_name) + " -t " + str(crystal_name) + " -x XRAY -N")
            print(submit_to_proasis)
            # submit the structure to proasis
            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)

            if err:
                raise Exception(err)
            print(out)


        # same as above, but for structures containing more than one ligand
        elif len(unique_ligands) > 1:
            ligands_list = proasis_api_funcs.get_lig_strings(unique_ligands)
            print(ligands_list)
            lig1 = ligands_list[0][1:]
            lign = " -o '"
            for i in range(1, len(ligands_list) - 1):
                lign += str(ligands_list[i][1:] + ',')
            lign += str(ligands_list[len(ligands_list) - 1] + "'")

            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig1 + "' " + lign + " -m " +
                                    str(os.path.join(proasis_crystal_directory, str(crystal_name) + '.sdf')) +
                                    " -p " + str(target_name) + " -t " + crystal_name + " -x XRAY -N")
            print(submit_to_proasis)

            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)

            if err:
                raise Exception(err)
            print(out)

        # add strucid to database
        proasis_hit.strucid = strucid
        proasis_hit.save()

        if not err:
            with self.output().open('w') as f:
                f.write('')


class AddFiles(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return UploadHit(crystal_id=self.crystal_id, refinement_id=self.refinement_id, hit_directory=self.hit_directory)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id))
        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' + mod_date + '.files')))

    def run(self):

        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id))

        proasis_pandda = ProasisPandda.objects.filter(hit=proasis_hit)

        strucid = proasis_hit.strucid

        out, err = proasis_api_funcs.add_proasis_file(file_type='2fofc_c',
                                                      filename=str(proasis_hit.two_fofc),
                                                      strucid=strucid, title=str(proasis_hit.crystal_name.crystal_name
                                                                                 + '_2fofc'))

        if err:
            raise Exception(err)
        print(out)

        out, err = proasis_api_funcs.add_proasis_file(file_type='fofc_c',
                                                      filename=str(proasis_hit.fofc),
                                                      strucid=strucid, title=str(proasis_hit.crystal_name.crystal_name
                                                                                 + '_fofc'))
        if err:
            raise Exception(err)
        print(out)

        out, err = proasis_api_funcs.add_proasis_file(file_type='mtz',
                                                      filename=str(proasis_hit.mtz),
                                                      strucid=strucid, title=str(proasis_hit.crystal_name.crystal_name
                                                                                 + '_mtz'))
        if err:
            raise Exception(err)
        print(out)

        # TODO: Add this back in at some point. Skip for now as no option for native maps
        # for entry in proasis_pandda:
        #     out, err = proasis_api_funcs.add_proasis_file(file_type='', filename=str(entry.event_map_native),
        #                                                   strucid=strucid, title=str(proasis_hit.crystal.crystal_name
        #                                                                              + '_event_' +
        #                                                                              str(entry.event.event)))

        with self.output().open('w') as f:
            f.write('')


class UploadHits(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        hits = ProasisHits.objects.filter(strucid=None)
        c_id = []
        r_id = []
        for hit in hits:
            c_id.append(hit.crystal_name_id)
            r_id.append(hit.refinement_id)

        return [AddFiles(crystal_id=c, refinement_id=r, hit_directory=self.hit_directory) for (c, r) in zip(c_id, r_id)]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/hits/proasis_hits_%Y%m%d%H.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class WriteBlackLists(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return UploadHits(date=self.date, hit_directory=self.hit_directory)

    def output(self):
        return luigi.LocalTarget('logs/blacklists.done')

    def run(self):
        proposal_dict = {'proposal':[], 'strucids':[], 'fedids':[]}
        fedids = []
        all_strucids = []
        for proposal in Proposals.objects.all():
            feds = proposal.fedids
            print(feds)
            if feds:
                fd=[]
                for fed in str(feds).split(','):
                    print(fed)
                    fd.append(fed)
                    fedids.append(fed)

                strucids = list(filter(None,
                                       [hit.strucid for hit in ProasisHits.objects.filter(
                                           crystal_name__visit__proposal=proposal)]))

                if strucids:

                    all_strucids.extend(strucids)

                    proposal_dict['proposal'].append(proposal.proposal)
                    proposal_dict['fedids'].append(list(set(fd)))
                    proposal_dict['strucids'].append(strucids)

        print(proposal_dict)

        directory_path = '/usr/local/Proasis2/Data/BLACKLIST'
        fedid_list = list(set(fedids))
        for fedid in fedid_list:
            strucid_not_allowed = []
            for i in range(0, len(proposal_dict['fedids'])):
                if fedid not in proposal_dict['fedids'][i]:
                    for strucid in proposal_dict['strucids'][i]:
                        strucid_not_allowed.append(strucid)
            if strucid_not_allowed:
                print(strucid_not_allowed)
                out_file = str(directory_path + '/' + fedid + '.dat')
                with open(out_file, 'w') as writefile:
                    wr = csv.writer(writefile)
                    wr.writerow(strucid_not_allowed)

        with open('/usr/local/Proasis2/Data/BLACKLIST/other_user.dat', 'w') as f:
            wr = csv.writer(f)
            wr.writerow(all_strucids)

        with open(str(directory_path + '/uzw12877.dat'),'w') as f:
            f.write('')

        with self.output().open('w') as f:
            f.write('')

class UpdateField(luigi.Task):
    model = luigi.Parameter()
    field = luigi.Parameter()
    value = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        crystal = self.model.crystal
        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal.crystal_name + '_' +
                                                                       str(crystal.visit.modification_date) +
                                                                       '.' + self.field)))

    def run(self):
        setattr(self.model, self.field, self.value)
        self.model.save()
        if getattr(self.model, self.field) != self.value:
            raise Exception('val not written, will try again!')
        with self.output().open('wb') as f:
            f.write('')


class UpdateOtherFields(luigi.Task):
    def requires(self):
        for p in ProasisPandda.objects.exclude(event_map_native__contains='.tar.gz'):
            if ProasisOut.objects.filter(crystal=p.crystal, ligand=p.event.lig_id).exists():
                o = ProasisOut.objects.get(crystal=p.crystal, ligand=p.event.lig_id)
                yield UpdateField(model=o, field='event', value=str(str('/'.join(
                    p.event_map_native.replace(str(os.path.join(o.root, o.start) + '/'), '').split('/')) + '.tar.gz')))
                mtz_file = o.proasis.mtz.replace(str(os.path.join(o.root, o.start) + '/'), '')
                yield UpdateField(model=o, field='mtz', value=mtz_file)
        for o in ProasisOut.objects.all():
            for f in glob.glob(os.path.join(o.root, o.start, '*.ccp4.tar.gz')):
                if 'donor' in f:
                    yield UpdateField(model=o, field='don', value=f.split('/')[-1])
                if 'acceptor' in f:
                    yield UpdateField(model=o, field='acc', value=f.split('/')[-1])
                if 'apolar' in f:
                    yield UpdateField(model=o, field='lip', value=f.split('/')[-1])



