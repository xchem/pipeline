import csv
import glob
import os
import re
import shutil
import subprocess

from setup_django import setup_django

setup_django()


import datetime
import luigi
import numpy as np
from Bio.PDB import NeighborSearch, PDBParser, Atom, Residue
from itertools import chain

from functions import misc_functions, db_functions, proasis_api_funcs
from xchem_db.models import *
from . import transfer_soakdb


class InitDBEntries(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_db_%Y%m%d%H.txt'))

    def run(self):
        fail_count = 0
        # select anything 'in refinement' (3) or above
        refinement = Refinement.objects.filter(outcome__gte=4)
        # set up info for each entry that matches the filter
        for obj in refinement:
            # set up blank fields for entries in proasis hits table
            bound_conf = ''
            files = []
            mtz = ''
            two_fofc = ''
            fofc = ''
            mod_date = ''
            proasis_hit_entry = ''
            entry = ''
            confs = []
            ligand_list = []

            # if there is a pdb file named in the bound_conf field, use it as the upload structure for proasis
            if obj.bound_conf:
                if os.path.isfile(obj.bound_conf):
                    bound_conf = obj.bound_conf
            # otherwise, use the most recent pdb file (according to soakdb)
            elif obj.pdb_latest:
                if os.path.isfile(obj.pdb_latest):
                    # if this is from a refinement folder, find the bound-state pdb file, rather than the ensemble
                    if 'Refine' in obj.pdb_latest:
                        search_path = '/'.join(obj.pdb_latest.split('/')[:-1])
                        files = glob.glob(str(search_path + '/refine*split.bound*.pdb'))
                        if len(files) == 1:
                            bound_conf = files[0]
                    else:
                        # if can't find bound state, just use the latest pdb file
                        bound_conf = obj.pdb_latest
            else:
                # no pdb = no proasis upload (same for mtz, two_fofc and fofc)
                # TODO: Turn this into a function instead of repeating file check
                fail_count += 1
                continue

            mtz = db_functions.check_file_status('refine.mtz', bound_conf)
            two_fofc = db_functions.check_file_status('2fofc.map', bound_conf)
            fofc = db_functions.check_file_status('fofc.map', bound_conf)

            if not mtz[0] or not two_fofc[0] or not fofc[0]:
                fail_count += 1
                continue

            # if a suitable pdb file is found, then search for ligands
            if bound_conf:
                try:
                    pdb_file = open(bound_conf, 'r')
                    ligand_list = []
                    for line in pdb_file:
                        # ignore LIG in link for strange phenix format
                        if "LIG" in line and 'LINK' not in line:
                            try:
                                # ligands identified by 'LIG', with preceeding '.' for alt conf letter
                                lig_string = re.search(r".LIG.......", line).group()
                                # just use lig string instead of separating into list items (to handle altconfs)
                                ligand_list.append(lig_string)
                            except:
                                continue
                # if no ligands are found in the pdb file, no upload to proasis (checked that no strucs. had alternative
                # labels in them)
                except:
                    ligand_list = None

            if not ligand_list:
                continue

            # get a unique list of ligands
            unique_ligands = list(set(ligand_list))
            # remove the first letter (alt conf) from unique ligands
            lig_no_conf = [l[1:] for l in unique_ligands]

            for l in lig_no_conf:
                # check whether there are more than 1 entries for any of the lig strings without alt conf
                if lig_no_conf.count(l) > 1:
                    # this is an alt conf situation - add the alt confs to the conf list
                    confs.extend([lig for lig in unique_ligands if l in lig])

            # get the date the pdb file was modified
            mod_date = misc_functions.get_mod_date(bound_conf)

            if mod_date:
                # if there's already an entry for that structure
                if ProasisHits.objects.filter(refinement=obj, crystal_name=obj.crystal_name).exists():
                    # if there are no alternate conformations
                    if not confs:
                        # get the relevant entry
                        entries = ProasisHits.objects.filter(refinement=obj, crystal_name=obj.crystal_name)
                        for entry in entries:
                            # if the pdb file is older than the current one, or it has not been uploaded to proasis
                            if entry.modification_date < mod_date or not entry.strucid:
                                # delete structure and remove files to remove from proasis is strucid exists
                                if entry.strucid:

                                    proasis_out = ProasisOut.objects.filter(proasis=entry)
                                    for o in proasis_out:
                                        out_dir = os.path.join(o.root, o.start)
                                        shutil.rmtree(out_dir)

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

                                # otherwise, just update the relevant fields
                                entry.pdb_file = bound_conf
                                entry.modification_date = mod_date
                                entry.mtz = mtz[1]
                                entry.two_fofc = two_fofc[1]
                                entry.fofc = fofc[1]
                                entry.ligand_list = unique_ligands
                                entry.save()
                    # if there ARE alternate conformations
                    else:
                        # for each conformation
                        for conf in confs:
                            # do the same as above, but setting the altconf field too
                            # TODO: functionalise to add altconfs and not repeat method
                            entry = ProasisHits.objects.get(refinement=obj, crystal_name=obj.crystal_name, altconf=conf)
                            if entry.modification_date < mod_date or not entry.strucid:
                                if entry.strucid:
                                    proasis_out = ProasisOut.objects.filter(proasis=entry)
                                    for o in proasis_out:
                                        out_dir = os.path.join(o.root, o.start)
                                        shutil.rmtree(out_dir)

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
                                entry.altconf = conf
                                entry.save()

                # if there's not already an entry for that structure
                else:
                    # if no altconfs
                    if not confs:
                        # create entry without an altconf
                        ProasisHits.objects.get_or_create(refinement=obj,
                                                          crystal_name=obj.crystal_name,
                                                          pdb_file=bound_conf,
                                                          modification_date=mod_date,
                                                          mtz=mtz[1], two_fofc=two_fofc[1],
                                                          fofc=fofc[1], ligand_list=unique_ligands)
                    # if altconfs
                    if confs:
                        for conf in confs:
                            # create an entry for each altconf
                            # TODO: The pdb file will need to be edited later to pull out other altconfs of the same lig

                            ProasisHits.objects.get_or_create(refinement=obj,
                                                              crystal_name=obj.crystal_name,
                                                              pdb_file=bound_conf,
                                                              modification_date=mod_date,
                                                              mtz=mtz[1], two_fofc=two_fofc[1],
                                                              fofc=fofc[1],
                                                              ligand_list=unique_ligands,
                                                              altconf=conf)

                dimple = Dimple.objects.filter(crystal_name=obj.crystal_name)
                if dimple.count() == 1:
                    if dimple[0].reference and dimple[0].reference.reference_pdb:
                        if os.path.isfile(dimple[0].reference.reference_pdb):
                            ProasisLeads.objects.get_or_create(reference_pdb=dimple[0].reference)
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
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return InitDBEntries(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        self.protein_name = str(self.protein_name).upper()
        return luigi.LocalTarget(str('logs/proasis/' + str(self.protein_name) + '.added'))

    def run(self):
        # use upper case so proasis can differentiate
        self.protein_name = str(self.protein_name).upper()

        # add project to proasis (see: /usr/local/Proasis2/utils/addnewproject.py)
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
    date = luigi.DateParameter(default=datetime.datetime.now())
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
    date = luigi.DateParameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    site_centroids = luigi.Parameter()
    reference_structure = luigi.Parameter()
    target = luigi.Parameter()

    def requires(self):
        return AddProjects(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(str(self.reference_structure + '_lead.proasis'))

    def run(self):
        for centroid in self.site_centroids:
            # print('next centroid')
            structure = PDBParser(PERMISSIVE=0).get_structure(str(self.target).upper(), str(self.reference_structure))

            # initial distance for nearest neighbor (NN) search is 10A
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

                    # establish string spacing for proasis
                    if len(str(parent.get_id()[1])) >= 3:
                        space = ' '
                    if len(str(parent.get_id()[1])) == 2:
                        space = '  '
                    if len(str(parent.get_id()[1])) == 1:
                        space = '   '
                    # ignore waters
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
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        # get all leads that haven't yet been uploaded
        leads = ProasisLeads.objects.filter(strucid=None)
        # set a dict for holding values for run
        out_dict = {'reference': [], 'sites': [], 'targets': []}
        # for each lead
        for lead in leads:
            targets = []
            # get the related dimple info for the reference pdb structure
            dimple = Dimple.objects.filter(reference=lead.reference_pdb)
            # get all crystals related to the reference
            crystals = [dimp.crystal_name for dimp in dimple]
            lead_crystals = ProasisHits.objects.filter(crystal_name__in=crystals)
            # get all targets related to those crystals
            targets = list(set([hit.crystal_name.target.target_name for hit in lead_crystals]))
            # init blank list for sites
            site_list = []
            # for each crystal related to the reference
            for crys in lead_crystals:
                # get all relevant pandda events, and find sites (round to 2 d.p)
                events = PanddaEvent.objects.filter(crystal=crys.crystal_name)
                sites = list(set([(round(event.site.site_native_centroid_x, 2),
                                   round(event.site.site_native_centroid_y, 2),
                                   round(event.site.site_native_centroid_z, 2)) for event in events]))
                if events and sites:
                    for site in sites:
                        site_list.append(site)
                # if sites have been found, add to dict to add lead structure to proasis
                if site_list:
                    if len(targets) == 1:
                        out_dict['targets'].append(targets[0])
                        out_dict['reference'].append(lead.reference_pdb.reference_pdb)
                        out_dict['sites'].append(list(set(site_list)))

        run_zip = zip(out_dict['reference'], out_dict['sites'], out_dict['targets'])

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
        return UploadLeads()

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

        # # check if symlink exists, and remove it if it does
        # if os.path.lexists(os.path.join(proasis_crystal_directory, str(self.filename.split('/')[-1]))):
        #     os.remove(os.path.join(proasis_crystal_directory, str(self.filename.split('/')[-1])))

        # copy the file (symlinking effs up output file)
        shutil.copy(self.filename, os.path.join(proasis_crystal_directory, str(self.filename.split('/')[-1])))

        # update the relevant field in xchem_db
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


class CopyInputFiles(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

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
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return luigi.LocalTarget(str(proasis_hit.pdb_file) + '.proasis.in')

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class GetPanddaMaps(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CopyInputFiles(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                              hit_directory=self.hit_directory, altconf=self.altconf)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)

        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        # if there's an alternate conformation involved, add an extension to identify it
        if self.altconf:
            alt_ext = '_' + str(self.altconf).replace(' ', '')
        else:
            alt_ext = ''

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' +
                                                                       mod_date + alt_ext + '.pandda')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        crystal = Crystal.objects.get(pk=self.crystal_id)

        target_name = str(crystal.target.target_name).upper()
        crystal_name = str(crystal.crystal_name)
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        pandda_events = PanddaEvent.objects.filter(crystal=crystal).distinct('pandda_event_map_native')

        for event in pandda_events:
            if not os.path.isdir(proasis_crystal_directory):
                os.makedirs(proasis_crystal_directory)

            # symlink pandda event map instead of copying - save space
            if os.path.lexists(os.path.join(proasis_crystal_directory,
                                            str(event.pandda_event_map_native).split('/')[-1])):
                os.remove(os.path.join(proasis_crystal_directory, str(event.pandda_event_map_native).split('/')[-1]))
            os.symlink(str(event.pandda_event_map_native),
                       os.path.join(proasis_crystal_directory, str(event.pandda_event_map_native).split('/')[-1]))

            # symlink pandda model instead of copying - save space
            if os.path.lexists(os.path.join(proasis_crystal_directory, str(event.pandda_model_pdb).split('/')[-1])):
                os.remove(os.path.join(proasis_crystal_directory, str(event.pandda_model_pdb).split('/')[-1]))
            os.symlink(str(event.pandda_model_pdb),
                       os.path.join(proasis_crystal_directory, str(event.pandda_model_pdb).split('/')[-1]))

            # update proasis model fields
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
    altconf = luigi.Parameter()

    def requires(self):
        return GetPanddaMaps(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                             hit_directory=self.hit_directory, altconf=self.altconf)

    def output(self):
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        return luigi.LocalTarget(os.path.join(proasis_crystal_directory, str(crystal_name + '.sdf')))

    def run(self):
        # get crystal name and target name from ids provided
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name

        # if the input dirs for the current crystal don't exist, make them
        if not os.path.isdir(os.path.join(self.hit_directory, target_name, crystal_name, 'input/')):
            os.makedirs(os.path.join(self.hit_directory, target_name, crystal_name, 'input/'))

        # take the smiles string for the ligand and create an sdfile (this is the same for altconfs)
        smiles = crystal.compound.smiles
        misc_functions.create_sd_file(crystal_name, smiles, self.output().path)
        proasis_hit = ProasisHits.objects.get(crystal_name=crystal,
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)
        proasis_hit.sdf = self.output().path
        proasis_hit.save()


class UploadHit(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return GenerateSdf(crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                           hit_directory=self.hit_directory, altconf=self.altconf)

    def output(self):
        # get the proasis hit entry
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)

        # get the modification date and crystal name for output file name
        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        # if there's an alternate conformation involved, add an extension to identify it
        if self.altconf:
            alt_ext = '_' + str(self.altconf).replace(' ', '')
        else:
            alt_ext = ''

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' + mod_date + alt_ext +
                                                                       '.structure')))

    def run(self):
        # get crystal and target name from provided ids
        crystal = Crystal.objects.get(pk=self.crystal_id)
        target_name = str(crystal.target.target_name).upper()
        crystal_name = crystal.crystal_name

        # path for input files for proasis
        proasis_crystal_directory = os.path.join(self.hit_directory, target_name, crystal_name, 'input/')

        # the proasis_hit entry
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)

        # eval the list saved in the table to reproduce a python list
        unique_ligands = eval(proasis_hit.ligand_list)
        proasis_bound_pdb = proasis_hit.pdb_file

        # if there's only one ligand in the ligand list, then easy upload; if altconf, is also uploading only one ligand
        if len(unique_ligands) == 1 or self.altconf:
            # set lig string and upload title - changed if altconf, remain same if not
            lig_string = str(unique_ligands[0][1:])
            title = crystal_name

            # if there's an alternate conformation
            # ignore for phenix refinements - weird
            if self.altconf:
                    #and 'PHENIX refinement' not in open(proasis_bound_pdb, 'r').read():
                # set name for altconf pdb file
                altconf_pdb_file = str(proasis_crystal_directory + proasis_bound_pdb.split('/')[-1].replace(
                    '.pdb', str('_' + self.altconf.replace(' ', '') + '.pdb')))

                # remove if exists so not appending to existing file
                if os.path.isfile(altconf_pdb_file):
                    os.remove(altconf_pdb_file)

                # remove the current altconf from the ligands list
                ligands = [lig for lig in unique_ligands if lig != self.altconf]

                # remove ligands that are not the altconf from the bound state pdb and save as new pdb for upload
                for line in open(proasis_bound_pdb, 'r'):
                    if any(lig in line for lig in ligands):
                        continue
                    else:
                        # get rid of preceeding altconf letter to make sure correct format for proasis
                        if self.altconf in line:
                            line = line.replace(self.altconf, str(' ' + self.altconf[1:]))

                        with open(altconf_pdb_file, 'a') as f:
                            f.write(line)

                lig_string = self.altconf[1:]
                proasis_bound_pdb = altconf_pdb_file
                title = str(crystal_name + '_alt_' + self.altconf.replace(' ', ''))

            elif unique_ligands[0][1:][0] != ' ':
                lig = unique_ligands[0]
                newlines = ''
                for line in open(proasis_bound_pdb, 'r'):
                    if lig in line:
                        line = line.replace(lig, str(' ' + lig[1:]))
                    newlines += line

                with open(proasis_bound_pdb.replace('.pdb', '_proasis.pdb'), 'w') as f:
                    f.write(newlines)

                proasis_bound_pdb = proasis_bound_pdb.replace('.pdb', '_proasis.pdb')

            # see /usr/local/Proasis2/utils/submitStructure.py for explanation
            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig_string + "' -m " +
                                    str(os.path.join(proasis_crystal_directory, str(crystal_name) + '.sdf')) +
                                    " -p " + str(target_name) + " -t " + str(title) + " -x XRAY -N")
            print(submit_to_proasis)
            # submit the structure to proasis
            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)

            if 'strucid' not in out:
                raise Exception(str(submit_to_proasis + '\n' + out))
            print(out)

        # same as above, but for structures containing more than one ligand
        elif len(unique_ligands) > 1 and not self.altconf:
            lig1 = unique_ligands[0][1:]
            lign = " -o '"
            for i in range(1, len(unique_ligands) - 1):
                lign += str(unique_ligands[i][1:] + ',')
            lign += str(unique_ligands[len(unique_ligands) - 1][1:] + "'")

            submit_to_proasis = str("/usr/local/Proasis2/utils/submitStructure.py -d 'admin' -f " + "'" +
                                    str(proasis_bound_pdb) + "' -l '" + lig1 + "' " + lign + " -m " +
                                    str(os.path.join(proasis_crystal_directory, str(crystal_name) + '.sdf')) +
                                    " -p " + str(target_name) + " -t " + crystal_name + " -x XRAY -N")
            print(submit_to_proasis)

            strucid, err, out = proasis_api_funcs.submit_proasis_job_string(submit_to_proasis)

            if 'strucid' not in out:
                raise Exception(str(submit_to_proasis + '\n' + out))
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
    altconf = luigi.Parameter()

    def requires(self):
        return UploadHit(crystal_id=self.crystal_id, refinement_id=self.refinement_id, hit_directory=self.hit_directory,
                         altconf=self.altconf)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)

        mod_date = str(proasis_hit.modification_date)
        crystal_name = str(proasis_hit.crystal_name.crystal_name)

        # if there's an alternate conformation involved, add an extension to identify it
        if self.altconf:
            alt_ext = '_' + str(self.altconf).replace(' ', '')
        else:
            alt_ext = ''

        return luigi.LocalTarget(os.path.join('logs/proasis/hits', str(crystal_name + '_' + mod_date + alt_ext +
                                                                       '.files')))

    def run(self):

        proasis_hit = ProasisHits.objects.get(crystal_name=Crystal.objects.get(pk=self.crystal_id),
                                              refinement=Refinement.objects.get(pk=self.refinement_id),
                                              altconf=self.altconf)

        # proasis_pandda = ProasisPandda.objects.filter(hit=proasis_hit)

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
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        hits1 = ProasisHits.objects.filter(strucid=None)
        hits2 = ProasisHits.objects.filter(strucid='')
        hits = chain(hits1, hits2)
        c_id = []
        r_id = []
        a = []

        for hit in hits:
            c_id.append(hit.crystal_name_id)
            r_id.append(hit.refinement_id)
            a.append(hit.altconf)

        return [AddFiles(crystal_id=c, refinement_id=r, hit_directory=self.hit_directory, altconf=alt) for
                (c, r, alt) in zip(c_id, r_id, a)]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/hits/proasis_hits_%Y%m%d%H.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class WriteBlackLists(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')

    def requires(self):
        return UploadHits(date=self.date, hit_directory=self.hit_directory)

    def output(self):
        return luigi.LocalTarget('logs/blacklists.done')

    def run(self):
        proposal_dict = {'proposal': [], 'strucids': [], 'fedids': []}
        fedids = []
        all_strucids = []
        for proposal in Proposals.objects.all():
            feds = proposal.fedids
            print(feds)
            if feds:
                fd = []
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

        with open(str(directory_path + '/uzw12877.dat'), 'w') as f:
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
            for f in glob.glob(os.path.join(o.root, o.start, '*.ccp4.gz')):
                if 'donor' in f:
                    yield UpdateField(model=o, field='don', value=f.split('/')[-1])
                if 'acceptor' in f:
                    yield UpdateField(model=o, field='acc', value=f.split('/')[-1])
                if 'apolar' in f:
                    yield UpdateField(model=o, field='lip', value=f.split('/')[-1])



