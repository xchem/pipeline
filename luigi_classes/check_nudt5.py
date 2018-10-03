import glob
import os
import re
import setup_django
setup_django.setup_django()

from functions import db_functions
from functions import misc_functions
from xchem_db.models import *

fail_count = 0
refinement = Refinement.objects.filter(outcome__gte=4, crystal_name__target__target_name='NS3Hel')
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
                print(search_path)
                files = glob.glob(str(search_path + '/refine*split.bound*.pdb'))
                print(files)
                if len(files) == 1:
                    bound_conf = files[0]
            else:
                # if can't find bound state, just use the latest pdb file
                bound_conf = obj.pdb_latest

    else:
        # no pdb = no proasis upload (same for mtz, two_fofc and fofc)
        # TODO: Turn this into a function instead of repeating file check
        fail_count += 1
        print('FAILURE FOR: ' + str(obj.crystal_name.crystal_name))
        print('no pdb = no proasis upload (same for mtz, two_fofc and fofc)')
        continue

    print(bound_conf)

    if 'split.bound-state' in bound_conf:

        mtz = db_functions.check_file_status('refine.mtz', obj.pdb_latest)
        two_fofc = db_functions.check_file_status('2fofc.map', obj.pdb_latest)
        fofc = db_functions.check_file_status('fofc.map', obj.pdb_latest)

    else:
        mtz = db_functions.check_file_status('refine.mtz', bound_conf)
        two_fofc = db_functions.check_file_status('2fofc.map', bound_conf)
        fofc = db_functions.check_file_status('fofc.map', bound_conf)

    if not mtz[0] or not two_fofc[0] or not fofc[0]:
        fail_count += 1
        print('FAILURE FOR: ' + str(obj.crystal_name.crystal_name))
        print('missing file')
        print(bound_conf)
        print(mtz)
        print(two_fofc)
        print(fofc)
        continue

    # if a suitable pdb file is found, then search for ligands
    if bound_conf:
        try:
            pdb_file = open(bound_conf, 'r')
            ligand_list = []
            for line in pdb_file:
                if "LIG" in line:
                    try:
                        # ligands identified by 'LIG', with preceeding '.' for alt conf letter
                        lig_string = re.search(r".LIG.......", line).group()
                        # just use lig string instead of separating into list items (to handle altconfs)
                        # TODO: This has changed from a list of ['LIG','RES','ID'] to string. Check usage
                        ligand_list.append(lig_string)
                    except:

                        continue
        # if no ligands are found in the pdb file, no upload to proasis (checked that no strucs. had alternative
        # labels in them)
        except:
            ligand_list = None

    if not ligand_list:
        print('FAILURE FOR: ' + str(obj.crystal_name.crystal_name))
        print('ligands not found')
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

        if not confs:
            # create entry without an altconf
            proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj,
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

                proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj,
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
                    proasis_lead_entry = ProasisLeads.objects.get_or_create(reference_pdb=dimple[0].reference)
                else:
                    if ProasisLeads.objects.filter(reference_pdb=dimple[0].reference).exists():
                        print('removing...')
                        proasis_lead_entry = ProasisLeads.objects.get(reference_pdb=dimple[0].reference)
                        proasis_lead_entry.delete()

    else:
        print('FAILURE FOR: ' + str(obj.crystal_name.crystal_name))
        print('mod date not found')

print(fail_count)
