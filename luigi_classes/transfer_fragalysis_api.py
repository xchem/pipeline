import glob

from functions import misc_functions
from setup_django import setup_django

setup_django()

import datetime
import luigi
import re
import os

from xchem_db import models
from django.core.exceptions import ObjectDoesNotExist
from .config_classes import SoakDBConfig, DirectoriesConfig


class BatchTranslateFragalysisAPIOutput(luigi.Task):
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)

    def requires(self):
        # Honestly do not know how slow this is haha...
        staging_folders = [x[0] for x in os.walk(self.staging_directory) if 'aligned' in x[0]]
        folders_containing_mols = [x for x in staging_folders if len(glob.glob(os.path.join(x, '*.mol'))) > 0]
        # Check Modification date to fire off!!
        return [TranslateFragalysisAPIOutput(target=x) for x in folders_containing_mols if compare_mod_date(glob.glob(os.path.join(x, '*.mol'))[0])]
        #return [TranslateFragalysisAPIOutput(target=x) for x in folders_containing_mols]  # if compare_mod_date(glob.glob(os.path.join(x, '*.mol'))[0])]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Translation/BatchTranslate_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class TranslateFragalysisAPIOutput(luigi.Task):
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    target = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Translation/Translate_' + str(
                                                  os.path.basename(self.target)) + '.done')))

    def run(self):
        split = self.target.split('/')
        target_name = split[split.index('aligned') - 1]
        # Do each file one at a time!
        # Ensure to only run if data is updated???
        Translate_Files(fragment_abs_dirname=self.target,
                        target_name=target_name,
                        staging_directory=os.path.join(self.staging_directory, target_name),
                        input_directory=os.path.join(self.input_directory, target_name)
                        )
        with self.output().open('w') as f:
            f.write('')


def compare_mod_date(molfile):
    new_date = misc_functions.get_mod_date(molfile)
    if new_date is 'None':
        # Cannot resolve mod date, do not process!
        return False

    ligand_name = os.path.basename(molfile).replace('.mol', '')
    target = ligand_name.rsplit('_', 1)[0]
    try:
        frag_target = models.FragalysisTarget.objects.get(target=target)
    except models.FragalysisTarget.DoesNotExist:
        print(f'{target} is a new Fragalysis Target')
        return True

    try:
        frag_ligand = models.FragalysisLigand.objects.get(ligand=ligand_name, fragalysis_target=frag_target)
    except models.FragalysisLigand.DoesNotExist:
        print(f'{ligand_name} is a new Ligand for {target}')
        return True

    old_date = frag_ligand.modification_date
    return int(new_date) > int(old_date)

def Translate_Files(fragment_abs_dirname, target_name, staging_directory, input_directory):
    '''
    fragment_abs_dirname = folder path for particular fragalysis Entry
    target_name = Name of the Crystal system: e.g. 70X or Mpro

    '''
    # Should be target_name_[0-9]{1}[A-Z]{1}
    ligand_name = os.path.basename(fragment_abs_dirname)

    # Should be /staging_directory/target_name/aligned/
    # target = os.path.dirname(fragment_abs_dirname)

    # Should be prefix of ligand_name e.g. 70x-x0001_0A would be 70x-x0001
    crystal_name = ligand_name.rsplit('_', 1)[0]

    # Get or Create FragTarget
    try:
        frag_target = models.FragalysisTarget.objects.get(target=target_name)
    except models.FragalysisTarget.DoesNotExist:
        frag_target = models.FragalysisTarget.objects.create(
            open=True,
            target=target_name,
            staging_root=staging_directory,
            input_root=input_directory
        )
        frag_target.save()

    mod_date = misc_functions.get_mod_date(os.path.join(fragment_abs_dirname, f'{ligand_name}.mol'))
    if mod_date is 'None':
        mod_date = 0
    # Frag Target information is edited post-pipeline?
    # Should test all paths to makesure they exist otherwise set to None?
    ligand_props = {
        'ligand_name': ligand_name,
        'fragalysis_target': frag_target,
        'crystallographic_bound': os.path.join(fragment_abs_dirname, f'{ligand_name}_bound.pdb'),
        'lig_mol_file': os.path.join(fragment_abs_dirname, f'{ligand_name}.mol'),
        'apo_pdb': os.path.join(fragment_abs_dirname, f'{ligand_name}_apo.pdb'),
        'bound_pdb': os.path.join(fragment_abs_dirname, f'{ligand_name}.pdb'),
        'smiles_file': os.path.join(fragment_abs_dirname, f'{ligand_name}_smiles.txt'),
        'desolvated_pdb': os.path.join(fragment_abs_dirname, f'{ligand_name}_apo-desolv.pdb'),
        'solvated_pdb': os.path.join(fragment_abs_dirname, f'{ligand_name}_apo-solv.pdb'),
        'pandda_event': os.path.join(fragment_abs_dirname, f'{ligand_name}_event_0.ccp4'),
        'two_fofc': os.path.join(fragment_abs_dirname, f'{ligand_name}_2fofc.map'),
        'fofc': os.path.join(fragment_abs_dirname, f'{ligand_name}_fofc.map'),
        'modification_date': int(mod_date)
    }
    try:
        frag_ligand = models.FragalysisLigand.objects.get(ligand_name=ligand_name,
                                                          fragalysis_target=frag_target)
        for key, value in ligand_props.items():
            print(key)
            print(value)
            setattr(frag_ligand, key, value)

        frag_ligand.save()
    except models.FragalysisLigand.DoesNotExist:
        print('Creating Fragalysis Ligand')
        print(ligand_props)
        frag_ligand = models.FragalysisLigand.objects.create(**ligand_props)  # Does this EVEN work?
        frag_ligand.save()

    # Bonza, now link frag_ligand to ligand table for internal stuff.
    path = os.path.join(input_directory, f'{crystal_name}.pdb')
    # Current Fix, need to figure out a way to derive visit number from sample for stuff.
    #try:
    #    path = os.readlink(symlink)
    #except OSError:
    #    # Exit out
    #    print('Ligand is directly deposited into input directory, no known reference crystal')
    #    return None
    #visit = re.findall('[a-z]{2}[0-9]{5}-[0-9]*', path)[0]
    crys = models.Crystal.objects.filter(crystal_name=crystal_name)#.filter(visit__visit=visit)  # This should only return one thing...
    if len(crys) > 1:
        try:
            raise Exception(ligand_name, path, crystal_name, crys)
        except Exception as e:
            bad_ligname, path, bad_crystal_name, bad_crys = e.args
            print(bad_ligname)
            print(path)
            print(bad_crystal_name)
            print(bad_crys)
            print(bad_crys.values())

    elif len(crys) == 1:
        crystal = crys[0]
        ligand_entry, created = models.Ligand.objects.get_or_create(
            fragalysis_ligand=frag_ligand,
            crystal=crystal,
            target=crystal.target,
            compound=crystal.compound
        )
        if created:
            print('Created New Ligand Entry!')
    else:
        print(f'No base Crystal entry for {ligand_name}, will not be linked')

