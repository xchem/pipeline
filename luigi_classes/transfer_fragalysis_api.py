from setup_django import setup_django
from xchem_db import models

setup_django()

import datetime
import luigi

from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig


class BatchTranslateFragalysisAPIOutput(luigi.Task):
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_dir)

    def requires(self):
        staging_folders = [x[0] for x in os.walk(self.staging_directory) if 'aligned' in x[0]]
        # Do each file one at a time!
        return [TranslateFragalysisAPIOutput(target=x) for x in staging_folders]

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
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_dir)
    target = target = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Translation/Translate_' + str(
                                                  os.path.basename(self.target)) + '.done')))

    def run(self):
        # Ensure to only run if data is updated???
        Translate_Files(target=self.target,
                        staging_directory=os.path.join(self.staging_directory, os.path.basename(self.target)),
                        input_directory=os.path.join(self.input_directory, os.path.basename(self.target)),)


def Translate_Files(target, staging_directory, input_directory):
    '''
    target = folder path for particular fragalysis Entry?
    '''

    ligand_name = os.path.basename(target)
    crystal_name = ligand_name.rsplit('_', 1)[0]

    # Create Ligand Object, THIS WONT WORK FOR NON XCDB Crystals! Or Renamed stuff!!!
    crys = models.Crystal.objects.get(crystal_name)
    ligand_entry, created = models.Ligand.objects.get_or_create(crystal=crys,
                                                                target=crys.target,
                                                                compount=crys.compound)
    fragtarget, created = models.FragalysisTarget.objects.get_or_create(target=crys.target,
                                                                        staging_root=staging_directory,
                                                                        input_root=input_directory)
    # Frag Target information is edited post-pipeline?
    ligand_props = {
        'ligand': ligand_entry,
        'fragalysis_target': fragtarget,
        'crystallographic_bound': os.path.join(target, ligand_name, '_bound.pdb'),
        'lig_mol_file': os.path.join(target, ligand_name, '.mol'),
        'apo_pdb': os.path.join(target, ligand_name, '_apo.pdb'),
        'bound_pdb': os.path.join(target, ligand_name, '_bound.pdb'),
        'smiles_file': os.path.join(target, ligand_name, '_smiles.txt'),
        'desolvated_pdb': os.path.join(target, ligand_name, '_apo-desolv.pdb'),
        'solvated_pdb': os.path.join(target, ligand_name, '_apo-solv.pdb'),
        'pandda_event': os.path.join(target, ligand_name, '_event_0.ccp4'),
        'two_fofc': os.path.join(target, ligand_name, '_2fofc.map'),
        'fofc': os.path.join(target, ligand_name, '_fofc.map')
    }

    # fragligand, created = models.FragalysisLigand.objects.update_or_create(ligand=ligand_entry, fragalysis_target=fragtarget, defaults=ligand_props)
    try:
        fragligand = models.FragalysisLigand.objects.get(ligand=ligand_entry,
                                                         fragalysis_target=fragtarget)
        for key, value in ligand_props.items():
            setattr(fragligand, key, value)
        fragligand.save()
    except FragalysisLigand.DoesNotExists:
        fragligand = models.FragalysisLigand.objects.create(**ligand_props)
        fragligand.save()
