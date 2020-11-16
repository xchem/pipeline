import datetime
import subprocess
import uuid
from sqlite3 import OperationalError
from setup_django import setup_django
from xchem_db import models

setup_django()

import luigi
import glob
import os
from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig
from luigi_classes.transfer_soakdb import StartTransfers
from utils.custom_output_targets import DjangoTaskTarget
from utils.refinement import RefinementObjectFiles

import datetime
import luigi

from functions.luigi_transfer_soakdb_functions import *
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
        Translate_Files(target=self.target)


def Translate_Files(target, staging_directory, input_directory):
    '''
    target = folder path for particular fragalysis Entry?
    '''

    ligand_name = os.path.basename(target)
    crystal_name = ligand_name.rsplit('_', 1)[0]
    crys = models.Crystal.objects.get(crystal_name)
    # Create Ligand Object
    ligand_entry, created = models.Ligand.objects.get_or_create(crystal=crys,
                                                                target=crys.target,
                                                                compount=crys.compound)
    fragtarget, created = models.FragalysisTarget.objects.get_or_create(target=crys.target, staging_root=staging_directory, input_directory)



    props = {
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

    # fragligand, created = models.FragalysisLigand.objects.update_or_create(ligand=ligand_entry, fragalysis_target=fragtarget, defaults=props)

    try:
        fragligand = models.FragalysisLigand.objects.get(ligand=ligand_entry,
                                                         fragalysis_target=fragtarget)
        for key, value in props.items():
            setattr(fragligand, key, value)
        fragligand.save()
    except FragalysisLigand.DoesNotExists:
        fragligand = models.FragalysisLigand.objects.create(**props)



    class FragalysisLigand(models.Model):
        ligand = models.ForeignKey(Ligand, on_delete=models.CASCADE)
        fragalysis_target = models.ForeignKey(FragalysisTarget, on_delete=models.CASCADE)
        crystallographic_bound = models.FileField()
        lig_mol_file = models.FileField()
        apo_pdb = models.FileField()
        bound_pdb = models.FileField()
        smiles_file = models.FileField()
        desolvated_pdb = models.FileField()
        solvated_pdb = models.FileField()
        # do we really want them all in fragalysis?
        pandda_event = models.FileField()
        two_fofc = models.FileField()
        fofc = models.FileField()




