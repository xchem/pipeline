import datetime
import subprocess
import uuid

from functions.misc_functions import get_mod_date, get_filepath_of_potential_symlink
from setup_django import setup_django

setup_django()

import luigi
import glob
import os
import shutil

from xchem_db.xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig
from luigi_classes.transfer_soakdb import StartTransfers, misc_functions
from utils.custom_output_targets import DjangoTaskTarget
from utils.refinement import RefinementObjectFiles

from fragalysis_api.xcimporter import *


class BatchAlignTargets(luigi.Task):
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        # Fetch a list of targets from the input directory.
        targets = glob.glob(os.path.join(self.input_directory, '*'))
        # targets = [target[0] for target in os.walk(self.input_directory)]
        # For each target decide what to run...
        return [DecideAlignTarget(target=target) for target in targets]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Alignment/aligned_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class DecideAlignTarget(luigi.Task):
    # Target is /inputdir/targetname
    target = luigi.Parameter()
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        base = os.path.basename(self.target)
        staging_dir = os.path.join(self.staging_directory, base)
        t = Target.objects.filter(target_name=base)
        if len(t) == 1:
            rrf = t[0].pl_reduce_reference_frame
            active = t[0].pl_active
            if not active:
                return None
        else:
            rrf = False  # Assume that we aren't wanting to reduce reference frame just yet...
        if not os.path.exists(staging_dir):
            # If staging direct with name does not exist do a big alignment woo.
            return AlignTarget(target=self.target, rrf=rrf)
        else:
            return AlignTargetOBO(target=self.target, rrf=rrf)

    def output(self):
        target_name = self.target.rsplit('/', 1)[1]
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/Decide_aligned_{target_name}' + str(self.date) + '.done'))

    def run(self):
        # Run the sites caller on the output here?
        with self.output().open('w') as f:
            f.write('')


class AlignTarget(luigi.Task):
    # Target is /inputdir/targetname
    target = luigi.Parameter()
    rrf = luigi.Parameter(default=True)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        infile = glob.glob(os.path.join(self.target, '*.pdb'))
        return [UnalignTargetToReference(target=i, rrf=self.rrf) for i in infile]

    def output(self):
        target_name = self.target.rsplit('/', 1)[1]
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/aligned_{target_name}' + str(self.date) + '.done'))

    def run(self):
        target_name = self.target.rsplit('/', 1)[1]
        if self.rrf:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi.sh {self.target} {self.staging_directory} {target_name}'
        else:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi_norrf.sh {self.target} {self.staging_directory} {target_name}'

        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                              executable='/bin/bash')
        try:
            sites_obj = sites.Sites.from_folder(folder=os.path.join(self.staging_directory, target_name),
                                                recalculate=True)
            sites_obj.to_json()
            sites.contextualize_crystal_ligands(folder=os.path.join(self.staging_directory, target_name))
        except:
            pass

        # Cut maps
        try:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/cutmaps_target.sh {target_name} {self.staging_directory}'
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                  executable='/bin/bash')
        except:
            pass

        # Call PLIP
        try:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/makePLIP4Target.sh {os.path.join(self.staging_directory, target)}'
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                  executable='/bin/bash')
        except:
            pass

        with self.output().open('w') as f:
            f.write('')


class AlignTargetOBO(luigi.Task):
    target = luigi.Parameter()
    rrf = luigi.Parameter(default=True)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        base = os.path.basename(self.target)
        staging_dir = os.path.join(self.staging_directory, base)
        # Otherwise curate list of things that need doing...
        aligned_dir = os.path.join(staging_dir, 'aligned')
        infile = glob.glob(os.path.join(self.target, '*.pdb'))
        infile_bases = [os.path.basename(x).replace('.pdb', '') for x in infile]
        staging_files = glob.glob(os.path.join(aligned_dir, '*', '*.mol'))
        staging_bases = [os.path.basename(x).rsplit('_', 1)[0] for x in staging_files]
        not_aligned = list(set(infile_bases) - set(staging_bases))
        check_for_updates = list(set(infile_bases) - set(not_aligned))
        updated = []
        for i in check_for_updates:
            infile_date = [get_mod_date(x) for x in infile if f'{i}.pdb' in x]
            if len(infile_date) == 1:
                infile_date = infile_date[0]
            else:
                raise Exception('Multiple input pdbs with the same name? Somehow?')
            staging_dates = [get_mod_date(get_filepath_of_potential_symlink(x)) for x in staging_files if f'{i}' in x]
            diffs = [int(infile_date) > int(y) for y in staging_dates]
            if any(diffs):
                updated.append(i)

        to_align = list(set(not_aligned).union(set(updated)))
        return [AlignTargetToReference(target=os.path.join(self.input_directory, base, f'{f}.pdb')) for f in to_align]

    def output(self):
        target_name = self.target.rsplit('/', 1)[1]
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/alignedOBO_{target_name}' + str(self.date) + '.done'))

    def run(self):
        try:
            target_name = self.target.rsplit('/', 1)[1]
            sites_obj = sites.Sites.from_folder(folder=os.path.join(self.staging_directory, target_name), recalculate=False)
            sites_obj.to_json()
            sites.contextualize_crystal_ligands(folder=os.path.join(self.staging_directory, target_name))
        except:
            pass
        with self.output().open('w') as f:
            f.write('')


class AlignTargetToReference(luigi.Task):
    worker_timeout = 900
    retry_count = 1
    # Target is /inputdir/targetname.pdb
    target = luigi.Parameter()
    rrf = luigi.Parameter(default=True)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return UnalignTargetToReference(target=self.target)

    def output(self):
        target_name = os.path.basename(self.target).replace('.pdb', '')
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/aligned_{target_name}' + str(self.date) + '.done'))

    # Clean up
    def run(self):
        target_name = os.path.dirname(self.target).rsplit('/', 1)[1]
        # Clean-up tmp and mono folders if previous tasks fail?
        tmpfolder = os.path.join(self.staging_directory, f'tmp{target_name}')
        if os.path.exists(tmpfolder) and os.path.isdir(tmpfolder):
            shutil.rmtree(tmpfolder)
        if self.rrf:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi_single.sh {self.target} {self.staging_directory} {target_name}'
        else:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi_single_norrf.sh {self.target} {self.staging_directory} {target_name}'
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                              executable='/bin/bash')

        # Run ./cutmaps_xtal.sh
        try:
            bn = os.path.basename(self.target).replace('.pdb', '*')
            outpath = os.path.join(self.staging_directory, target_name, 'aligned', bn)
            for fp in glob.glob(outpath):
                pdb = os.path.join(fp, str(os.path.basename(fp)) + '.pdb')
                command = f'/dls/science/groups/i04-1/fragprep/scripts/cutmaps_folder.sh {fp} {pdb}'
                proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                      executable='/bin/bash')
        except:
            pass

        # Run the PLIP.sh on output...
        try:
            mol_root = self.target.replace('.pdb', '')
            command = f'/dls/science/groups/i04-1/fragprep/scripts/makePLIP.sh {mol_root}'
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                  executable='/bin/bash')
        except:
            pass

        with self.output().open('w') as f:
            f.write('')


class UnalignTargetToReference(luigi.Task):
    worker_timeout = 900
    retry_count = 1
    # Target is /inputdir/targetname.pdb
    target = luigi.Parameter()
    rrf = luigi.Parameter(default=True)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    unaligned_directory = luigi.Parameter(default=DirectoriesConfig().unaligned_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return None

    def output(self):
        target_name = os.path.basename(self.target).replace('.pdb', '')
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/unaligned_{target_name}' + str(self.date) + '.done'))

    def run(self):
        target_name = os.path.dirname(self.target).rsplit('/', 1)[1]
        # Clean-up tmp and mono folders if previous tasks fail?
        tmpfolder = os.path.join(self.unaligned_directory, f'tmp{target_name}')
        if os.path.exists(tmpfolder) and os.path.isdir(tmpfolder):
            shutil.rmtree(tmpfolder)

        if self.rrf:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi_unalign.sh {self.target} {self.unaligned_directory} {target_name}'
        else:
            command = f'/dls/science/groups/i04-1/fragprep/scripts/run_fragapi_unalign_norrf.sh {self.target} {self.unaligned_directory} {target_name}'

        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                              executable='/bin/bash')
        # Run ./cutmaps_xtal.sh
        try:
            bn = os.path.basename(self.target).replace('.pdb', '*')
            outpath = os.path.join(self.unaligned_directory, target_name, 'aligned', bn)
            for fp in glob.glob(outpath):
                pdb = os.path.join(fp, str(os.path.basename(fp)) + '.pdb')
                command = f'/dls/science/groups/i04-1/fragprep/scripts/cutmaps_folder.sh {fp} {pdb}'
                proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,executable='/bin/bash')
        except:
            pass

        with self.output().open('w') as f:
            f.write('')
