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


# from fragalysis_api.pipelines.prep_multi_fragalysis import outlist_from_align, AlignTarget, ProcessAlignedPDB, BatchProcessAlignedPDB, BatchConvertAligned

class BatchRunCreateInputFiles(luigi.Task):
    '''
    Create Input Files for all Crystals in refinement that have outcomes >=4 and <=6 and also use the correct params.

    This class requires/schedules CreateInputFiles to be fun on each crystal in the refinement table that satisfies the condition.
    '''
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)

    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)

    # Should only be run if updated...
    def requires(self):
        # Holy crap.
        return[
            CreateInputFiles(
                crystal=crystal,
                smiles=[Compounds.objects.get(id=x['compound_id']).smiles for x in crystal.crystal_name.crystalcompoundpairs_set.values()],
                prod_smiles=[x['product_smiles'] for x in crystal.crystal_name.crystalcompoundpairs_set.values()],
                hit_directory=self.hit_directory,
                soak_db_filepath=crystal.crystal_name.visit.filename,
                monomer=crystal.crystal_name.target.pl_monomeric,
                outpath=os.path.join(self.input_directory, self.crystal.crystal_name.target.target_name, str(self.crystal.crystal_name.crystal_name + '.pdb'))
            ) for crystal in self.filter_by if misc_functions.compare_dates_to_action(
                sdb_mod=crystal.crystal_name.visit.modification_date,  # Is this the right thing to do???
                pdb_out=misc_functions.get_mod_date(os.path.join(self.input_directory, self.crystal.crystal_name.target.target_name, str(self.crystal.crystal_name.crystal_name + '.pdb')))
            )
        ]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('symboliclinks/transfers_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CreateInputFiles(luigi.Task):
    ssh_command = 'ssh mly94721@ssh.diamond.ac.uk'
    crystal = luigi.Parameter()
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date = luigi.Parameter(default=datetime.datetime.now())
    smiles = luigi.Parameter(default='')
    prod_smiles = luigi.Parameter(default='')
    monomer = luigi.Parameter(default=False)
    outpath = luigi.Parameter()
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)

    def requires(self):
        # Ensure that this task only runs IF the sdb file has been updated otherwise no need right?
        return None

    def output(self):
        # Change this to create a log entry? # Is it not running because of this?
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory, str('symboliclinks/transfers_' + str(self.crystal.crystal_name.crystal_name) + str(self.date) + '.done')))

    def run(self):
        # This shouldn't be scheduled unless soakdbfile has been updated...
        outpath = self.outpath

        try:
            #if not os.path.exists(os.readlink(outpath)):
            #    os.unlink(outpath)
            os.unlink(outpath)
        except FileNotFoundError:
            pass

        if not os.path.isdir('/'.join(outpath.split('/')[:-1])):
            os.makedirs('/'.join(outpath.split('/')[:-1]))

        file_obj = RefinementObjectFiles(refinement_object=self.crystal)
        file_obj.find_bound_file()
        cutmaps = True
        input_pdb = ''

        if file_obj.bound_conf:
            input_pdb = file_obj.bound_conf
        else:
            if file_obj.pdb_latest:
                input_pdb = file_obj.pdb_latest
            else:
                pass
        if input_pdb is '':
            cutmaps = False  # Do Not cut maps.
        else:
            if self.monomer:
                os.symlink(input_pdb, outpath)
            else:
                command = f'/dls/science/groups/i04-1/fragprep/scripts/biomol.sh {input_pdb} {outpath}'
                proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
            # This needs to be zipped up.
            if self.smiles is not '': # should be a [''] maybe who knows..
                smi = '\n'.join([p if p is not None else s for p, s in zip(self.prod_smiles, self.smiles)])
                smi_pth = outpath.replace('.pdb', '_smiles.txt')
                with open(smi_pth, 'w') as f:
                    f.write(str(smi))
        if cutmaps:
            bcdir = os.path.dirname(input_pdb)
            fofc = glob.glob(bcdir + '/fofc.map')
            if len(fofc) < 1:
                bcdir = os.path.dirname(bcdir)
            fofc = glob.glob(bcdir + '/fofc.map')
            fofc2 = glob.glob(bcdir + '/2fofc.map')
            event_maps = glob.glob(bcdir + '/*event*native*.ccp4')  # Does not capture all maps...
            fofc_pth = outpath.replace('.pdb', '_fofc.map')
            fofc2_pth = outpath.replace('.pdb', '_2fofc.map')

            if len(fofc) > 0:
                mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                    border %s
                    end
                eof
                ''' % (fofc[0], fofc_pth, outpath, str(0))
                proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')

            if len(fofc2) > 0:
                mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                    border %s
                    end
                eof
                ''' % (fofc2[0], fofc2_pth, outpath, str(0))
                proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')

            if len(event_maps) > 0:
                event_num = 0
                for i in event_maps:
                    fn = outpath.replace('.pdb', f'_event_{event_num}.ccp4')
                    mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                        border %s
                        end
                    eof
                    ''' % (i, fn, outpath, str(0))
                    proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
                    event_num += 1

        with self.output().open('w') as f:
            f.write('')


class BatchAlignTargets(luigi.Task):
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        # Check list of targets that have staging dirs
        targets = [target[0] for target in os.walk(self.input_directory)]
        # Decide which mode to run.
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
        if not os.path.exists(staging_dir):
            # If staging direct with name does not exist do a big alignment
            return AlignTarget(target=self.target)
        else:
            # If the folder exists:
            # Check if the file exists
            # If the file exists, check the date of the file to whats in the db!
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
                print(infile_date)
                print(staging_dates)
                print(diffs)
                if any(diffs):
                    updated.append(i)

            to_align = list(set(not_aligned).union(set(updated)))
            return [AlignTargetToReference(target=os.path.join(self.input_directory, base, f'{f}.pdb')) for f in to_align]

    def output(self):
        target_name = self.target.rsplit('/', 1)[1]
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/Decide_aligned_{target_name}' + str(self.date) + '.done'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class AlignTarget(luigi.Task):
    # Target is /inputdir/targetname
    target = luigi.Parameter()
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return None

    def output(self):
        target_name = self.target.rsplit('/', 1)[1]
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/aligned_{target_name}' + str(self.date) + '.done'))

    def run(self):
        target_name = self.target.rsplit('/', 1)[1]
        # This is NOT the way to do this Tyler. But I am a noob at python so it'll work...
        os.system(f'/dls/science/groups/i04-1/software/miniconda_3/envs/fragalysis_env2/bin/python /dls/science/groups/i04-1/software/tyler/fragalysis-api/fragalysis_api/xcimporter/xcimporter.py --in_dir={self.target} --out_dir={self.staging_directory} --target {target_name} -m -c')
        with self.output().open('w') as f:
            f.write('')

class AlignTargetToReference(luigi.Task):
    worker_timeout = 600
    retry_count = 1
    # Target is /inputdir/targetname.pdb
    target = luigi.Parameter()
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return None

    def output(self):
        target_name = os.path.basename(self.target).replace('.pdb', '')
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Alignment/aligned_{target_name}' + str(self.date) + '.done'))

    # Clean up
    def run(self):
        target_name = os.path.dirname(self.target).rsplit('/', 1)[1]
        # Clean-up tmp and mono folders if previous tasks fail?
        tmpfolder = os.path.join(self.staging_directory, f'tmp{target_name}')
        monofolder = os.path.join(self.staging_directory, f'mono{target_name}')
        if os.path.exists(tmpfolder) and os.path.isdir(tmpfolder):
            shutil.rmtree(tmpfolder)
        if os.path.exists(monofolder) and os.path.isdir(monofolder):
            shutil.rmtree(monofolder)
        # This is NOT the way to do this Tyler. But I am a noob at python so it'll work...
        command = f'/dls/science/groups/i04-1/software/miniconda_3/envs/fragalysis_env2/bin/python /dls/science/groups/i04-1/software/tyler/fragalysis-api/fragalysis_api/xcimporter/single_import.py --in_file={self.target} --out_dir={self.staging_directory} --target {target_name} -m -c'
        subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                       executable='/bin/bash')
        with self.output().open('w') as f:
            f.write('')


class BatchCutMaps(luigi.Task):
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        stagingfolders = glob.glob(os.path.join(self.staging_directory, '*'))
        stagingfolders = [x for x in stagingfolders if 'tmp' not in x]
        stagingfolders = [x for x in stagingfolders if 'mono' not in x]
        return [CutMaps(target=target) for target in stagingfolders]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Cutting/cut_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CutMaps(luigi.Task):
    target = luigi.Parameter()
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    date = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return None

    def output(self):
        target_name = os.path.basename(self.target)
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              f'Cutting/cut_{target_name}' + '.done'))

    def run(self):
        target_name = os.path.basename(self.target)
        targs = glob.glob(os.path.join(self.staging_directory, target_name, 'aligned', '*'))
        targs = [x for x in targs if 'pdb_file_failures' not in x]

        for i in targs:
            crys = os.path.basename(i)
            maps = glob.glob(os.path.join(i, '*.map')) + glob.glob(os.path.join(i, '*.ccp4'))
            for j in maps:
                fn = j
                mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                    border %s
                    end
                eof
                ''' % (fn, fn, os.path.join(i, f"{crys}.pdb"), str(12))
                print(mapmask)
                proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                      executable='/bin/bash')
                #os.system(f'module load ccp4 && mapmask mapin {j} mapout {j} xyzin {os.path.join(i, f"{crys}.pdb")} << eof\n border 6\n end\n eof')

        with self.output().open('w') as f:
            f.write('')
