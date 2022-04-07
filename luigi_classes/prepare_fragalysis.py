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

    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6).filter(crystal_name__target__pl_active=1) # Only look at data that is active

    # Should only be run if updated...
    def requires(self):
        # Holy crap.
        return [
            CreateInputFiles(
                crystal=crystal,
                smiles=[Compounds.objects.get(id=x['compound_id']).smiles for x in
                        crystal.crystal_name.crystalcompoundpairs_set.values()],
                prod_smiles=[x['product_smiles'] for x in crystal.crystal_name.crystalcompoundpairs_set.values()],
                hit_directory=self.hit_directory,
                soak_db_filepath=crystal.crystal_name.visit.filename,
                monomer=crystal.crystal_name.target.pl_monomeric,
                outpath=os.path.join(self.input_directory, crystal.crystal_name.target.target_name,
                                     str(crystal.crystal_name.crystal_name + '.pdb'))
            ) for crystal in self.filter_by if misc_functions.compare_dates_to_action(
                sdb_mod=crystal.crystal_name.visit.modification_date,  # Is this the right thing to do???
                pdb_out=misc_functions.get_mod_date(
                    os.path.join(self.input_directory, crystal.crystal_name.target.target_name,
                                 str(crystal.crystal_name.crystal_name + '.pdb')))
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
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory, str(
            'symboliclinks/transfers_' + str(self.crystal.crystal_name.crystal_name) + str(self.date) + '.done')))

    def run(self):
        # This shouldn't be scheduled unless soakdbfile has been updated...
        outpath = self.outpath

        if not os.path.isdir('/'.join(outpath.split('/')[:-1])):
            os.makedirs('/'.join(outpath.split('/')[:-1]))

        file_obj = RefinementObjectFiles(refinement_object=self.crystal)
        file_obj.find_bound_file()
        cutmaps = True  # Disable this for a moment for testing purpose...

        if file_obj.bound_conf:
            input_pdb = file_obj.bound_conf
        else:
            with self.output().open('w') as f:
                f.write('')
            return ''  # Just leave...

        if os.path.exists(outpath):
            old = get_mod_date(get_filepath_of_potential_symlink(outpath))
            new = get_mod_date(input_pdb)
            process = int(new) > int(old)
        else:
            process = True
        if os.path.exists(outpath) and process:
            os.unlink(outpath)
        if not process:
            cutmaps = False  # Do Not cut maps.
        else:
            if self.monomer:
                command = f'cp {input_pdb} {outpath}'
            else:
                command = f'/dls/science/groups/i04-1/fragprep/scripts/biomol.sh {input_pdb} {outpath}'

            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
            # This needs to be zipped up.
            if self.smiles is not '':  # should be a [''] maybe who knows..
                smi = '\n'.join([s if p in ['None', None, ''] else p for p, s in zip(self.prod_smiles, self.smiles)])
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
                proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                      executable='/bin/bash')

            if len(fofc2) > 0:
                mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                    border %s
                    end
                eof
                ''' % (fofc2[0], fofc2_pth, outpath, str(0))
                proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                      executable='/bin/bash')

            if len(event_maps) > 0:
                event_num = 0
                for i in event_maps:
                    fn = outpath.replace('.pdb', f'_event_{event_num}.ccp4')
                    mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                        border %s
                        end
                    eof
                    ''' % (i, fn, outpath, str(0))
                    proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                          executable='/bin/bash')
                    event_num += 1

        with self.output().open('w') as f:
            f.write('')
