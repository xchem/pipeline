import datetime
import uuid

from setup_django import setup_django

setup_django()

import luigi
from xchem_db.models import *
from .config_classes import SoakDBConfig, DirectoriesConfig
from luigi_classes.transfer_soakdb import StartTransfers
from utils.custom_output_targets import DjangoTaskTarget
from utils.refinement import RefinementObjectFiles


# from fragalysis_api.pipelines.prep_multi_fragalysis import outlist_from_align, AlignTarget, ProcessAlignedPDB, BatchProcessAlignedPDB, BatchConvertAligned


# Use this to generate fragalysis input + ligand stuff
# 1) After data is in XCDB, take all 4-6 structures and run them through fragalysis api outputting to staging/proteinname
# 2) Scour the staging directories for filenames, creating ligand and meta table entries with them. 
# 3) Cry

class BatchCreateSymbolicLinks(luigi.Task):
    resources = {'django': 1}
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)

    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)

    def requires(self):
        return [
            CreateSymbolicLinks(
                crystal=crystal,
                smiles=crystal.crystal_name.compound.smiles,
                prod_smiles=crystal.crystal_name.product,
                hit_directory=self.hit_directory,
                soak_db_filepath=self.soak_db_filepath
            )
            for crystal in self.filter_by
        ]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('symboliclinks/transfers_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class CreateSymbolicLinks(luigi.Task):
    crystal = luigi.Parameter()
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    date = luigi.Parameter(default=datetime.datetime.now())
    smiles = luigi.Parameter(default=None)
    prod_smiles = luigi.Parameter(default=None)
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)

    def requires(self):
        # Ensure that this task only runs IF the sdb file has been updated otherwise no need right?
        return None

    def output(self):
        # Change this to create a log entry?
        pth = os.path.join(self.input_directory,
                           self.crystal.crystal_name.target.target_name,
                           str(self.crystal.crystal_name.crystal_name + '.pdb'))
        return luigi.LocalTarget(pth)

    def run(self):
        try:
            if not os.path.exists(os.readlink(self.output().path)):
                os.unlink(self.output().path)
        except FileNotFoundError:
            pass

        if not os.path.isdir('/'.join(self.output().path.split('/')[:-1])):
            os.makedirs('/'.join(self.output().path.split('/')[:-1]))

        file_obj = RefinementObjectFiles(refinement_object=self.crystal)
        file_obj.find_bound_file()

        if file_obj.bound_conf:
            try:
                os.symlink(file_obj.bound_conf, self.output().path)
                if self.prod_smiles:
                    smi = self.prod_smiles
                elif self.smiles:
                    smi = self.smiles
                #                 if self.smiles:
                smi_pth = self.output().path.replace('.pdb', '_smiles.txt')
                with open(smi_pth, 'w') as f:
                    f.write(str(smi))
                #  f.close() should delete.
                # Try to create symlinks for the eventmap, 2fofc and fofc
                # Get root of file_obj.bound_conf
                bcdir = os.path.dirname(file_obj.bound_conf)
                # Check if this is the correct directory (most likely not)
                fofc = glob.glob(bcdir+'fofc.map')
                if len(fofc) < 1:
                    # go one deeper!
                    bcdir = os.path.dirname(bcdir)

                # Get the files
                fofc = glob.glob(bcdir + 'fofc.map')
                fofc2 = glob.glob(bcdir + '2fofc.map')
                event_maps = glob.glob(bcdir + '*event*native*.ccp4') # nice doesn't capture all of it though...
                fofc_pth = self.output().path.replace('.pdb', '_fofc.map')
                fofc2_pth = self.output().path.replace('.pdb', '_2fofc.map')
                os.symlink(fofc, fofc_pth)
                os.symlink(fofc2, fofc2_pth)

                # probably should use enumerate
                event_num = 0
                for i in event_maps:
                    os.symlink(i, self.output().path.replace('.pdb', f'event_{event_num}.ccp4'))
                    event_num += 1

            except:
                raise Exception(file_obj.bound_conf)
        else:
            self.crystal.outcome = 3
            self.crystal.save()


class BatchAlignTargets(luigi.Task):
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        targets = [target[0] for target in os.walk(self.input_directory)]
        print(targets)
        return [AlignTarget(target=target) for target in targets]

    def output(self):
        return luigi.LocalTarget(os.path.join(DirectoriesConfig().log_directory,
                                              str('Alignment/aligned_' + str(self.date) + '.done')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class AlignTarget(luigi.Task):
    # Need to account for -m argument?
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
        os.system(f'rm -rf {os.path.join(self.staging_directory, "tmp", "*")}')
        os.system(f'rm -rf {os.path.join(self.staging_directory, "mono", "*")}')
        # This is NOT the way to do this Tyler. But I am a noob at python so it'll work...
        os.system(
            f'/dls/science/groups/i04-1/software/miniconda_3/envs/pipeline/bin/python /dls/science/groups/i04-1/fragprep/fragalysis-api/fragalysis_api/xcimporter/xcimporter.py --in_dir={self.target} --out_dir={self.staging_directory} --target {target_name}')
        with self.output().open('w') as f:
            f.write('')




# class AlignTargetXChem(AlignTarget):
#    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)
#    resources = {'django': 1}
#    date = luigi.Parameter(default=datetime.datetime.now())
#    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
#    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
#    uuid = str(uuid.uuid4())
#    input_dir = luigi.Parameter(default=DirectoriesConfig().fraginput_directory)
#    output_dir = luigi.Parameter(default=DirectoriesConfig().fragstaging_directory)

#    def requires(self):
#        return BatchSymlinkBoundPDB(filter_by=self.filter_by,
#                                    resources=self.resources,
#                                    date=self.date,
#                                    soak_db_filepath=self.soak_db_filepath,
#                                    hit_directory=self.hit_directory(),
#                                    uuid=self.uuid)


# class ProcessAlignedPDBXChem(ProcessAlignedPDB):
#    def requires(self):
#        return AlignTargetXChem(input_dir=self.input_dir, output_dir=self.output_dir)


# class BatchProcessAlignedPDBXChem(BatchProcessAlignedPDB):
#    def requires(self):
#        self.aligned_list = outlist_from_align(self.input_dir, self.output_dir)
#        return [
#            ProcessAlignedPDBXChem(
#                target_name=self.target_name, input_file=i, output_dir=self.output_dir,
#                input_dir=self.input_dir
#            )
#            for i in self.aligned_list
#        ]

# class BatchConvertAlignedXChem(BatchConvertAligned):
#    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)
#    resources = {'django': 1}
#    date = luigi.Parameter(default=datetime.datetime.now())
#    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
#    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
#    uuid = str(uuid.uuid4())

#    search_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
#    output_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
#    def requires(self):
#        if os.path.isfile(self.output().path):
#            os.remove(self.output().path)
#        in_lst = [os.path.abspath(f.path) for f in os.scandir(self.search_directory) if f.is_dir()]
#        out_lst = []
#        target_names = []

#        for f in in_lst:
#            out = os.path.join(os.path.abspath(self.output_directory), f.split('/')[-1])
#            out_lst.append(out)
#            target_names.append(f.split('/')[-1])

#        return [BatchProcessAlignedPDBXChem(input_dir=i, output_dir=self.output_directory, target_name=t)
#                for (i, t) in list(zip(in_lst, target_names))]


# class SymlinkBoundPDB(luigi.Task):
#    crystal = luigi.Parameter()
#    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
#    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
#    date = luigi.Parameter(default=datetime.datetime.now())
#    smiles = luigi.Parameter(default=None)
#    prod_smiles = luigi.Parameter(default=None)

#    def requires(self):
#        # return StartTransfers()
#        return None


#    def output(self):
#        pth = os.path.join(self.hit_directory,
#                           self.crystal.crystal_name.target.target_name,
#                          str(self.crystal.crystal_name.crystal_name + '.pdb'))
#        return luigi.LocalTarget(pth)

#    def run(self):
#        try:
#            if not os.path.exists(os.readlink(self.output().path)):
#                os.unlink(self.output().path)
#        except FileNotFoundError:
#            pass
#        if not os.path.isdir('/'.join(self.output().path.split('/')[:-1])):
#            os.makedirs('/'.join(self.output().path.split('/')[:-1]))
#        file_obj = RefinementObjectFiles(refinement_object=self.crystal)
#        file_obj.find_bound_file()
#        if file_obj.bound_conf:
#            try:

#                os.symlink(file_obj.bound_conf, self.output().path)
#                if self.prod_smiles:
#                    smi = self.prod_smiles
#                elif self.smiles:
#                    smi = self.smiles
#                 if self.smiles:
#                smi_pth = self.output().path.replace('.pdb', '_smiles.txt')
#                with open(smi_pth, 'w') as f:
#                    f.write(str(smi))
#                f.close()

#            except:
#                raise Exception(file_obj.bound_conf)
#        else:
#            self.crystal.outcome = 3
#            self.crystal.save()


# class BatchSymlinkBoundPDB(luigi.Task):
#    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)
#    resources = {'django': 1}
#    date = luigi.Parameter(default=datetime.datetime.now())
#    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
#    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
#    uuid = str(uuid.uuid4())

#    def requires(self):
#        return [
#            SymlinkBoundPDB(
#                crystal=crystal,
#                smiles=crystal.crystal_name.compound.smiles,
#                prod_smiles=crystal.crystal_name.product,
#                hit_directory=self.hit_directory,
#                soak_db_filepath=self.soak_db_filepath
#            )
#            for crystal in self.filter_by
#        ]

#    def output(self):
#        return DjangoTaskTarget(class_name=self.__class__.__name__, uuid=self.uuid)

#    def run(self):
#        Tasks.objects.create(task_name=self.__class__.__name__, uuid=self.uuid)
