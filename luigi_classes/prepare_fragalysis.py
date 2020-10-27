import datetime
import subprocess
import uuid

from setup_django import setup_django

setup_django()

import luigi
import glob
import os
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
    ssh_command = 'ssh mly94721@ssh.diamond.ac.uk'
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
                # Try to create symlinks for the eventmap, 2fofc and fofc
                # Get root of file_obj.bound_conf
                bcdir = os.path.dirname(file_obj.bound_conf)
                print(bcdir)
                # Check if this is the correct directory (most likely not)
                fofc = glob.glob(bcdir+'/fofc.map')
                if len(fofc) < 1:
                    # go one deeper!
                    bcdir = os.path.dirname(bcdir)

                # Get the files
                fofc = glob.glob(bcdir + '/fofc.map')
                print(fofc)
                fofc2 = glob.glob(bcdir + '/2fofc.map')
                print(fofc2)
                event_maps = glob.glob(bcdir + '/*event*native*.ccp4') # nice doesn't capture all of it though...
                print(event_maps)
                fofc_pth = self.output().path.replace('.pdb', '_fofc.map')
                print(fofc_pth)
                fofc2_pth = self.output().path.replace('.pdb', '_2fofc.map')
                print(fofc2_pth)

                # Assumption only one file to use....
                if len(fofc) > 0:
                    mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                        border %s
                        end
                    eof
                    ''' % (fofc[0], fofc_pth, self.output().path, str(0))
                    print(mapmask)
                    proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                          executable='/bin/bash')
                    #process = subprocess.Popen(
                    #    str(self.ssh_command + ' "' + mapmask + '"'),
                    #    shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    #out, err = process.communicate()
                    #if '(mapmask) - normal termination' not in out:
                    #    raise Exception('mapmask failed!')
                    #os.symlink(fofc[0], fofc_pth)
                    #os.system(
                    #    f'module load ccp4 && mapmask mapin {fofc[0]} mapout {fofc_pth} xyzin {self.output().path} << eof\n border 6\n end\n eof')
                if len(fofc2) > 0:
                    mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                        border %s
                        end
                    eof
                    ''' % (fofc2[0], fofc2_pth, self.output().path, str(0))
                    print(mapmask)
                    proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                          executable='/bin/bash')
                    #process = subprocess.Popen(
                    #    str(self.ssh_command + ' "' + mapmask + '"'),
                    #    shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    #out, err = process.communicate()
                    #if '(mapmask) - normal termination' not in out:
                    #    raise Exception('mapmask failed!')
                    #os.symlink(fofc2[0], fofc2_pth)
                    #os.system(
                    #    f'module load ccp4 && mapmask mapin {fofc2[0]} mapout {fofc2_pth} xyzin {self.output().path} << eof\n border 6\n end\n eof')

                # probably should use enumerate
                if len(event_maps) > 0:
                    event_num = 0
                    for i in event_maps:
                        fn = self.output().path.replace('.pdb', f'_event_{event_num}.ccp4')
                        #os.symlink(i, fn)
                        mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
                            border %s
                            end
                        eof
                        ''' % (i, fn, self.output().path, str(0))
                        print(mapmask)
                        proc = subprocess.run(mapmask, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                                              executable='/bin/bash')
                        #process = subprocess.Popen(
                        #    str(self.ssh_command + ' "' + mapmask + '"'),
                        #    shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                        #out, err = process.communicate()
                        #if '(mapmask) - normal termination' not in out:
                        #    raise Exception('mapmask failed!')
                        #os.system(
                        #    f'module load ccp4 && mapmask mapin {i} mapout {fn} xyzin {self.output().path} << eof\n border 6\n end\n eof')
                        event_num += 1

                if self.prod_smiles:
                    smi = self.prod_smiles
                elif self.smiles:
                    smi = self.smiles
                #                 if self.smiles:
                smi_pth = self.output().path.replace('.pdb', '_smiles.txt')
                with open(smi_pth, 'w') as f:
                    f.write(str(smi))
                #  f.close() should delete.


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
        os.system(f'/dls/science/groups/i04-1/software/miniconda_3/envs/fragalysis_env2/bin/python /dls/science/groups/i04-1/software/tyler/fragalysis-api/fragalysis_api/xcimporter/xcimporter.py --in_dir={self.target} --out_dir={self.staging_directory} --target {target_name} -m')
        with self.output().open('w') as f:
            f.write('')

class BatchCutMaps(luigi.Task):
    log_directory = luigi.Parameter(default=DirectoriesConfig().log_directory)
    staging_directory = luigi.Parameter(default=DirectoriesConfig().staging_directory)
    input_directory = luigi.Parameter(default=DirectoriesConfig().input_directory)
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        stagingfolders = glob.glob(f'{self.staging_directory}*')
        stagingfolders = [x for x in stagingfolders if 'tmp' not in x]
        stagingfolders = [x for x in stagingfolders if 'mono' not in x]
        print(stagingfolders)
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
                os.system(f'module load ccp4 && mapmask mapin {j} mapout {j} xyzin {os.path.join(i, f"{crys}.pdb")} << eof\n border 6\n end\n eof')

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
