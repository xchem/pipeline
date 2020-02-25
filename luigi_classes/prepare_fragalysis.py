import datetime
import uuid

from setup_django import setup_django

setup_django()

import luigi
from xchem_db.models import *
# from . import transfer_soakdb
from .config_classes import SoakDBConfig, DirectoriesConfig

from utils.custom_output_targets import DjangoTaskTarget
from utils.refinement import RefinementObjectFiles


class SymlinkBoundPDB(luigi.Task):
    crystal = luigi.Parameter()
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

    def requires(self):
        # return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)
        pass

    def output(self):
        pth = os.path.join(self.hit_directory,
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
        os.symlink(file_obj.bound_conf, self.output().path)


class BatchSymlinkBoundPDB(luigi.Task):
    filter_by = Refinement.objects.filter(outcome__gte=4).filter(outcome__lte=6)
    resources = {'django': 1}
    date = luigi.Parameter(default=datetime.datetime.now())
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)
    hit_directory = luigi.Parameter(default=DirectoriesConfig().hit_directory)
    uuid = str(uuid.uuid4())

    def requires(self):
        return [
            SymlinkBoundPDB(
                crystal=crystal,
                hit_directory=self.hit_directory,
                soak_db_filepath=self.soak_db_filepath
            )
            for crystal in self.filter_by
        ]

    def output(self):
        return DjangoTaskTarget(class_name=self.__class__.__name__, uuid=self.uuid)

    def run(self):
        Tasks.objects.create(task_name=self.__class__.__name__, uuid=self.uuid)
