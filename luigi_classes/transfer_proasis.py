import luigi
import luigi_classes.transfer_soakdb as transfer_soakdb
import datetime
from db.models import Crystal, Refinement, ProasisHits, ProasisLeads, Dimple
from functions import misc_functions, db_functions
import setup_django
import traceback


class InitDBEntries(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        pass

    def run(self):
        fail_count = 0
        refinement = Refinement.objects.filter(outcome__gte=3)
        print(len(refinement))
        for obj in refinement:
            if obj.bound_conf !='':
                bound_conf = obj.bound_conf
            elif obj.pdb_latest != '':
                bound_conf = obj.pdb_latest
            else:
                fail_count += 1
                continue

            mtz = db_functions.check_file_status('refine.mtz', bound_conf)
            if not mtz[0]:
                continue

            two_fofc = db_functions.check_file_status('2fofc.map', bound_conf)
            if not two_fofc[0]:
                continue

            fofc = db_functions.check_file_status('fofc.map', bound_conf)
            if not fofc[0]:
                continue



            mod_date = misc_functions.get_mod_date(obj.bound_conf)
            proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj, crystal_name=obj.crystal_name,
                                                      pdb_file=obj.bound_conf, modification_date=mod_date,
                                                      mtz=mtz[1], two_fofc=two_fofc[1], fofc=fofc[1])

            dimple = Dimple.objects.filter(crystal_name=obj.crystal_name)
            if dimple.count==1:
                if dimple[0].reference and dimple[0].reference.reference_pdb:
                    proasis_lead_entry = ProasisLeads.objects.get_or_create(reference_pdb=dimple.reference)



class CopyFiles(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class AddProject(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class UploadLead(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class UploadHit(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass