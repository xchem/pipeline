import luigi
import luigi_classes.transfer_soakdb as transfer_soakdb
import datetime
import subprocess
from db.models import *
from functions import misc_functions, db_functions
import setup_django


class InitDBEntries(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return transfer_soakdb.CheckUploadedFiles(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_db_%Y%m%d%H.txt'))

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
                fail_count += 1
                continue

            two_fofc = db_functions.check_file_status('2fofc.map', bound_conf)
            if not two_fofc[0]:
                fail_count += 1
                continue

            fofc = db_functions.check_file_status('fofc.map', bound_conf)
            if not fofc[0]:
                fail_count += 1
                continue

            mod_date = misc_functions.get_mod_date(obj.bound_conf)
            if mod_date:
                proasis_hit_entry = ProasisHits.objects.get_or_create(refinement=obj, crystal_name=obj.crystal_name,
                                                          pdb_file=obj.bound_conf, modification_date=mod_date,
                                                          mtz=mtz[1], two_fofc=two_fofc[1], fofc=fofc[1])

                dimple = Dimple.objects.filter(crystal_name=obj.crystal_name)
                print(dimple.count())
                if dimple.count()==1:
                    if dimple[0].reference and dimple[0].reference.reference_pdb:
                        proasis_lead_entry = ProasisLeads.objects.get_or_create(reference_pdb=dimple[0].reference)

        print(fail_count)

        with self.output().open('w') as f:
            f.write('')


class AddProject(luigi.Task):
    protein_name = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return InitDBEntries(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        self.protein_name = str(self.protein_name).upper()
        return luigi.LocalTarget(str('logs/proasis/' + str(self.protein_name) + '.added'))

    def run(self):
        self.protein_name = str(self.protein_name).upper()
        add_project = str(
            '/usr/local/Proasis2/utils/addnewproject.py -c admin -q OtherClasses -p ' + str(self.protein_name))
        process = subprocess.Popen(add_project, stdout=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        out = out.decode('ascii')
        if err:
            err = err.decode('ascii')
            raise Exception(err)
        if len(out) > 1:
            with self.output().open('w') as f:
                f.write(str(out))
                print(out)


class AddProjects(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        proteins = Target.objects.all()
        return [AddProject(protein_name=protein.target_name, date=self.date, soak_db_filepath=self.soak_db_filepath)
                for protein in proteins]

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/proasis_projects_%Y%m%d%H.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')

class AddLead(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return AddProjects(date=self.date, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        pass

    def run(self):
        leads = ProasisLeads.objects.filter()
        for lead in leads:
            dimple = Dimple.objects.filter(reference=lead.reference_pdb)
            crystals = [dimp.crystal_name for dimp in dimple]
            lead_crystals = ProasisHits.objects.filter(crystal_name__in=crystals)

            for crys in lead_crystals:
                events = PanddaEvent.objects.filter(crystal=crys.crystal_name)
                sites = list(set([(event.site, event.crystal.crystal_name) for event in events]))
                for site in sites:
                    site = site + (lead.reference_pdb.reference_pdb,)
                    print(site)

class UploadLeads(luigi.Task):

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