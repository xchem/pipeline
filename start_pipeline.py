import setup_django
import luigi
import datetime
from luigi_classes.transfer_pandda import TransferPandda
from luigi_classes.transfer_proasis import InitDBEntries, UploadLeads, CheckLigands, UploadHits
import os


class StartPipeline(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        try:
            os.remove(TransferPandda(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath).output().path)
            os.remove(InitDBEntries(date=self.date, hit_directory=self.hit_directory).output().path)
            os.remove(UploadLeads(date=self.date, hit_directory=self.hit_directory).output().path)
            os.remove(UploadHits(date=self.date, hit_directory=self.hit_directory).output().path)
        except:
            pass

        yield TransferPandda(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath)
        yield InitDBEntries(date=self.date, hit_directory=self.hit_directory)
        yield UploadLeads(date=self.date, hit_directory=self.hit_directory)
        yield UploadHits(date=self.date, hit_directory=self.hit_directory)

    def output(self):
        pass

    def run(self):
        pass