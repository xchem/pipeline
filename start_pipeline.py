import setup_django
import luigi
import os
import pandas as pd
import datetime
from luigi_classes.transfer_pandda import FindSearchPaths, AddPanddaData


class StartPipeline(luigi.Task):
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        in_file = FindSearchPaths(soak_db_filepath=self.soak_db_filepath, date_time=self.date_time).output().path
        print(in_file)
        if not os.path.isfile(in_file):
            return FindSearchPaths(soak_db_filepath=self.soak_db_filepath, date_time=self.date_time)
        else:
            frame = pd.DataFrame.from_csv(in_file)
            return [AddPanddaData(search_path=search_path, soak_db_filepath=filepath, sdbfile=sdbfile) for
                    search_path, filepath, sdbfile in list(
                    zip(frame['search_path'], frame['soak_db_filepath'], frame['sdbfile']))]

    def run(self):
        os.remove(FindSearchPaths(soak_db_filepath=self.soak_db_filepath, date_time=self.date_time).output().path)