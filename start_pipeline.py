from setup_django import setup_django

setup_django()

import luigi

import sentry_sdk
from sentry_sdk import capture_exception
from sentry_sdk import configure_scope

from luigi_classes.transfer_pandda import AnnotateAllEvents, TransferPandda
from luigi_classes.transfer_proasis import InitDBEntries, UploadLeads, WriteBlackLists, UploadHits, AddProjects
from luigi_classes.pull_proasis import GetOutFiles
from luigi_classes.transfer_soakdb import StartTransfers
from luigi_classes.transfer_verne import UpdateVerne
from luigi_classes.config_classes import SentryConfig

import os
import datetime

# set sentry key url from config
sentry_string = str("https://" + SentryConfig().key + "@sentry.io/" + SentryConfig().ident)
# initiate sentry sdk
sentry_sdk.init(sentry_string)


# custom handler for luigi exception
@luigi.Task.event_handler(luigi.Event.FAILURE)
def send_failure_to_sentry(task, exception):
    # add additional information to sentry scope (about task)
    with configure_scope() as scope:
        scope.set_extra('os_pid', os.getpid())
        scope.set_extra('task_id', task.task_id)
        scope.set_extra('task_family', task.task_family)
        scope.set_extra('param_args', task.param_args)
        scope.set_extra('param_kwargs', task.param_kwargs)
    # send error to sentry
    capture_exception()


class StartPipeline(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.datetime.now())
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        yield StartTransfers()
        yield AddProjects()
        yield TransferPandda(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath)
        yield AnnotateAllEvents(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath)
        yield InitDBEntries(date=self.date, hit_directory=self.hit_directory)
        yield UploadLeads(date=self.date, hit_directory=self.hit_directory)
        yield GetOutFiles()
        yield WriteBlackLists(date=self.date, hit_directory=self.hit_directory)
        yield UpdateVerne()

    def output(self):
        return luigi.LocalTarget('logs/pipe.done')

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class PostPipeClean(luigi.Task):
    def requires(self):
        return StartPipeline()

    def run(self):
        paths = [TransferPandda(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath).output().path,
                 AnnotateAllEvents(date_time=self.date_time, soak_db_filepath=self.soak_db_filepath).output().path,
                 InitDBEntries(date=self.date, hit_directory=self.hit_directory).output().path,
                 UploadLeads(date=self.date, hit_directory=self.hit_directory).output().path,
                 UploadHits(date=self.date, hit_directory=self.hit_directory).output().path,
                 WriteBlackLists(date=self.date, hit_directory=self.hit_directory).output().path,
                 os.path.join(os.getcwd(), 'logs/pipe.done')]
        for path in paths:
            try:
                os.remove(path)
            except:
                pass


if __name__ == '__main__':
    luigi.build([PostPipeClean()], workers=1, no_lock=False)
