import datetime
import luigi

import functions.data_analysis_functions as daf
from luigi_classes import ligand_analysis


class ProjectSummaryCSV(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        pass
        # return ligand_analysis.StartEdstatsScores()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('html/project_summary_csv/%Y%m%d.csv'))

    def run(self):
        df = daf.get_project_counts()
        df.to_csv(self.date.strftime('html/project_summary_csv/%Y%m%d.csv'))


class ProjectSummaryHTML(luigi.Task):
    html_out = luigi.Parameter(default='html/summary-bar-stacked.html')

    def requires(self):
        return ProjectSummaryCSV()

    def output(self):
        return luigi.LocalTarget('logs/summary_html.done')

    def run(self):
        daf.project_summary_html(self.input().path, self.html_out)


class LigandEdstatsCSV(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ligand_analysis.StartEdstatsScores()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('html/ligand_edstats_csv/%Y%m%d.csv'))

    def run(self):
        daf.export_ligand_edstats(self.date.strftime('html/ligand_edstats_csv/%Y%m%d.csv'))


class LigandEdstatsViolinHTML(luigi.Task):
    html_root = luigi.Parameter(default='html/')

    def requires(self):
        return LigandEdstatsCSV()

    def output(self):
        return luigi.LocalTarget('logs/violin_html.done')

    def run(self):
        daf.edstats_violin(self.input().path, self.html_root)
