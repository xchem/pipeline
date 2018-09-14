import luigi
import pandas

from functions import data_analysis_functions as daf
from functions import db_functions as dbf
from luigi_classes.archive import data_in_proasis


class EdstatsScores(luigi.Task):
    strucid = luigi.Parameter()
    crystal = luigi.Parameter()

    def output(self):
        filename = str('logs/edstats/' + str(self.crystal) + '_' + str(self.strucid) + '.done')
        return luigi.LocalTarget(filename)

    def run(self):
        results_dict = {'crystal': [], 'strucid': [], 'ligand': []}

        output_data, header = daf.run_edstats(self.strucid)

        if output_data:
            for ligand in output_data:

                lig_string = '-'.join([str(x) for x in ligand[0]])
                print(lig_string)
                results_dict['ligand'].append(lig_string)
                for j in range(0, len(header[24:36])):
                    if header[24 + j] not in list(results_dict.keys()):
                        results_dict[header[24 + j]] = []
                    results_dict[header[24 + j]].append(str(ligand[1][24 + j]))
                results_dict['crystal'].append(self.crystal)
                results_dict['strucid'].append(self.strucid)

        data_frame = pandas.DataFrame.from_dict(results_dict)

        xchem_engine = dbf.create_engine('postgresql://uzw12877@localhost:5432/xchem')
        data_frame.to_sql('ligand_edstats', xchem_engine, if_exists='append')

        with self.output().open('wb') as f:
            f.write('')


class StartEdstatsScores(luigi.Task):

    def requires(self):
        conn, c = dbf.connectDB()
        c.execute("select crystal_name, strucid from proasis_hits where strucid !=''")
        rows = c.fetchall()

        crystal_list = []
        strucid_list = []

        for row in rows:
            crystal = str(row[0])
            strucid = str(row[1])
            crystal_list.append(crystal)
            strucid_list.append(strucid)

        run_list = list(zip(crystal_list, strucid_list))

        return data_in_proasis.StartHitTransfers(), \
               [EdstatsScores(crystal=crystal_name, strucid=strucid_no)
               for (crystal_name, strucid_no) in run_list]

    def output(self):
        return luigi.LocalTarget('logs/edstats.done')

    def run(self):
        with self.output().open('wb') as f:
            f.write('')
