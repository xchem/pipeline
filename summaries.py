import os
import datetime
import luigi
from functions import proasis_api_funcs as paf
from functions import db_functions as dbf
import pandas as pd


class CheckProasisForProtein(luigi.Task):

    protein = luigi.Parameter()
    log_dir = luigi.Parameter(default='proasis_testing/logs')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(self.date.strftime(os.path.join(self.log_dir, str(self.protein + '_%Y%m%d.log'))))

    def run(self):

        conn, c = dbf.connectDB()

        c.execute('select crystal_name from proasis_hits where protein=%s', (self.protein,))
        rows = c.fetchall()
        crystal_list = []

        for row in rows:
            crystal_list.append(str(row[0]))

        crystal_list = list(set(crystal_list))

        project_strucids = paf.get_strucids_from_project(self.protein)
        print(project_strucids)
        db_strucids = []
        current_list = []
        status_list = []
        good_list = []

        file_checks = {'crystal': [], 'bound_state': [], 'mod_date': [], 'pdb': [], 'mtz': [], '2fofc': [], 'fofc': [],
                       'ligs': []}

        good_structures = {'crystal': [], 'bound_state': [], 'mod_date': [], 'strucid': []}

        for crys in crystal_list:
            if str(self.protein + '-') in crys:
                current_list.append(crys)

        for crystal in list(set(current_list)):

            error_list = []

            c.execute(
                "select strucid, bound_conf, modification_date from proasis_hits "
                "where crystal_name like %s and strucid NOT LIKE ''",
                (crystal,))

            bound_list = []
            strucid_list = []
            mod_date_list = []

            rows = c.fetchall()

            for row in rows:
                strucid = str(row[0])
                strucid_list.append(strucid)

                bound_conf = str(row[1])
                bound_list.append(bound_conf)

                modification_date = str(row[2])
                mod_date_list.append(modification_date)

            unique_bound = list(set(bound_list))
            unique_modification_date = list(set(mod_date_list))
            unique_strucids = list(set(strucid_list))

            for ids in unique_strucids:
                db_strucids.append(ids)

            c.execute("select strucid from proasis_leads where protein=%s and strucid!=''", (self.protein,))
            rows = c.fetchall()

            for row in rows:
                db_strucids.append(str(row[0]))

            if len(set([len(unique_modification_date), len(unique_bound), len(unique_strucids)])) == 1:
                status_list.append(0)
                good_list.append(crystal)
                for i in range(0, len(unique_bound)):
                    good_structures['crystal'].append(crystal)
                    good_structures['bound_state'].append(unique_bound[i])
                    good_structures['mod_date'].append(unique_modification_date[i])
                    good_structures['strucid'].append(unique_strucids[i])

            for i in range(0, len(good_structures['strucid'])):
                if good_structures['strucid'][i] not in project_strucids:
                    print('missing or incorrect strucid in db for '
                          + str(good_structures['crystal'][i] + ' (' + str(good_structures['strucid'][i]) + ')'))

            if sum([len(unique_modification_date), len(unique_bound), len(unique_strucids)]) == 0:

                c.execute(
                    "select bound_conf, modification_date, exists_pdb, exists_mtz, exists_2fofc, exists_fofc, "
                    "ligand_list from proasis_hits where crystal_name like %s",
                    (crystal,))
                rows = c.fetchall()

                for row in rows:
                    file_checks['crystal'].append(crystal)
                    file_checks['bound_state'].append(str(row[0]))
                    file_checks['mod_date'].append(str(row[1]))
                    file_checks['pdb'].append(str(row[2]))
                    file_checks['mtz'].append(str(row[3]))
                    file_checks['2fofc'].append(str(row[4]))
                    file_checks['fofc'].append(str(row[5]))
                    file_checks['ligs'].append(str(row[6]))

        #         for key in file_checks.keys():
        #             if '0' in file_checks[key]:
        #                 error_list.append(str('missing ' + str(key) + ' file!'))
        #
        #             if 'None' in file_checks[key]:
        #                 error_list.append(str('None value found for ' + str(key)))
        #
        #     elif len(set([len(unique_modification_date), len(unique_bound), len(unique_strucids)])) > 1:
        #         status_list.append(1)
        #
        # error_frame = pd.DataFrame.from_dict(file_checks)
        # cols = ['crystal', 'bound_state', 'mod_date', 'ligs', 'mtz', 'pdb', '2fofc', 'fofc']
        # error_frame = error_frame[cols]
        # error_frame.sort_values(by=['crystal'], inplace=True)
        #
        # good_frame = pd.DataFrame.from_dict(good_structures)
        # cols = ['crystal', 'bound_state', 'mod_date', 'strucid']
        # good_frame = good_frame[cols]
        # good_frame.sort_values(by=['crystal'], inplace=True)
        #
        #
        try:
            c.execute("select strucid from proasis_leads where protein=%s and strucid!=''", (self.protein,))
            rows = c.fetchall()

            for row in rows:
                db_strucids.append(str(row[0]))
            in_common = list(set(db_strucids) & set(project_strucids))
            for strucid in db_strucids:
                if strucid not in in_common:
                    print(self.protein + ': ' + strucid + ' found in database but not in proasis')

            for strucid in project_strucids:
                if strucid not in in_common:
                    print(self.protein + ': ' + strucid + ' found in proasis but not in db')
        except:
            pass