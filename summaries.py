import os
import datetime
import luigi
from functions import proasis_api_funcs as paf
from functions import db_functions as dbf
import pandas as pd
from collections import Counter


def find_proasis_repeats(protein):
    project_strucids = paf.get_strucids_from_project(protein)
    project_titles = [paf.get_strucid_json(strucid)['allStrucs'][0]['TITLE'].split()[-1] for strucid in
                      project_strucids]

    counts = dict(Counter(project_titles))

    repeats = {'crystal': [], 'strucids': [], 'bound_confs':[]}

    for key in counts.keys():
        if counts[key] > 1:
            repeats['crystal'].append(key)
            repeats['strucids'].append([project_strucids[i] for i, x in enumerate(project_titles) if x == key])

    conn, c = dbf.connectDB()
    for strucid_list in repeats['strucids']:
        bound_list = []
        for struc in strucid_list:
            c.execute('select bound_conf from proasis_hits where strucid=%s', (struc,))
            rows = c.fetchall()
            for row in rows:
                bound_list.append(str(row[0]))
        repeats['bound_confs'].append(bound_list)

    return repeats


class CheckProasisForProtein(luigi.Task):

    protein = luigi.Parameter()
    log_dir = luigi.Parameter(default='proasis_testing/logs')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(self.date.strftime(os.path.join(self.log_dir, str(self.protein + '_%Y%m%d.log'))))

    def run(self):

        project_strucids = paf.get_strucids_from_project(self.protein)

        # get crystal names for protein according to db
        conn, c = dbf.connectDB()

        c.execute('select crystal_name from proasis_hits where protein=%s', (self.protein,))
        rows = c.fetchall()
        crystal_list = []

        for row in rows:
            crystal_list.append(str(row[0]))

        crystal_list = list(set(crystal_list))

        db_strucids = []
        # status_list = []
        # good_list = []

        file_checks = {'crystal': [], 'bound_state': [], 'mod_date': [], 'pdb': [], 'mtz': [], '2fofc': [], 'fofc': [],
                       'ligs': []}

        # get info for crystals identified
        for crystal in list(set(crystal_list)):

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

        #     # get info about crystals in proasis_hits (ones identified as in refinement) that haven't made it to
        #     # proasis
        #
        #     if sum([len(unique_modification_date), len(unique_bound), len(unique_strucids)]) == 0:
        #
        #         c.execute(
        #             "select bound_conf, modification_date, exists_pdb, exists_mtz, exists_2fofc, exists_fofc, "
        #             "ligand_list from proasis_hits where crystal_name like %s",
        #             (crystal,))
        #         rows = c.fetchall()
        #
        #         for row in rows:
        #             file_checks['crystal'].append(crystal)
        #             file_checks['bound_state'].append(str(row[0]))
        #             file_checks['mod_date'].append(str(row[1]))
        #             file_checks['pdb'].append(str(row[2]))
        #             file_checks['mtz'].append(str(row[3]))
        #             file_checks['2fofc'].append(str(row[4]))
        #             file_checks['fofc'].append(str(row[5]))
        #             file_checks['ligs'].append(str(row[6]))

        # clear up mismatching entries
        in_common = list(set(db_strucids) & set(project_strucids))
        for strucid in db_strucids:
            if strucid not in in_common:
                print(self.protein + ': ' + strucid + ' found in database but not in proasis')
                print('removing entry from db...')
                c.execute("UPDATE proasis_hits set strucid='' where strucid=%s", (strucid,))
                conn.commit()
                c.execute("UPDATE proasis_leads set strucid='' where strucid=%s", (strucid,))
                conn.commit()
                print('\n')

        for strucid in project_strucids:
            if strucid not in in_common:
                print(self.protein + ': ' + strucid + ' found in proasis but not in db')
                print('removing entry from proasis...')
                paf.delete_structure(strucid)
                print('\n')


            # good_structures = {'crystal': [], 'bound_state': [], 'mod_date': [], 'strucid': []}
            # if len(set([len(unique_modification_date), len(unique_bound), len(unique_strucids)])) == 1:
            #     status_list.append(0)
            #     good_list.append(crystal)
            #     for i in range(0, len(unique_bound)):
            #         good_structures['crystal'].append(crystal)
            #         good_structures['bound_state'].append(unique_bound[i])
            #         good_structures['mod_date'].append(unique_modification_date[i])
            #         good_structures['strucid'].append(unique_strucids[i])

            # for i in range(0, len(good_structures['strucid'])):
            #     if good_structures['strucid'][i] not in project_strucids:
            #         print('missing or incorrect strucid in db for '
            #               + str(good_structures['crystal'][i] + ' (' + str(good_structures['strucid'][i]) + ')'))

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

        # clean up repeats
        repeats = find_proasis_repeats(self.protein)
        for i, x in enumerate(repeats['crystal']):
            bound_list = repeats['bound_confs'][i]
            strucids = repeats['strucids'][i]

            if len(bound_list)==len(strucids):
                if len(list(set(bound_list)))==1:
                    print(str('identical uploaded structures: ' + str(strucids)) + ' (' + x + ')')
                    print('removing repeat structures from proasis, and updating database...')
                    to_delete_strucs=strucids[1:]
                    to_delete_confs=bound_list[1:]

                    for j in range(0, len(to_delete_strucs)):
                        c.execute('DELETE FROM proasis_hits WHERE strucid=%s and bound_conf=%s', (to_delete_strucs[j],
                                                                                                  to_delete_confs[j]))
                        conn.commit()
                        paf.delete_structure(to_delete_strucs[j])

        pd.DataFrame.from_dict(repeats).to_csv('test.csv')


class GenProasisSummary(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):

