import luigi
import subprocess
import os
import datetime
from functions import db_functions, misc_functions
from sqlalchemy import create_engine
import pandas
import sqlite3


class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):
        # this should be run somewhere else
        # subprocess.call('./pg_backup.sh')

        # maybe change to *.sqlite to find renamed files? - this will probably pick up a tonne of backups
        command = str('''find ''' + self.filepath +  ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print''')
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print(command)

        # run process to find sqlite files
        out, err = process.communicate()
        out = out.decode('ascii')
        print(out)

        # write filepaths to file as output
        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    def requires(self):
        conn, c = db_functions.connectDB()
        exists = db_functions.table_exists(c, 'soakdb_files')
        if not exists:
            return TransferAllFedIDsAndDatafiles()
        else:
            return FindSoakDBFiles()

    def output(self):
        return luigi.LocalTarget('logs/checked_files/files_' + str(self.date) + '.checked')

    def run(self):

        conn, c = db_functions.connectDB()
        exists = db_functions.table_exists(c, 'soakdb_files')

        checked = []

        # Status codes:-
        # 0 = new
        # 1 = changed
        # 2 = not changed

        if exists:
            with self.input().open('r') as f:
                files = f.readlines()

            for filename in files:

                filename_clean = filename.rstrip('\n')

                c.execute('select filename, modification_date, status_code from soakdb_files where filename like %s;', (filename_clean,))

                for row in c.fetchall():
                    if len(row) > 0:
                        data_file = str(row[0])
                        checked.append(data_file)
                        old_mod_date = str(row[1])
                        current_mod_date = misc_functions.get_mod_date(data_file)

                        if current_mod_date > old_mod_date:
                            c.execute('UPDATE soakdb_files SET status_code = 1 where filename like %s;', (filename_clean,))
                            c.execute('UPDATE soakdb_files SET modification_date = %s where filename like %s;', (current_mod_date, filename_clean))
                            conn.commit()

                if filename_clean not in checked:
                    out, err, proposal = db_functions.pop_soakdb(filename_clean)
                    db_functions.pop_proposals(proposal)
                    c.execute('UPDATE soakdb_files SET status_code = 0 where filename like %s;', (filename_clean,))
                    conn.commit()

            c.execute('select filename from soakdb_files;')

            # for row in c.fetchall():
            #     if str(row[0]) not in checked:
            #         data_file = str(row[0])

        exists = db_functions.table_exists(c, 'lab')
        if not exists:
            c.execute('UPDATE soakdb_files SET status_code = 0;')
            conn.commit()

        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    # date parameter for daily run - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # needs a list of soakDB files from the same day
    def requires(self):
        return FindSoakDBFiles()

    # output is just a log file
    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/transfer_logs/fedids_%Y%m%d.txt'))

    # transfers data to a central postgres db
    def run(self):
        # connect to central postgres db
        conn, c = db_functions.connectDB()

        # use list from previous step as input to write to postgres
        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')

                out, err, proposal = db_functions.pop_soakdb(database_file)

        proposal_list = []
        c.execute('SELECT proposal FROM soakdb_files')
        rows = c.fetchall()
        for row in rows:
            proposal_list.append(str(row[0]))

        for proposal_number in set(proposal_list):
            db_functions.pop_proposals(proposal_number)

        c.close()

        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    data_file = luigi.Parameter()
    file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles()

    def output(self):
        pass

    def run(self):
        conn, c = db_functions.connectDB()
        c.execute('delete from lab where file_id=%s', (self.file_id,))
        conn.commit()
        c.execute('delete from refinement where file_id=%s', (self.file_id,))
        conn.commit()
        c.execute('delete from dimple where file_id=%s', (self.file_id,))
        conn.commit()
        c.execute('delete from data_processing where file_id=%s', (self.file_id,))
        conn.commit()
        db_functions.transfer_data(self.data_file)
        c.execute('UPDATE soakdb_files SET status_code=2 where filename like %s;', (self.data_file,))
        conn.commit()


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles()

    def run(self):
        db_functions.transfer_data(self.data_file)
        conn, c = db_functions.connectDB()
        c.execute('UPDATE soakdb_files SET status_code=2 where filename like %s;', (self.data_file,))
        conn.commit()


class StartTransfers(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def get_file_list(self, status_code):
        datafiles = []
        fileids = []
        conn, c = db_functions.connectDB()
        c.execute('SELECT filename, id FROM soakdb_files WHERE status_code = %s', (str(status_code),))
        rows = c.fetchall()
        for row in rows:
            datafiles.append(str(row[0]))
            fileids.append(str(row[1]))

        out_list = list(zip(datafiles, fileids))
        return out_list

    def requires(self):
        new_list = self.get_file_list(0)
        changed_list = self.get_file_list(1)
        return [TransferNewDataFile(data_file=datafile, file_id=fileid) for (datafile, fileid) in new_list], \
               [TransferChangedDataFile(data_file=newfile, file_id=newfileid) for (newfile, newfileid) in changed_list]

    def output(self):
        return luigi.LocalTarget('logs/transfer_logs/transfers_' + str(self.date) + '.done')

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class FindProjects(luigi.Task):
    def add_to_postgres(self, table, protein, subset_list, data_dump_dict, title):
        xchem_engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')

        temp_frame = table.loc[table['protein'] == protein]
        temp_frame.reset_index(inplace=True)
        temp2 = temp_frame.drop_duplicates(subset=subset_list)

        try:
            nodups = db_functions.clean_df_db_dups(temp2, title, xchem_engine,
                                                   list(data_dump_dict.keys()))
            nodups.to_sql(title, xchem_engine, if_exists='append')
        except:
            temp2.to_sql(title, xchem_engine, if_exists='append')


    def requires(self):
        return CheckFiles(), StartTransfers()

    def output(self):
        return luigi.LocalTarget('logs/findprojects.done')

    def run(self):
        # all data necessary for uploading hits
        crystal_data_dump_dict = {'crystal_name': [], 'protein': [], 'smiles': [], 'bound_conf': [],
                                  'modification_date': [], 'strucid':[]}

        # all data necessary for uploading leads
        project_data_dump_dict = {'protein': [], 'pandda_path': [], 'reference_pdb': [], 'strucid':[]}

        outcome_string = '(%3%|%4%|%5%|%6%)'

        conn, c = db_functions.connectDB()

        c.execute('''SELECT crystal_id, bound_conf, pdb_latest FROM refinement WHERE outcome SIMILAR TO %s''',
                  (str(outcome_string),))

        rows = c.fetchall()

        print((str(len(rows)) + ' crystals were found to be in refinement or above'))

        for row in rows:

            c.execute('''SELECT smiles, protein FROM lab WHERE crystal_id = %s''', (str(row[0]),))

            lab_table = c.fetchall()

            if len(str(row[0])) < 3:
                continue

            if len(lab_table) > 1:
                print(('WARNING: ' + str(row[0]) + ' has multiple entries in the lab table'))
                # print lab_table

            for entry in lab_table:
                if len(str(entry[1])) < 2 or 'None' in str(entry[1]):
                    protein_name = str(row[0]).split('-')[0]
                else:
                    protein_name = str(entry[1])


                crystal_data_dump_dict['protein'].append(protein_name)
                crystal_data_dump_dict['smiles'].append(entry[0])
                crystal_data_dump_dict['crystal_name'].append(row[0])
                crystal_data_dump_dict['bound_conf'].append(row[1])
                crystal_data_dump_dict['strucid'].append('')

                try:
                    modification_date = misc_functions.get_mod_date(str(row[1]))

                except:
                    modification_date = ''

                crystal_data_dump_dict['modification_date'].append(modification_date)

            c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_id = %s''', (str(row[0]),))

            pandda_info = c.fetchall()

            for pandda_entry in pandda_info:
                project_data_dump_dict['protein'].append(protein_name)
                project_data_dump_dict['pandda_path'].append(pandda_entry[0])
                project_data_dump_dict['reference_pdb'].append(pandda_entry[1])
                project_data_dump_dict['strucid'].append('')

        project_table = pandas.DataFrame.from_dict(project_data_dump_dict)
        crystal_table = pandas.DataFrame.from_dict(crystal_data_dump_dict)

        protein_list = set(list(project_data_dump_dict['protein']))
        print(protein_list)

        for protein in protein_list:

            self.add_to_postgres(project_table, protein, ['reference_pdb'], project_data_dump_dict, 'proasis_leads')

            self.add_to_postgres(crystal_table, protein, ['crystal_name', 'smiles', 'bound_conf'],
                                 crystal_data_dump_dict, 'proasis_hits')

        with self.output().open('wb') as f:
            f.write('')

