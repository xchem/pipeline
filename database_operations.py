import luigi
import psycopg2
import sqlite3
import subprocess
import sys
import os
import datetime
import pandas
from sqlalchemy import create_engine
import logging
import db_functions, misc_functions

class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def output(self):
        return luigi.LocalTarget(self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):
        # maybe change to *.sqlite to find renamed files? - this will probably pick up a tonne of backups
        process = subprocess.Popen(str('''find ''' + self.filepath +  ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print'''),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        # run process to find sqlite files
        out, err = process.communicate()

        # write filepaths to file as output
        with self.output().open('w') as f:
            f.write(out)


class CheckFiles(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    def requires(self):
        conn, c = db_functions.connectDB()
        exists = db_functions.table_exists(c, 'soakdb_files')
        if not exists:
            return TransferAllFedIDsAndDatafiles()
        else:
            return FindSoakDBFiles()

    def output(self):
        pass

    def run(self):
        logfile = self.date.strftime('transfer_logs/CheckFiles_%Y%m%d.txt')
        logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s %(message)s',
                            datefrmt='%m/%d/%y %H:%M:%S')

        conn, c = db_functions.connectDB()
        exists = db_functions.table_exists(c, 'soakdb_files')

        #checked = []
        #changed = []
        #new = []

        # Status codes:-
        # 0 = new
        # 1 = changed
        # 2 = not changed

        if exists:
            with self.input().open('r') as f:
                files = f.readlines()

            for filename in files:

                filename_clean = filename.rstrip('\n')

                c.execute('select filename, modification_date from soakdb_files where filename like %s;', (filename_clean,))

                for row in c.fetchall():
                    if len(row) > 0:
                        data_file = str(row[0])
                        #checked.append(data_file)
                        old_mod_date = str(row[1])
                        current_mod_date = misc_functions.get_mod_date(data_file)

                        if current_mod_date > old_mod_date:
                            logging.info(str(data_file) + ' has changed!')
                            #changed.append(data_file)
                            c.execute('UPDATE soakdb_files SET status_code = 1 where filename like %s;', (filename_clean,))
                            conn.commit()
                            # start class to add row and kick off process for that file
                        else:
                            logging.info(str(data_file) + ' has not changed!')
                            c.execute('UPDATE soakdb_files SET status_code = 2 where filename like %s;', (filename_clean,))
                            conn.commit()

            c.execute('select filename from soakdb_files;')

            for row in c.fetchall():
                if str(row[0]) not in checked:
                    data_file = str(row[0])
                    file_exists = os.path.isfile(data_file)

                    if not file_exists:
                        logging.warning(str(data_file) + ' no longer exists! - notify users!')

                    else:
                        logging.info(str(row[0]) + ' is a new file!')
                        c.execute('UPDATE soakdb_files SET status_code = 0 where filename like %s;', (filename_clean,))
                        conn.commit()
                        #new.append(str(row[0]))


class TransferAllFedIDsAndDatafiles(luigi.Task):
    # date parameter for daily run - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # needs a list of soakDB files from the same day
    def requires(self):
        return FindSoakDBFiles()

    # output is just a log file
    def output(self):
        return luigi.LocalTarget(self.date.strftime('transfer_logs/fedids_%Y%m%d.txt'))

    # transfers data to a central postgres db
    def run(self):
        # connect to central postgres db
        conn, c = db_functions.connectDB()
        # create a table to hold info on sqlite files
        c.execute('''CREATE TABLE IF NOT EXISTS soakdb_files (id SERIAL UNIQUE PRIMARY KEY, filename TEXT, modification_date BIGINT, proposal TEXT, status_code INT)'''
                  )
        conn.commit()

        # set up logging
        logfile = self.date.strftime('transfer_logs/fedids_%Y%m%d.txt')
        logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s %(message)s',
                            datefrmt='%m/%d/%y %H:%M:%S')

        # use list from previous step as input to write to postgres
        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')

                # take proposal number from filepath (for whitelist)
                proposal = database_file.split('/')[5].split('-')[0]
                proc = subprocess.Popen(str('getent group ' + str(proposal)), stdout=subprocess.PIPE, shell=True)
                out, err = proc.communicate()

                # need to put modification date to use in the proasis upload scripts
                modification_date = misc_functions.get_mod_date(database_file)
                c.execute('''INSERT INTO soakdb_files (filename, modification_date, proposal) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT filename, modification_date FROM soakdb_files WHERE filename = %s AND modification_date = %s)''', (database_file, int(modification_date), proposal, database_file, int(modification_date)))
                conn.commit()

                logging.info(str('FedIDs written for ' + proposal))

        c.execute('CREATE TABLE IF NOT EXISTS proposals (proposal TEXT, fedids TEXT)')

        proposal_list = []
        c.execute('SELECT proposal FROM soakdb_files')
        rows = c.fetchall()
        for row in rows:
            proposal_list.append(str(row[0]))

        for proposal_number in set(proposal_list):
            proc = subprocess.Popen(str('getent group ' + str(proposal_number)), stdout=subprocess.PIPE, shell=True)
            out, err = proc.communicate()
            append_list = out.split(':')[3].replace('\n', '')

            c.execute(str('''INSERT INTO proposals (proposal, fedids) SELECT %s, %s WHERE NOT EXISTS (SELECT proposal, fedids FROM proposals WHERE proposal = %s AND fedids = %s);'''), (proposal_number, append_list, proposal_number, append_list))
            conn.commit()

        c.close()

        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')

class TransferChangedDataFile(luigi.Task):
    data_file = luigi.Parameter()
    def requires(self):
        pass
    def output(self):
        pass
    def run(self):
        pass


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    def requires(self):
        pass
    def output(self):
        pass
    def run(self):
        pass


class TransferExperiment(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())
    # data_file = luigi.Parameter()

    # needs soakDB list, but not fedIDs - this task needs to be spawned by soakDB class
    def requires(self):
        return FindSoakDBFiles()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('transfer_logs/transfer_experiment_%Y%m%d.txt'))

    def run(self):
        # set up logging
        logfile = self.date.strftime('transfer_logs/transfer_experiment_%Y%m%d.txt')
        logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s %(message)s', datefrmt='%m/%d/%y %H:%M:%S')

        def create_list_from_ind(row, array, numbers_list, crystal_id):
            for ind in numbers_list:
                array.append(row[ind])
            array.append(crystal_id)

        def pop_dict(array, dictionary, dictionary_keys):
            for i in range(0, len(dictionary_keys)):
                dictionary[dictionary_keys[i]].append(array[i])
            return dictionary

        def add_keys(dictionary, keys):
            for key in keys:
                dictionary[key] = []

        lab_dictionary_keys, crystal_dictionary_keys, data_processing_dictionary_keys, dimple_dictionary_keys, \
        refinement_dictionary_keys, dictionaries, lab_dict, crystal_dict, data_collection_dict, \
        data_processing_dict, dimple_dict, refinement_dict = db_functions.define_dicts_and_keys()

        # add keys to dictionaries
        for dictionary in dictionaries:
            add_keys(dictionary[0], dictionary[1])

        # numbers relating to where selected in query
        # 17 = number for crystal_name
        lab_table_numbers = range(0, 21)

        crystal_table_numbers = range(22, 33)
        crystal_table_numbers.insert(len(crystal_table_numbers), 17)

        data_collection_table_numbers = range(33, 36)
        data_collection_table_numbers.insert(len(data_collection_table_numbers), 17)

        data_processing_table_numbers = range(36, 79)
        data_processing_table_numbers.insert(len(data_processing_table_numbers), 17)

        dimple_table_numbers = range(79, 89)
        dimple_table_numbers.insert(len(dimple_table_numbers), 17)

        refinement_table_numbers = range(91, 122)
        refinement_table_numbers.insert(len(refinement_table_numbers), 17)

        # connect to master postgres db
        conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
        c = conn.cursor()

        # get all soakDB file names and close postgres connection
        # change this to take filename from input, and only handle one data file at a time - i.e. when file is selected
        # as new or changed, kick off everything below here as appropriate. Have a function to determine the query to
        # drop rows relating to a file that has changed, and then a function to add rows - don't use pandas to add data
        c.execute('select filename from soakdb_files')
        rows = c.fetchall()
        c.close()

        crystal_list = []

        project_protein = {'datafile': [], 'protein_field': [], 'protein_from_crystal': []}

        # set database filename from postgres query
        for row in rows:

            database_file = str(row[0])

            project_protein['datafile'].append(database_file)

            temp_protein_list = []
            temp_protein_cryst_list = []

            # connect to soakDB
            conn2 = sqlite3.connect(str(database_file))
            c2 = conn2.cursor()

            try:
                # columns with issues: ProjectDirectory, DatePANDDAModelCreated
                for row in db_functions.soakdb_query(c2):

                    temp_protein_list.append(str(row[5]))

                    try:
                        if str(row[17]) in crystal_list:
                            crystal_name = row[17].replace(str(row[17]), str(str(row[17]) + 'I'))
                        if str(row[17].replace(str(row[17]), str(str(row[17]) + 'I'))) in crystal_list:
                            crystal_name = row[17].replace(str(row[17]), str(str(row[17]) + 'II'))
                        else:
                            crystal_name = row[17]

                        temp_protein_cryst_list.append(crystal_name.split('-')[0])

                        crystal_list.append(row[17])
                        crystal_list = list(set(crystal_list))
                    except:
                        logging.warning(str('Database file: ' + database_file + ' WARNING: ' + str(sys.exc_info()[1])))
                        temp_protein_cryst_list.append(str(sys.exc_info()[1]))


                    lab_table_list = []
                    crystal_table_list = []
                    data_collection_table_list = []
                    data_processing_table_list = []
                    dimple_table_list = []
                    refinement_table_list = []

                    lists = [lab_table_list, crystal_table_list, data_collection_table_list, data_processing_table_list,
                             dimple_table_list, refinement_table_list]

                    numbers = [lab_table_numbers, crystal_table_numbers, data_collection_table_numbers,
                               data_processing_table_numbers, dimple_table_numbers, refinement_table_numbers]
                    listref = 0

                    for listname in lists:
                        create_list_from_ind(row, listname, numbers[listref], crystal_name)
                        listref += 1

                    # populate query return into dictionary, so that it can be turned into a df and transfered to DB
                    pop_dict(lab_table_list, lab_dict, lab_dictionary_keys)
                    pop_dict(crystal_table_list, crystal_dict, crystal_dictionary_keys)
                    pop_dict(refinement_table_list, refinement_dict, refinement_dictionary_keys)
                    pop_dict(dimple_table_list, dimple_dict, dimple_dictionary_keys)
                    pop_dict(data_collection_table_list, data_collection_dict,
                             data_collection_dictionary_keys)
                    pop_dict(data_processing_table_list, data_processing_dict,
                             data_processing_dictionary_keys)


                protein_list = list(set(temp_protein_list))
                project_protein['protein_field'].append(protein_list)
                protein_cryst_list = list(set(temp_protein_cryst_list))
                project_protein['protein_from_crystal'].append(protein_cryst_list)

            except:
                logging.warning(str('Database file: ' + database_file + ' WARNING: ' + str(sys.exc_info()[1])))
                project_protein['protein_from_crystal'].append(str(sys.exc_info()[1]))
                project_protein['protein_field'].append(str(sys.exc_info()[1]))
                c2.close()

        print project_protein

        # turn dictionaries into dataframes
        labdf = pandas.DataFrame.from_dict(lab_dict)
        dataprocdf = pandas.DataFrame.from_dict(data_processing_dict)
        refdf = pandas.DataFrame.from_dict(refinement_dict)
        dimpledf = pandas.DataFrame.from_dict(dimple_dict)

        # create a project list
        projectdf = pandas.DataFrame.from_dict(project_protein)
        projectdf.to_csv('project_list.csv')

        # start a postgres engine for data transfer
        xchem_engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')

        # compare dataframes to database rows and remove duplicates
        labdf_nodups = db_functions.clean_df_db_dups(labdf, 'lab', xchem_engine, lab_dictionary_keys)
        dataprocdf_nodups = db_functions.clean_df_db_dups(dataprocdf, 'data_processing', xchem_engine,
                                                          data_processing_dictionary_keys)
        refdf_nodups = db_functions.clean_df_db_dups(refdf, 'refinement', xchem_engine, refinement_dictionary_keys)
        dimpledf_nodups = db_functions.clean_df_db_dups(dimpledf, 'dimple', xchem_engine, dimple_dictionary_keys)

        # append new entries to relevant tables
        labdf_nodups.to_sql('lab', xchem_engine, if_exists='append')
        dataprocdf_nodups.to_sql('data_processing', xchem_engine, if_exists='append')
        refdf_nodups.to_sql('refinement', xchem_engine, if_exists='append')
        dimpledf_nodups.to_sql('dimple', xchem_engine, if_exists='append')