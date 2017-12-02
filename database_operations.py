import luigi
import psycopg2
import sqlite3
import subprocess
import sys
import os
import datetime


class FindSoakDBFiles(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):
        process = subprocess.Popen('''find /dls/labxchem/data/*/lb*/* -maxdepth 4 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -name "soakDBDataFile.sqlite" -print''',
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        out, err = process.communicate()

        print out
        print err

        with self.output().open('w') as f:
            f.write(out)

        f.close()


class WriteWhitelists(luigi.Task):
    pass

class WriteBlacklist(luigi.Task):
    pass

class TransferDB(luigi.Task):
    def requires(self):
        return FindSoakDBFiles()

    def run(self):
        conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS soakdb_files (filename TEXT, modification_date BIGINT, proposal TEXT)'''
                  )
        conn.commit()

        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')
                proposal = database_file.split('/')[5].split('-')[0]
                proc = subprocess.Popen(str('getent group ' + str(proposal)), stdout=subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                fedids_forsqltable = str(out.split(':')[3].replace('\n', ''))
                modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(database_file)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                modification_date = modification_date.replace('-', '')
                modification_date = modification_date.replace(':', '')
                modification_date = modification_date.replace(' ', '')
                c.execute('''INSERT INTO soakdb_files (filename, modification_date, proposal) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT filename, modification_date FROM soakdb_files WHERE filename = %s AND modification_date = %s)''', (database_file, int(modification_date), proposal, database_file, int(modification_date)))
                conn.commit()

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
