import os
import shutil

import luigi
import pandas as pd
from sqlalchemy import create_engine

import functions.db_functions as dbf
import functions.proasis_api_funcs as paf


class PullCurated(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands_list = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'curated.pdb')))

    def run(self):
        results_dict = {'strucid': [], 'root_dir': [], 'curated_name': [], 'apo_name': [], 'mtz_name': [],
                        'twofofc_name': [], 'fofc_name': [], 'mol_name': [], 'ligands': []}

        if not os.path.isdir(os.path.join(self.root_dir, self.docking_dir)):
                if not os.access(os.path.join(self.root_dir, self.docking_dir), os.W_OK):
                    return None
                if os.path.isdir(os.path.join(self.root_dir, self.docking_dir)) and \
                        os.access(os.path.join(self.root_dir, self.docking_dir), os.W_OK):
                    os.remove(os.path.join(self.root_dir, self.docking_dir))
                os.mkdir(os.path.join(self.root_dir, self.docking_dir))

        out_file = self.output().path
        print(out_file)
        curated_pdb = paf.get_struc_file(self.strucid, out_file, 'curatedpdb')

        results_dict['strucid'].append(self.strucid)
        results_dict['root_dir'].append(os.path.join(self.root_dir, self.docking_dir))
        results_dict['curated_name'].append(curated_pdb.split('/')[-1])
        results_dict['ligands'].append(self.ligands_list)

        for key in list(results_dict.keys()):
            if len(results_dict[key]) == 0:
                results_dict[key].append('')

        frame = pd.DataFrame.from_dict(results_dict)
        print(frame)
        xchem_engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')
        frame.to_sql('proasis_out', xchem_engine, if_exists='append')


class CreateApo(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal,
                           ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'apo.pdb')))

    def run(self):
        self.ligands = eval(self.ligands)
        print((len(self.ligands)))
        # if len(list(self.ligands))>1:
        # raise Exception('Structures containing more than 1 ligand are currently unsupported')
        conn, c = dbf.connectDB()
        c.execute('SELECT curated_name from proasis_out WHERE strucid=%s', (self.strucid,))
        rows = c.fetchall()
        print((len(rows)))
        if len(rows) > 1:
            raise Exception('Multiple files where found for this structure: ' + str(rows))
        if len(rows)>0 and len(rows[0]) == 0:
            # raise Exception('No entries found for this strucid... check the datasource!')
            c.execute('DELETE from proasis_out WHERE curated_name=%s', (str(self.crystal + '_' + 'curated.pdb'),))
            conn.commit()
            shutil.rmtree(os.path.join(self.root_dir, self.docking_dir))
            raise Exception('DB problem... resetting the datasource and files for this crystal')

        for row in rows:
            curated_pdb = str(row[0])
        try:
            print(curated_pdb)
        except:
            c.execute('DELETE from proasis_out WHERE curated_name=%s', (str(self.crystal + '_' + 'curated.pdb'),))
            conn.commit()
            shutil.rmtree(os.path.join(self.root_dir, self.docking_dir))
            raise Exception('DB problem... resetting the datasource and files for this crystal')

        ligand_string = paf.get_lig_strings(self.ligands)

        working_dir = os.getcwd()
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        try:
            pdb_file = open(curated_pdb, 'r')
        except:
            raise Exception(str(rows))
        for line in pdb_file:
            if any(lig in line for lig in ligand_string):
                continue
            else:
                with open(self.output().path, 'a') as f:
                    f.write(line)
        os.chdir(working_dir)


class PullMol(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal,
                           ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'mol.sdf')))

    def run(self):
        out_file = self.output().path
        print(out_file)
        mol_sdf = paf.get_struc_file(self.strucid, out_file, 'sdf')

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET mol_name = %s WHERE strucid = %s', (mol_sdf.split('/')[-1], self.strucid))
        conn.commit()


class PullMtz(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal,
                           ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'mtz.mtz')))

    def run(self):
        out_mtz = paf.get_struc_mtz(self.strucid, os.path.join(self.root_dir, self.docking_dir))
        out_mtz = os.path.join(self.root_dir, self.docking_dir, out_mtz)
        shutil.move(out_mtz, self.output().path)

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET mtz_name = %s WHERE strucid = %s',
                  (self.output().path.split('/')[-1], self.strucid))
        conn.commit()


class PullFofc(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal,
                           ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'fofc.map')))

    def run(self):
        out_fofc = paf.get_struc_map(self.strucid, os.path.join(self.root_dir, self.docking_dir), 'fofc')
        out_fofc = os.path.join(self.root_dir, self.docking_dir, out_fofc)
        shutil.move(out_fofc, self.output().path)

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET fofc_name = %s WHERE strucid = %s',
                  (self.output().path.split('/')[-1], self.strucid))
        conn.commit()


class Pull2Fofc(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal,
                           ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + '2fofc.map')))

    def run(self):
        out_2fofc = paf.get_struc_map(self.strucid, os.path.join(self.root_dir, self.docking_dir), '2fofc')
        out_2fofc = os.path.join(self.root_dir, self.docking_dir, out_2fofc)
        shutil.move(out_2fofc, self.output().path)

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET twofofc_name = %s WHERE strucid = %s',
                  (self.output().path.split('/')[-1], self.strucid))
        conn.commit()
