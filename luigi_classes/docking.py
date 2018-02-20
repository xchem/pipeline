import luigi
import functions.db_functions as dbf
import functions.docking_functions as dock
import functions.proasis_api_funcs as paf
import os
from sqlalchemy import create_engine
import pandas as pd

class FindCompChemReady(luigi.Task):

    def requires(self):
        run_list = dock.get_comp_chem_ready()
        out_dict = dock.get_strucids(run_list)

        return [PullCurated(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands_list=ligands_list) for
                (strucid, root_dir, crystal, ligands_list) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [PullMol(strucid=strucid, root_dir=root_dir, crystal=crystal) for (strucid, root_dir, crystal) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'])]

    def output(self):
        pass

    def run(self):
        pass


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
        results_dict = {'strucid':[], 'root_dir':[], 'curated_name':[], 'apo_name':[], 'mtz_name':[],
                        '2fofc_name':[], 'fofc_name':[], 'mol_name':[], 'ligands':[]}

        if not os.path.isdir(os.path.join(self.root_dir, self.docking_dir)):
            os.mkdir(os.path.join(self.root_dir, self.docking_dir))

        out_file = self.output().path
        print out_file
        curated_pdb = paf.get_struc_file(self.strucid, out_file, 'curatedpdb')

        results_dict['strucid'].append(self.strucid)
        results_dict['root_dir'].append(os.path.join(self.root_dir, self.docking_dir))
        results_dict['curated_name'].append(curated_pdb.split('/')[-1])
        results_dict['ligands'].append(self.ligands_list)

        for key in results_dict.keys():
            if len(results_dict[key])==0:
                results_dict[key].append('')

        frame = pd.DataFrame.from_dict(results_dict)
        print frame
        xchem_engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')
        frame.to_sql('proasis_out', xchem_engine, if_exists='append')



class CreateApo(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class PullMol(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'mol.sdf')))

    def run(self):
        out_file = self.output().path
        print out_file
        mol_sdf = paf.get_struc_file(self.strucid, out_file, 'sdf')

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET mol_name = %s WHERE strucid = %s', (mol_sdf.split('/')[-1], self.strucid))
        conn.commit()


class PullMaps(luigi.Task):
    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass