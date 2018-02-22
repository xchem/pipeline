import luigi
import functions.db_functions as dbf
import functions.docking_functions as dock
import functions.proasis_api_funcs as paf
import os
from sqlalchemy import create_engine
import pandas as pd
import shutil
import subprocess

class FindCompChemReady(luigi.Task):

    def requires(self):
        run_list = dock.get_comp_chem_ready()
        out_dict = dock.get_strucids(run_list)

        return [PullMol(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [PullMtz(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [Pull2Fofc(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [CreateApo(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [PullFofc(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])]

    def output(self):
        return luigi.LocalTarget(os.path.join('logs', 'findcc.done'))

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


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
                        'twofofc_name':[], 'fofc_name':[], 'mol_name':[], 'ligands':[]}

        if not os.path.isdir(os.path.join(self.root_dir, self.docking_dir)):
            try:
                os.mkdir(os.path.join(self.root_dir, self.docking_dir))
            except:
                os.remove(os.path.join(self.root_dir, self.docking_dir))
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
        print len(self.ligands)
        if len(list(self.ligands))>1:
            raise Exception('Structures containing more than 1 ligand are currently unsupported')
        conn, c = dbf.connectDB()
        c.execute('SELECT curated_name from proasis_out WHERE strucid=%s', (self.strucid,))
        rows = c.fetchall()
        if len(rows)>1:
            raise Exception('Multiple files where found for this structure: ' + str(rows))
        for row in rows:
            curated_pdb = str(row[0])

        ligand_string = paf.get_lig_strings(self.ligands)[0]
        working_dir = os.getcwd()
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        pdb_file = open(curated_pdb , 'r')
        for line in pdb_file:
            if ligand_string in line:
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
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal, ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'mol.sdf')))

    def run(self):
        out_file = self.output().path
        print out_file
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
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal, ligands_list=self.ligands)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.crystal + '_' + 'mtz.mtz')))

    def run(self):
        out_mtz = paf.get_struc_mtz(self.strucid, os.path.join(self.root_dir, self.docking_dir))
        out_mtz = os.path.join(self.root_dir, self.docking_dir, out_mtz)
        shutil.move(out_mtz, self.output().path)

        conn, c = dbf.connectDB()

        c.execute('UPDATE proasis_out SET mtz_name = %s WHERE strucid = %s', (self.output().path.split('/')[-1], self.strucid))
        conn.commit()

class PullFofc(luigi.Task):
    strucid = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    crystal = luigi.Parameter()
    ligands = luigi.Parameter()

    def requires(self):
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal, ligands_list=self.ligands)

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
        return PullCurated(strucid=self.strucid, root_dir=self.root_dir, crystal=self.crystal, ligands_list=self.ligands)

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

class ReceptorPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_receptor4_script = luigi.Parameter(default=
                                             '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                             'AutoDockTools/Utilities24/prepare_receptor4.py')
    receptor_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdb', 'pdbqt'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        command = ' '.join([self.ssh_command, self.pythonsh_executable, self.prepare_receptor4_script, '-r', receptor, '-o', self.output().path])
        print command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print out
        print err

class LigPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_ligand4_script = luigi.Parameter(default=
                                               '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                               'AutoDockTools/Utilities24/prepare_ligand4.py')
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.ligand_file_name.replace('sdf', 'pdbqt'))))

    def run(self):
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join([self.ssh_command, 'obabel', ligand, '-O', ligand.replace('sdf', 'mol2')])
        print command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print out
        print err
        command = ' '.join(
            [self.ssh_command, self.pythonsh_executable, self.prepare_ligand4_script, '-l', ligand.replace('sdf', 'mol2'), '-o',
             self.output().path])
        print command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print out
        print err

class GridPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_gpf4_script = luigi.Parameter(default=
                                               '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                               'AutoDockTools/Utilities24/prepare_gpf4.py')
    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdbqt', 'gpf'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join(
            [self.ssh_command,'"','cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_gpf4_script, '-r', receptor, '-l', ligand, '"'])
        print command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print out
        print err

class ParamPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_dpf4_script = luigi.Parameter(default=
                                          '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                          'AutoDockTools/Utilities24/prepare_dpf4.py')
    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_file_name.replace('.pdbqt', '_') +
                                                  str(self.receptor_file_name.replace('.pdbqt', '.dpf')))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join(
            [self.ssh_command, '"', 'cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_dpf4_script, '-r', receptor, '-l', ligand, '"'])
        print command
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print out
        print err
