import subprocess

import luigi
from htmd.ui import *

from functions.docking_functions import *


class PrepProtein(luigi.Task):
    protein_pdb = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        mol = Molecule(self.protein_pdb)
        prot = proteinPrepare(mol)
        prot.remove('resname WAT')
        prot.write(str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))

        protein = os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))
        command = ' '.join([
            self.ssh_command,
            'obabel', protein, '-O', protein.replace('.pdb', '.pdbqt'), '-xr'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)


class PrepLigand(luigi.Task):
    ligand_sdf = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_sdf).replace('.sdf', '_prepared.pdbqt')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))

        # prepare mol2 file from sdf
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf)
        command = ' '.join([
            self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('sdf', 'mol2'), '-p'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

        # prepare pdbqt from mol2
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '.mol2'))
        command = ' '.join([
            self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('.mol2', '_prepared.pdbqt')])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)


class GridPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_gpf4_script = luigi.Parameter(default=
                                          '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                          'AutoDockTools/Utilities24/prepare_gpf4.py')
    # additional parameters for autodock
    ad_parameter_file = luigi.Parameter(default='-p parameter_file=/dls_sw/apps/xchem/autodock/AD4_parameters.dat')
    box_size = luigi.Parameter(default='-p npts="40,40,40" -p spacing=0.375')

    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        # return LigPrepADT(), ReceptorPrepADT()
        return PrepProtein(protein_pdb=str(self.receptor_file_name).replace('_prepared.pdbqt', '.pdb'),
                           root_dir=self.root_dir), PrepLigand(
            ligand_sdf=str(self.receptor_file_name).replace('_prepared.pdbqt', '.sdf'), root_dir=self.root_dir)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdbqt', 'gpf'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)

        command = ' '.join(
            [self.ssh_command, '"', 'cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_gpf4_script, '-r', receptor, '-l', ligand,
             self.ad_parameter_file, self.box_size, '"'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)


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
        # return LigPrepADT(), ReceptorPrepADT()
        return PrepProtein(protein_pdb=str(self.receptor_file_name).replace('_prepared.pdbqt', '.pdb'),
                           root_dir=self.root_dir), PrepLigand(
            ligand_sdf=str(self.receptor_file_name).replace('_prepared.pdbqt', '.sdf'), root_dir=self.root_dir)

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
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)


class BatchPrep(luigi.Task):
    def requires(self):
        to_run = {'root_dir': [],
                  'protein_pdb': [],
                  'ligand_sdf': []}
        update_apo_field()
        conn, c = dbf.connectDB()
        c.execute("select root_dir, apo_name, mol_name from proasis_out where apo_name!=''")
        rows = c.fetchall()
        for row in rows:
            to_run['root_dir'].append('/'.join(str(row[0]).split('/')[:-1]))
            to_run['protein_pdb'].append(str(row[1]))
            to_run['ligand_sdf'].append(str(row[2]))

        zipped_list = list(zip(to_run['root_dir'], to_run['protein_pdb'], to_run['ligand_sdf']))

        return [PrepProtein(protein_pdb=protein_pdb, root_dir=root_dir) for (root_dir, protein_pdb, _) in
                zipped_list], [PrepLigand(root_dir=root_dir, ligand_sdf=ligand_sdf) for (root_dir, _, ligand_sdf) in
                               zipped_list]
