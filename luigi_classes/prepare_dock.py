import luigi
from htmd.ui import *
import os
import subprocess


class PrepProtein(luigi.Task):
    protein_pdb = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        mol = Molecule(self.protein_pdb)
        prot = proteinPrepare(mol)
        prot.remove('resname WAT')
        prot.write(str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))

        protein = os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))
        command = ' '.join([
            self.ssh_command,
            'obabel', protein, '-O', protein.replace('.pdb', '.pdbqt'), '-r'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()


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

        # prepare pdbqt from mol2
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '.mol2'))
        command = ' '.join([
            self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('.mol2', '_prepared.pdbqt')])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()


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
        # return LigPrepADT(), ReceptorPrepADT()
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
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

