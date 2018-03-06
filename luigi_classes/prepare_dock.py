import luigi
from htmd.ui import *
import os
import subprocess

class PrepProtein(luigi.Task):
    protein_pdb = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdb')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        mol = Molecule(self.protein_pdb)
        prot = proteinPrepare(mol)
        prot.write(self.output().path)

class PrepLigand(luigi.Task):
    ligand_sdf = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_sdf).replace('.sdf', '.mol2')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))

        # prepare mol2 file from sdf
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf)
        command = ' '.join([
            #self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('sdf', 'mol2'), '-p'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()



