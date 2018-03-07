import luigi
from htmd.ui import *
import os
import subprocess
from rdkit import Chem
from rdkit.Chem import AllChem
from rdkit import Geometry
from rdkit.Chem import rdMolTransforms

class PrepProtein(luigi.Task):
    protein_pdb = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

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
            # self.ssh_command,
            'obabel', protein, '-O', protein.replace('.pdb', '.pdbqt'), '-r'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()


class PrepLigand(luigi.Task):
    ligand_sdf = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

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
            #self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('sdf', 'mol2'), '-p'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()

        # prepare pdbqt from mol2
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '.mol2'))
        command = ' '.join([
            # self.ssh_command,
            'obabel', ligand, '-O', ligand.replace('.mol2', '_prepared.pdbqt')])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()


class VinaDock(luigi.Task):
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ligand_sdf = luigi.Parameter()
    receptor_pdb = luigi.Parameter()
    vina_exe = luigi.Parameter(default='/dls_sw/apps/xchem/autodock_vina_1_1_2_linux_x86/bin/vina')
    box_size = luigi.Parameter(default='[40, 40, 40]')

    def requires(self):
        return PrepLigand(root_dir=self.root_dir, ligand_sdf=self.ligand_sdf), \
               PrepProtein(root_dir=self.root_dir, protein_pdb=self.receptor_pdb)

    def output(self):
        pass

    def run(self):
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '.mol2'))

        mol = Chem.MolFromMol2File(ligand)
        conf = mol.GetConformer()
        centre = rdMolTransforms.ComputeCentroid(conf)

        box_size = eval(self.box_size)

        os.chdir(os.path.join(self.root_dir, self.docking_dir))

        outfile = os.path.join(self.root_dir, self.docking_dir, self.receptor_pdb.replace('.pdb', '_prepared2.pdbqt'))
        if os.path.isfile(outfile):
            os.remove(outfile)

        with open(os.path.join(self.root_dir, self.docking_dir, self.receptor_pdb.replace('.pdb', '_prepared.pdbqt')), 'r') as infile:
            for line in infile:
                if 'ROOT' in line or 'BRANCH' in line or 'TORSDOF' in line:
                    continue
                else:
                    with open(outfile, 'a') as f:
                        f.write(line)

        command = [
            'cd',
            os.path.join(self.root_dir, self.docking_dir),
            ';',
            self.vina_exe,
            '--receptor',
            os.path.join(self.root_dir, self.docking_dir, self.receptor_pdb.replace('.pdb', '_prepared2.pdbqt')),
            '--ligand',
            os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '_prepared.pdbqt')),
            '--center_x',
            centre.x,
            '--center_y',
            centre.y,
            '--center_z',
            centre.z,
            '--size_x',
            str(box_size[0]),
            '--size_y',
            str(box_size[1]),
            '--size_z',
            str(box_size[2]),
        ]

        command = ' '.join(str(v) for v in command)

        print(command)

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()

        print(out)
        print(err)






