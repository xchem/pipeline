import luigi
import os
import subprocess

from rdkit import Chem
from rdkit.Chem import rdMolTransforms


class WriteAutoGridScript(luigi.Task):
    autogrid4_executable= luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autogrid4')
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    bash_script = luigi.Parameter(default='autogrid.sh')

    def requires(self):
        pass
        # return GridPrepADT()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.bash_script))

    def run(self):
        string = '''#!/bin/bash
cd %s
touch autogrid.running
%s -p %s > autogrid.log
rm autogrid.running
touch autogrid.done
        ''' % (os.path.join(self.root_dir, self.docking_dir), self.autogrid4_executable, self.parameter_file)
        with self.output().open('wb') as f:
            f.write(string)

class WriteAutoDockScript(luigi.Task):
    autodock4_executable= luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autodock4')
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    bash_script = luigi.Parameter(default='autodock.sh')

    def requires(self):
        return ParamPrepADT()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.bash_script))

    def run(self):
        string = '''#!/bin/bash
cd %s
touch autodock.running
%s -p %s > autodock.log
rm autodock.running
touch autodock.done
        ''' % (os.path.join(self.root_dir, self.docking_dir), self.autodock4_executable, self.parameter_file)
        with self.output().open('wb') as f:
            f.write(string)

class RunAutoGrid(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    done_file = luigi.Parameter(default='autogrid.done')

    def requires(self):
        return WriteAutoGridScript(parameter_file=self.parameter_file, root_dir=self.root_dir)

    def output(self):
        pass

    def run(self):
        pass

class RunAutoDock(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    done_file = luigi.Parameter(default='autodock.done')

    def requires(self):
        return WriteAutoDockScript(parameter_file=self.parameter_file, root_dir=self.root_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.done_file))

    def run(self):
        pass


class DLGtoPDBQT(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    dlg_file = luigi.Parameter()

    def requires(self):
        return RunAutoDock(parameter_file = self.parameter_file)

    def output(self):
        return luigi.LocalTarget(self.dlg_file.replace('.dlg', '.pdbqt'))

    def run(self):
        with open(self.dlg_file, 'r') as infile:
            for line in infile:
                if 'DOCKED:' in line:
                    outline = line.replace('DOCKED: ', '')
                    with open(self.output().path, 'a') as f:
                        f.write(outline)

class PDBQTtoPDB(luigi.Task):
    lig_dir = luigi.Parameter(default='autodock_ligands')
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    pdqbqt_file = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


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
