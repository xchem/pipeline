import luigi
import os
import subprocess

from rdkit import Chem
from rdkit.Chem import rdMolTransforms

from cluster_submission import WriteJob
from cluster_submission import SubmitJob
from prepare_dock import PrepProtein, PrepLigand, GridPrepADT, ParamPrepADT


class RunAutoGrid(luigi.Task):

    job_executable = luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autogrid4')

    # job directory = os.path.join \/
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

    job_filename = luigi.Parameter(default='autogrid.sh')
    job_name = luigi.Parameter(default='autogrid')

    receptor_pdbqt = luigi.Parameter()
    ligand_pdbqt = luigi.Parameter()

    # for job_options
    parameter_flag = luigi.Parameter(default='-p')
    parameter_file = luigi.Parameter(
        default=os.path.join(root_dir, docking_dir, str(receptor_pdbqt.replace('.pdbqt', '.gpf'))))

    def requires(self):
        job_options = str(self.parameter_flag + ' ' + self.parameter_file)
        return GridPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                           root_dir=self.root_dir), \
               WriteJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                        job_name=self.job_name, job_executable=self.job_executable, job_options=job_options), \
               SubmitJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, 'runautogrid.done'))

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


class RunAutoDock(luigi.Task):

    job_executable = luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autodock4')

    # job directory = os.path.join \/
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

    job_filename = luigi.Parameter(default='autodock.sh')
    job_name = luigi.Parameter(default='autodock')

    receptor_pdbqt = luigi.Parameter()
    ligand_pdbqt = luigi.Parameter()

    # for job_options
    parameter_flag = luigi.Parameter(default='-p')
    parameter_file = luigi.Parameter(default=os.path.join(root_dir, docking_dir,
                                                          str(ligand_pdbqt.replace('.pdbqt', '_') +
                                                              str(receptor_pdbqt.replace('.pdbqt', '.dpf')))))

    def requires(self):
        job_options = str(self.parameter_flag + ' ' + self.parameter_file)
        return ParamPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                            root_dir=self.root_dir),\
               WriteJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                        job_name=self.job_name, job_executable=self.job_executable, job_options=job_options), \
               SubmitJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, 'runautodock.done'))

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


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

