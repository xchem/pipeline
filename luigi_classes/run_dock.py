import subprocess

import luigi
from rdkit import Chem
from rdkit.Chem import rdMolTransforms

from functions.docking_functions import *
from .cluster_submission import CheckJobOutput
from .cluster_submission import SubmitJob
from .cluster_submission import WriteJob
from .prepare_dock import PrepProtein, PrepLigand, GridPrepADT, ParamPrepADT


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
    # parameter_flag = luigi.Parameter(default='-p')

    def requires(self):
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(str(self.receptor_pdbqt).replace('.pdbqt', '.gpf')))
        log_file = str(parameter_file).replace('.gpf', '.glg')
        job_options = str('-p ' + parameter_file + ' -l ' + log_file)
        return GridPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                           root_dir=self.root_dir), \
               WriteJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                        job_name=self.job_name, job_executable=self.job_executable, job_options=job_options), \
               SubmitJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename), \
               CheckJobOutput(job_directory=os.path.join(self.root_dir, self.docking_dir), job_output_file=log_file)

    def output(self):
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(self.ligand_pdbqt.replace('.pdbqt', '_') +
                                          str(self.receptor_pdbqt.replace('.pdbqt', '.dpf'))))
        log_file = parameter_file.replace('.dpf', '.dlg')
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(log_file + '.done')))

        # def run(self):
        #     with self.output().open('wb') as f:
        #         f.write('')


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
    # parameter_flag = luigi.Parameter(default='-p')

    def requires(self):
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(self.ligand_pdbqt.replace('.pdbqt', '_') +
                                          str(self.receptor_pdbqt.replace('.pdbqt', '.dpf'))))
        log_file = parameter_file.replace('.dpf', '.dlg')
        print(parameter_file)
        job_options = str(str('-p ' + parameter_file))
        return ParamPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                            root_dir=self.root_dir), \
               RunAutoGrid(root_dir=self.root_dir, receptor_pdbqt=self.receptor_pdbqt, ligand_pdbqt=self.ligand_pdbqt), \
               WriteJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                        job_name=self.job_name, job_executable=self.job_executable, job_options=job_options), \
               SubmitJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename), \
               CheckJobOutput(job_directory=os.path.join(self.root_dir, self.docking_dir), job_output_file=log_file)

    def output(self):
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(self.ligand_pdbqt.replace('.pdbqt', '_') +
                                          str(self.receptor_pdbqt.replace('.pdbqt', '.dpf'))))
        log_file = parameter_file.replace('.dpf', '.dlg')
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(log_file + '.done')))

        # def run(self):
        #     with self.output().open('wb') as f:
        #         f.write('')


class BatchAutoDock(luigi.Task):
    def requires(self):
        to_run = {'root_dir': [],
                  'protein_pdbqt': [],
                  'ligand_pdbqt': []}
        update_apo_field()
        conn, c = dbf.connectDB()
        c.execute("select root_dir, apo_name, mol_name from proasis_out where apo_name!=''")
        rows = c.fetchall()
        for row in rows:
            to_run['root_dir'].append('/'.join(str(row[0]).split('/')[:-1]))
            to_run['protein_pdbqt'].append(str(row[1]).replace('.pdb', '_prepared.pdbqt'))
            to_run['ligand_pdbqt'].append(str(row[2]).replace('.sdf', '_prepared.pdbqt'))

        zipped_list = list(zip(to_run['root_dir'], to_run['protein_pdbqt'], to_run['ligand_pdbqt']))

        return [RunAutoDock(root_dir=root, receptor_pdbqt=receptor, ligand_pdbqt=ligand) for
                               (root, receptor, ligand) in zipped_list]


class RunVinaDock(luigi.Task):
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ligand_sdf = luigi.Parameter()
    receptor_pdb = luigi.Parameter()
    vina_exe = luigi.Parameter(default='/dls_sw/apps/xchem/autodock_vina_1_1_2_linux_x86/bin/vina')
    box_size = luigi.Parameter(default='[40, 40, 40]')
    job_filename = luigi.Parameter(default='vina.sh')
    job_name = luigi.Parameter(default='vina')

    def requires(self):

        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf.replace('.sdf', '.mol2'))
        mol = Chem.MolFromMol2File(ligand)
        conf = mol.GetConformer()
        centre = rdMolTransforms.ComputeCentroid(conf)

        box_size = eval(self.box_size)

        params = [
            '--receptor',
            os.path.join(self.root_dir, self.docking_dir, self.receptor_pdb.replace('.pdb', '_prepared.pdbqt')),
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

        parameters = ' '.join(str(v) for v in params)

        return PrepLigand(root_dir=self.root_dir, ligand_sdf=self.ligand_sdf), \
               PrepProtein(root_dir=self.root_dir, protein_pdb=self.receptor_pdb), \
               WriteJob(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                        job_name=self.job_name, job_executable=self.vina_exe, job_options=parameters), \
               SubmitJob(), \
               CheckJobOutput()

    def output(self):
        pass

