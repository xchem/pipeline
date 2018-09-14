import luigi
import openbabel
from rdkit import Chem
from rdkit.Chem import rdMolTransforms

from functions.docking_functions import *
from .cluster_submission import submit_job
from .cluster_submission import write_job
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

    def requires(self):
        # 1. Prepare autogrid parameter file with autodock tools
        # 2. Write a job script for autodock
        # 3. Run autodock on the cluster
        # 4. Check that the job has finished - doesn't check for errors. This needs a separate class
        return GridPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                           root_dir=self.root_dir, docking_dir=self.docking_dir)
        # CheckJobOutput(job_directory=os.path.join(self.root_dir, self.docking_dir), job_output_file=log_file)

    def run(self):
        # grid parameter file output by ADT
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(str(self.receptor_pdbqt).replace('.pdbqt', '.gpf')))

        # log file to be written out by AutoGrid
        log_file = str(parameter_file).replace('.gpf', '.glg')

        # parameters to be passes to WriteJobScript
        job_options = str('-p ' + parameter_file + ' -l ' + log_file)

        write_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                  job_name=self.job_name, job_executable=self.job_executable, job_options=job_options)

        submit_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(os.path.join(self.root_dir, self.docking_dir),
                         str(str(self.job_filename).replace('.sh', '.job.id'))))


class RunAutoDock(luigi.Task):
    job_executable = luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autodock4')

    # job directory = os.path.join \/
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')

    job_filename = luigi.Parameter(default='autodock.sh')
    job_name = luigi.Parameter(default='autodock')

    receptor_pdbqt = luigi.Parameter()
    ligand_pdbqt = luigi.Parameter()

    def requires(self):
        # 1. Prepare parameters for docking with autodock tools
        # 2. Run Autogrid
        # 3. Write the job for autodock
        # 4. Run the autodock job on the cluster
        # 5. Check that the job has finished - doesn't check for errors. This needs a separate class

        # grid parameter file output by ADT
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(str(self.receptor_pdbqt).replace('.pdbqt', '.gpf')))

        # log file to be written out by AutoGrid
        log_file = str(parameter_file).replace('.gpf', '.glg')

        return [ParamPrepADT(receptor_file_name=self.receptor_pdbqt, ligand_file_name=self.ligand_pdbqt,
                             root_dir=self.root_dir, docking_dir=self.docking_dir),
                RunAutoGrid(root_dir=self.root_dir, receptor_pdbqt=self.receptor_pdbqt, ligand_pdbqt=self.ligand_pdbqt,
                            docking_dir=self.docking_dir)
                # CheckJobOutput(job_directory=os.path.join(self.root_dir, self.docking_dir), job_output_file=log_file)
                ]

    def run(self):
        # docking parameter file written out by autodock tools
        parameter_file = os.path.join(self.root_dir, self.docking_dir,
                                      str(self.ligand_pdbqt.replace('.pdbqt', '_') +
                                          str(self.receptor_pdbqt.replace('.pdbqt', '.dpf'))))

        # specify the dpf file for autodock (WriteJob)
        job_options = str(str('-p ' + parameter_file))

        write_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                  job_name=self.job_name, job_executable=self.job_executable, job_options=job_options)

        submit_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(os.path.join(self.root_dir, self.docking_dir),
                         str(str(self.job_filename).replace('.sh', '.job.id'))))


class RunVinaDock(luigi.Task):
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ligand_pdbqt = luigi.Parameter()
    receptor_pdbqt = luigi.Parameter()
    vina_exe = luigi.Parameter(default='/dls_sw/apps/xchem/autodock_vina_1_1_2_linux_x86/bin/vina')
    box_size = luigi.Parameter(default='[40, 40, 40]')
    job_filename = luigi.Parameter(default='vina.sh')
    job_name = luigi.Parameter(default='vina')

    def requires(self):
        # 1. Prep Ligand (should have already been done for autodock)
        # 2. Prep Protein (should have already been done for autodock)
        # 3. Write vina job script
        # 4. Run vina on cluster
        # 5. Check job output
        return [PrepLigand(docking_dir=self.docking_dir, root_dir=self.root_dir,
                           ligand_sdf=self.ligand_pdbqt.replace('_prepared.pdbqt', '.sdf')),
                PrepProtein(docking_dir=self.docking_dir, root_dir=self.root_dir,
                            protein_pdb=self.receptor_pdbqt.replace('_prepared.pdbqt', '.pdb'))]

        # CheckJobOutput(job_directory=os.path.join(self.root_dir, self.docking_dir), job_output_file=out_name)]

    def run(self):
        # open ligand mol2 file (generated during PrepLigand)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_pdbqt.replace('_prepared.pdbqt', '.mol2'))

        # create an rdkit mol from ligand
        mol = Chem.MolFromMol2File(ligand)

        if mol is None:
            # convert to mol with obabel
            obConv = openbabel.OBConversion()
            obConv.SetInAndOutFormats('mol2', 'mol')

            mol = openbabel.OBMol()

            # read pdb and write pdbqt
            obConv.ReadFile(mol, ligand)
            obConv.WriteFile(mol, ligand.replace('.mol2', '.mol'))

            ligand = ligand.replace('.mol2', '.mol')
            mol = Chem.MolFromMolFile(ligand)

        # get the ligand conformer and find its' centroid
        conf = mol.GetConformer()
        centre = rdMolTransforms.ComputeCentroid(conf)  # out = centre.x, centre.y and centre.z for coords

        # box size allowed for vina
        box_size = eval(self.box_size)

        # name of output file from vina
        out_name = str(self.ligand_pdbqt).replace('.pdbqt', '_vinaout.pdbqt')

        params = [
            '--receptor',
            os.path.join(self.root_dir, self.docking_dir, self.receptor_pdbqt),
            '--ligand',
            os.path.join(self.root_dir, self.docking_dir, self.ligand_pdbqt),
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
            '--out',
            out_name
        ]

        parameters = ' '.join(str(v) for v in params)

        write_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_filename=self.job_filename,
                  job_name=self.job_name, job_executable=self.vina_exe, job_options=parameters)

        submit_job(job_directory=os.path.join(self.root_dir, self.docking_dir), job_script=self.job_filename)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(os.path.join(self.root_dir, self.docking_dir),
                         str(str(self.job_filename).replace('.sh', '.job.id'))))


class BatchDock(luigi.Task):
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

        return [[RunAutoDock(root_dir=root, receptor_pdbqt=receptor, ligand_pdbqt=ligand) for
                 (root, receptor, ligand) in zipped_list],
                [RunVinaDock(root_dir=root, receptor_pdbqt=receptor, ligand_pdbqt=ligand) for
                 (root, receptor, ligand) in zipped_list]]

    def output(self):
        return luigi.LocalTarget('logs/batchdock.done')

    def run(self):
        with self.output().open('wb') as f:
            f.write('')
