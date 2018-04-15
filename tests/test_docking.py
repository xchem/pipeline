import os
import shutil
import subprocess
import unittest

import luigi_classes.prepare_dock
import luigi_classes.run_dock
from test_functions import run_luigi_worker
from test_functions import kill_job


class TestFilePrep(unittest.TestCase):
    protein_pdb = 'SHH-x17_apo.pdb'
    ligand_sdf = 'SHH-x17_mol.sdf'
    root_dir = os.path.join(os.getcwd(), 'tests/docking_files/')
    tmp_dir = 'tmp/'

    @classmethod
    def setUpClass(cls):

        cls.top_dir = os.getcwd()

        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)

        if not os.path.isdir(cls.working_dir):
            os.mkdir(cls.working_dir)

        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdb), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_sdf), cls.working_dir)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def file_checks(self, expected_file, produced_file):
        # check the file has been created
        self.assertTrue(os.path.isfile(produced_file))

        # get file info
        info = os.stat(os.path.join(produced_file))

        # check file is not empty
        self.assertNotEqual(info.st_size, 0)

        # check if file contents are the same (ignoring 'REMARK' lines)
        with open(expected_file) as f1, open(produced_file) as f2:
            f1 = filter(self.predicate, f1)
            f2 = filter(self.predicate, f2)
            self.assertTrue(all(x == y for x, y in zip(f1, f2)))

    def predicate(self, line):
        # ingnore as remark lines contain directory of run, which should be different anyway
        if 'REMARK' in line:
            return False  # ignore it
        return True

    def test_protein_prep(self):
        # run test data on luigi worker
        protein_prep = run_luigi_worker(luigi_classes.prepare_dock.PrepProtein(
            root_dir=self.working_dir, protein_pdb=self.protein_pdb, docking_dir=''))

        # check that the job ran successfully
        self.assertTrue(protein_prep)

        # define the file that should have been produced, and the example file
        expected_file = os.path.join(self.root_dir, 'comp_chem', self.protein_pdb.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(self.working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt'))

        # run file checks
        self.file_checks(expected_file=expected_file, produced_file=produced_file)

    def test_lig_prep(self):
        # run test data on luigi worker
        lig_prep = run_luigi_worker(luigi_classes.prepare_dock.PrepLigand(
            root_dir=self.working_dir, ligand_sdf=self.ligand_sdf, docking_dir=''))

        # check that the task was run successfully
        self.assertTrue(lig_prep)

        # define the file that should have been produced, and the example file
        expected_file = os.path.join(self.root_dir, 'comp_chem', self.ligand_sdf.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(self.working_dir, str(self.ligand_sdf).replace('.pdb', '_prepared.pdbqt'))

        # run file checks
        self.file_checks(expected_file=expected_file, produced_file=produced_file)


class TestVina(unittest.TestCase):
    protein_pdbqt = 'SHH-x17_apo_prepared.pdbqt'
    ligand_pdbqt = 'SHH-x17_mol_prepared.pdbqt'
    ligand_mol2 = 'SHH-x17_mol.mol2'
    root_dir = os.path.join(os.getcwd(), 'tests/docking_files/')
    tmp_dir = 'tmp/'

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()

        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)

        if not os.path.isdir(cls.working_dir):
            os.mkdir(cls.working_dir)

        # copy all files needed for vina to run over to tmp directory
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_mol2), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def test_vina(self):
        # run vina task via luigi
        vina_submit = run_luigi_worker(luigi_classes.run_dock.RunVinaDock(root_dir=self.working_dir,
                                                                          docking_dir='',
                                                                          ligand_pdbqt=self.ligand_pdbqt,
                                                                          receptor_pdbqt=self.protein_pdbqt))

        # check job ran successfully, and that a job id was generated from cluster
        self.assertTrue(vina_submit)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir, 'vina.job.id')))

        out, job_id = kill_job(self.working_dir, 'vina.job.id')

        # make sure communicated with queue successfully
        self.assertTrue(str(job_id) in out.decode('ascii'))

    def test_vina_local(self):
        os.chdir(self.working_dir)

        # run the vina job produced by luigi worker in test_vina
        process = subprocess.Popen('chmod 755 vina.sh; ./vina.sh',
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        out, err = process.communicate()

        # check an output file was generated by vina
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir,
                                                    str(self.ligand_mol2.replace('.mol2', '_prepared_vinaout.pdbqt')))))


class TestAutodockPrep(unittest.TestCase):
    protein_pdbqt = 'SHH-x17_apo_prepared.pdbqt'
    ligand_pdbqt = 'SHH-x17_mol_prepared.pdbqt'
    root_dir = os.path.join(os.getcwd(), 'tests/docking_files/')
    tmp_dir = 'tmp/'

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()

        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)

        if not os.path.isdir(cls.working_dir):
            os.mkdir(cls.working_dir)

        # copy all files needed for vina to run over to tmp directory
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_pdbqt), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def test_param_prep(self):
        param_prep = run_luigi_worker(luigi_classes.prepare_dock.ParamPrepADT(ligand_file_name=self.ligand_pdbqt,
                                                                              receptor_file_name=self.protein_pdbqt,
                                                                              docking_dir='',
                                                                              root_dir=self.working_dir))
        self.assertTrue(param_prep)

    def test_grid_prep(self):
        grid_prep = run_luigi_worker(luigi_classes.prepare_dock.GridPrepADT(ligand_file_name=self.ligand_pdbqt,
                                                                            receptor_file_name=self.protein_pdbqt,
                                                                            docking_dir='',
                                                                            root_dir=self.working_dir))
        self.assertTrue(grid_prep)


class TestAutoDock(unittest.TestCase):
    protein_pdbqt = 'SHH-x17_apo_prepared.pdbqt'
    ligand_pdbqt = 'SHH-x17_mol_prepared.pdbqt'
    dpf_file = 'SHH-x17_mol_prepared_SHH-x17_apo_prepared.dpf'
    gpf_file = 'SHH-x17_apo_prepared.gpf'
    root_dir = os.path.join(os.getcwd(), 'tests/docking_files/')
    tmp_dir = 'tmp/'

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()

        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)

        if not os.path.isdir(cls.working_dir):
            os.mkdir(cls.working_dir)

        # copy all files needed for vina to run over to tmp directory
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.dpf_file), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.gpf_file), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def test_run_grid(self):
        run_grid = run_luigi_worker(luigi_classes.run_dock.RunAutoGrid(ligand_pdbqt=self.ligand_pdbqt,
                                                                       receptor_pdbqt=self.protein_pdbqt,
                                                                       docking_dir='',
                                                                       root_dir=self.working_dir))
        self.assertTrue(run_grid)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir, 'autogrid.job.id')))

        out, job_id = kill_job(self.working_dir, 'autogrid.job.id')

        # make sure communicated with queue successfully
        self.assertTrue(str(job_id) in out.decode('ascii'))

    def test_run_autodock(self):
        run_autodock = run_luigi_worker(luigi_classes.run_dock.RunAutoDock(ligand_pdbqt=self.ligand_pdbqt,
                                                                           receptor_pdbqt=self.protein_pdbqt,
                                                                           docking_dir='',
                                                                           root_dir=self.working_dir))
        self.assertTrue(run_autodock)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir, 'autodock.job.id')))

        out, job_id = kill_job(self.working_dir, 'autodock.job.id')

        # make sure communicated with queue successfully
        self.assertTrue(str(job_id) in out.decode('ascii'))

    def test_run_grid_local(self):
        pass

    def test_run_autodock_local(self):
        pass


if __name__ == '__main__':
    unittest.main()
