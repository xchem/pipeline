import unittest
import subprocess
import luigi_classes.prepare_dock
import luigi_classes.run_dock
import os, shutil
from test_functions import run_luigi_worker


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
        if 'REMARK' in line:
            return False  # ignore it
        return True

    def test_protein_prep(self):
        # run test data on luigi worker
        protein_prep = run_luigi_worker(luigi_classes.prepare_dock.PrepProtein(
            root_dir=self.working_dir, protein_pdb=self.protein_pdb, docking_dir=''))

        self.assertTrue(protein_prep)

        expected_file = os.path.join(self.root_dir, 'comp_chem', self.protein_pdb.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(self.working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt'))

        # run file checks
        self.file_checks(expected_file=expected_file, produced_file=produced_file)

    def test_lig_prep(self):
        # run test data on luigi worker
        lig_prep = run_luigi_worker(luigi_classes.prepare_dock.PrepLigand(
            root_dir=self.working_dir, ligand_sdf=self.ligand_sdf, docking_dir=''))

        self.assertTrue(lig_prep)

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

        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_pdbqt), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_mol2), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)

    def test_vina(self):
        vina_submit = run_luigi_worker(luigi_classes.run_dock.RunVinaDock(root_dir=self.working_dir,
                                                            docking_dir='', ligand_pdbqt=self.ligand_pdbqt,
                                                            receptor_pdbqt=self.protein_pdbqt))

        self.assertTrue(vina_submit)
        self.assertTrue(os.path.isfile(os.path.join(self.working_dir, 'vina.job.id')))

        with open(os.path.join(self.working_dir, 'vina.job.id'), 'r') as f:
            job_id = int(f.readline())

        process = subprocess.Popen(str('ssh -t uzw12877@nx.diamond.ac.uk '
                                       '"module load global/cluster >>/dev/null 2>&1; qdel ' + str(job_id) + '"'),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        out, err = process.communicate()
        self.assertTrue(str(job_id) in out.decode('ascii'))

    def test_vina_local(self):

        os.chdir(self.working_dir)

        process = subprocess.Popen('chmod 755 vina.sh; ./vina.sh',
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        out, err = process.communicate()

        self.assertTrue(os.path.isfile(os.path.join(self.working_dir,
                                                    str(self.ligand_mol2.replace('.mol2', '_prepared_vinaout.pdbqt')))))


if __name__ == '__main__':
    unittest.main()


