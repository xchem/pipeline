import unittest
import luigi
import luigi_classes.prepare_dock
import os, shutil


def run_luigi_worker(task):
    w = luigi.worker.Worker()
    w.add(task)
    w.run()


class TestPFilePrep(unittest.TestCase):
    protein_pdb = 'SHH-x17_apo.pdb'
    ligand_sdf = 'SHH-x17_mol.sdf'
    root_dir = '/dls/science/groups/i04-1/software/luigi_pipeline/tests/docking_files/'
    tmp_dir = 'tmp/'

    @classmethod
    def setUpClass(cls):

        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)

        if not os.path.isdir(cls.working_dir):
            os.mkdir(cls.working_dir)

        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.protein_pdb), cls.working_dir)
        shutil.copy(os.path.join(cls.root_dir, 'comp_chem', cls.ligand_sdf), cls.working_dir)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(cls.working_dir)

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
        run_luigi_worker(luigi_classes.prepare_dock.PrepProtein(
            root_dir=self.working_dir, protein_pdb=self.protein_pdb, docking_dir=''))

        expected_file = os.path.join(self.root_dir, 'comp_chem', self.protein_pdb.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(self.working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt'))

        self.file_checks(expected_file=expected_file, produced_file=produced_file)

    def test_lig_prep(self):
        # run test data on luigi worker
        run_luigi_worker(luigi_classes.prepare_dock.PrepLigand(
            root_dir=self.working_dir, ligand_sdf=self.ligand_sdf, docking_dir=''))

        expected_file = os.path.join(self.root_dir, 'comp_chem', self.ligand_sdf.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(self.working_dir, str(self.ligand_sdf).replace('.pdb', '_prepared.pdbqt'))

        self.file_checks(expected_file=expected_file, produced_file=produced_file)

if __name__ == '__main__':
    unittest.main()


