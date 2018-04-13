import unittest
import luigi
import luigi_classes.prepare_dock
import os, shutil


def run_luigi_worker(task):
    w = luigi.worker.Worker()
    w.add(task)
    w.run()


def predicate(line):
    if 'REMARK' in line:
        return False # ignore it
    return True


class TestProteinPrep(unittest.TestCase):
    protein_pdb = 'SHH-x17_apo.pdb'
    root_dir = '/dls/science/groups/i04-1/software/luigi_pipeline/tests/docking_files/'
    tmp_dir = 'tmp/'

    def test_protein_prep(self):
        working_dir = os.path.join(os.getcwd(), self.tmp_dir)

        if not os.path.isdir(working_dir):
            os.mkdir(working_dir)

        shutil.copy(os.path.join(self.root_dir, 'comp_chem', self.protein_pdb), working_dir)

        # run test data on luigi worker
        run_luigi_worker(luigi_classes.prepare_dock.PrepProtein(
            root_dir=working_dir, protein_pdb=self.protein_pdb, docking_dir=''))

        # check the file has been created
        self.assertTrue(os.path.isfile(os.path.join(working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt'))))
        # get file info
        info = os.stat(os.path.join(working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt')))
        # check file is not empty
        self.assertNotEqual(info.st_size,0)

        expected_file = os.path.join(self.root_dir, 'comp_chem', self.protein_pdb.replace('.pdb', '_prepared.pdbqt'))
        produced_file = os.path.join(working_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt'))

        with open(expected_file) as f1, open(produced_file) as f2:
            f1 = filter(predicate, f1)
            f2 = filter(predicate, f2)
            # check if file contents are the same (ignoring 'REMARK' lines)
            self.assertTrue(all(x == y for x, y in zip(f1, f2)))

        shutil.rmtree(working_dir)


class TestLigPrep(unittest.TestCase):
    ligand_sdf = 'SHH-x17_mol.sdf'
    root_dir = '/dls/science/groups/i04-1/software/luigi_pipeline/tests/docking_files/'
    tmp_dir = 'tmp/'

    def test_ligand_prep(self):
        pass

if __name__ == '__main__':
    unittest.main()


