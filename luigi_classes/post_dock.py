import luigi
from run_dock import RunAutoDock


class DLGtoPDBQT(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    dlg_file = luigi.Parameter()

    def requires(self):
        return RunAutoDock(parameter_file=self.parameter_file)

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