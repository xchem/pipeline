import os

import luigi

import functions.db_functions as dbf


class DLGtoPDBQT(luigi.Task):
    # parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    dlg_file = luigi.Parameter()

    def requires(self):
        pass
        # return RunAutoDock(parameter_file=self.parameter_file)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.dlg_file).replace('.dlg',
                                                                                                          '.pdbqt')))

    def run(self):
        print(os.path.join(self.root_dir, self.docking_dir, self.dlg_file))
        with open(os.path.join(self.root_dir, self.docking_dir, self.dlg_file), 'r') as infile:
            for line in infile:
                print(line)
                if 'DOCKED:' in line:
                    # print(line)
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


class RemoveADFiles(luigi.Task):
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    # autodock_dir = luigi.Parameter(default='autodock')

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        move_from_dir = os.path.join(self.root_dir, self.docking_dir)
        # move_to_dir = os.path.join(move_from_dir, self.autodock_dir)

        # if not os.path.isdir(move_to_dir):
        # os.mkdir(move_to_dir)

        types_to_move = ['autodock*', 'autogrid*', '*.*.map', '*.fld', '*.xyz', '*.dlg', '*.dpf', '*.gpf', '*.glg*']

        for extension in types_to_move:
            os.system(str('rm ' + move_from_dir + '/' + extension))


class RemoveAllADFiles(luigi.Task):

    def requires(self):
        conn, c = dbf.connectDB()
        c.execute('select root_dir from proasis_out')
        run_list = []
        for row in c.fetchall():
            run_list.append(str(row[0]).replace('comp_chem', ''))
        return [RemoveADFiles(root_dir=direc) for direc in run_list]
