import luigi
import os
import subprocess
import openbabel


class CutOutEvent(luigi.Task):
    ssh_command = 'ssh uzw12877@nx.diamond.ac.uk'
    directory = luigi.Parameter()
    mapin = luigi.Parameter()
    mapout = luigi.Parameter()
    mol_file = luigi.Parameter()
    border = luigi.Parameter(default='6')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, self.mapout))


    def run(self):
        # convert to pdbqt with obabel
        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('mol', 'pdb')
        mol = openbabel.OBMol()

        # read pdb and write pdbqt
        obConv.ReadFile(mol, os.path.join(self.directory, self.mol_file))
        obConv.WriteFile(mol, os.path.join(self.directory, self.mol_file.replace('.mol', '_mol.pdb')))

        mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
            border %s
            end
        eof
        ''' % (self.mapin, self.mapout, self.mol_file.replace('.mol', '.pdb'), str(self.border))

        process = subprocess.Popen(str(self.ssh_command + ' "' + 'cd ' + self.directory + ';' + mapmask + '"'),
                                   shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = process.communicate()

        # os.remove(os.path.join(self.directory, self.mol_file.replace('.mol', '_mol.pdb')))

        if '(mapmask) - normal termination' not in out:
            raise Exception('mapmask failed!')
