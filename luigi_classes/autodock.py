import luigi
import os
import subprocess

class ReceptorPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_receptor4_script = luigi.Parameter(default=
                                             '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                             'AutoDockTools/Utilities24/prepare_receptor4.py')
    receptor_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdb', 'pdbqt'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        command = ' '.join([self.ssh_command, self.pythonsh_executable, self.prepare_receptor4_script, '-r', receptor, '-o', self.output().path])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

class LigPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_ligand4_script = luigi.Parameter(default=
                                               '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                               'AutoDockTools/Utilities24/prepare_ligand4.py')
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.ligand_file_name.replace('sdf', 'pdbqt'))))

    def run(self):
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join([self.ssh_command, 'obabel', ligand, '-O', ligand.replace('sdf', 'mol2')])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)
        command = ' '.join(
            [self.ssh_command, self.pythonsh_executable, self.prepare_ligand4_script, '-l', ligand.replace('sdf', 'mol2'), '-o',
             self.output().path])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

class GridPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_gpf4_script = luigi.Parameter(default=
                                               '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                               'AutoDockTools/Utilities24/prepare_gpf4.py')
    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        return LigPrepADT(), ReceptorPrepADT()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdbqt', 'gpf'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join(
            [self.ssh_command,'"','cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_gpf4_script, '-r', receptor, '-l', ligand, '"'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

class ParamPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_dpf4_script = luigi.Parameter(default=
                                          '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                          'AutoDockTools/Utilities24/prepare_dpf4.py')
    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        return LigPrepADT(), ReceptorPrepADT()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_file_name.replace('.pdbqt', '_') +
                                                  str(self.receptor_file_name.replace('.pdbqt', '.dpf')))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join(
            [self.ssh_command, '"', 'cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_dpf4_script, '-r', receptor, '-l', ligand, '"'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

class WriteAutoGridScript(luigi.Task):
    autogrid4_executable= luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autogrid4')
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    bash_script = luigi.Parameter(default='autogrid.sh')

    def requires(self):
        pass
        # return GridPrepADT()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.bash_script))

    def run(self):
        string = '''#!/bin/bash
cd %s
touch autogrid.running
%s -p %s > autogrid.log
rm autogrid.running
touch autogrid.done
        ''' % (os.path.join(self.root_dir, self.docking_dir), self.autogrid4_executable, self.parameter_file)
        with self.output().open('wb') as f:
            f.write(string)

class WriteAutoDockScript(luigi.Task):
    autodock4_executable= luigi.Parameter(default='/dls_sw/apps/xchem/autodock/autodock4')
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    bash_script = luigi.Parameter(default='autodock.sh')

    def requires(self):
        return ParamPrepADT()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.bash_script))

    def run(self):
        string = '''#!/bin/bash
cd %s
touch autodock.running
%s -p %s > autodock.log
rm autodock.running
touch autodock.done
        ''' % (os.path.join(self.root_dir, self.docking_dir), self.autodock4_executable, self.parameter_file)
        with self.output().open('wb') as f:
            f.write(string)

class RunAutoGrid(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    done_file = luigi.Parameter(default='autogrid.done')

    def requires(self):
        return WriteAutoGridScript(parameter_file=self.parameter_file, root_dir=self.root_dir)

    def output(self):
        pass

    def run(self):
        pass

class RunAutoDock(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    done_file = luigi.Parameter(default='autodock.done')

    def requires(self):
        return WriteAutoDockScript(parameter_file=self.parameter_file, root_dir=self.root_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir, self.done_file))

    def run(self):
        pass


class DLGtoPDBQT(luigi.Task):
    parameter_file = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    dlg_file = luigi.Parameter()

    def requires(self):
        return RunAutoDock(parameter_file = self.parameter_file)

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
