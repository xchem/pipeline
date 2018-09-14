import shutil
import subprocess

import luigi
import openbabel
from htmd.ui import *

from functions.docking_functions import *


class PrepProtein(luigi.Task):
    protein_pdb = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdbqt')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))
        mol = Molecule(self.protein_pdb)
        prot = proteinPrepare(mol)
        prot.remove('resname WAT')
        prot.write(str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))

        protein = os.path.join(self.root_dir, self.docking_dir, str(self.protein_pdb).replace('.pdb', '_prepared.pdb'))

        # convert to pdbqt with obabel
        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('pdb', 'pdbqt')

        # set protein to be rigid
        obConv.AddOption('x')
        obConv.AddOption('r')
        mol = openbabel.OBMol()

        # read pdb and write pdbqt
        obConv.ReadFile(mol, protein)
        obConv.WriteFile(mol, protein.replace('.pdb', '.pdbqt'))


class PrepLigand(luigi.Task):
    ligand_sdf = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_sdf).replace('.sdf', '_prepared.pdbqt')))

    def run(self):
        os.chdir(os.path.join(self.root_dir, self.docking_dir))

        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_sdf)

        # convert to pdbqt with obabel
        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('sdf', 'pdbqt')

        mol = openbabel.OBMol()

        # read pdb and write pdbqt
        obConv.ReadFile(mol, ligand)
        obConv.WriteFile(mol, ligand.replace('.sdf', '_prepared.pdbqt'))

        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('sdf', 'mol2')

        mol = openbabel.OBMol()

        # read pdb and write mol2
        obConv.ReadFile(mol, ligand)
        obConv.WriteFile(mol, ligand.replace('.sdf', '.mol2'))


class GridPrepADT(luigi.Task):
    pythonsh_executable = luigi.Parameter(default='/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/bin/pythonsh')
    prepare_gpf4_script = luigi.Parameter(default=
                                          '/dls_sw/apps/xchem/mgltools_i86Linux2_1.5.6/MGLToolsPckgs/'
                                          'AutoDockTools/Utilities24/prepare_gpf4.py')
    # additional parameters for autodock
    ad_parameter_file = luigi.Parameter(default='-p parameter_file=/dls_sw/apps/xchem/autodock/AD4_parameters.dat')
    box_size = luigi.Parameter(default='-p npts="40,40,40" -p spacing=0.375')

    receptor_file_name = luigi.Parameter()
    ligand_file_name = luigi.Parameter()
    root_dir = luigi.Parameter()
    docking_dir = luigi.Parameter(default='comp_chem')
    ssh_command = luigi.Parameter(default='ssh -t uzw12877@cs04r-sc-serv-38.diamond.ac.uk')

    def requires(self):
        # return LigPrepADT(), ReceptorPrepADT()
        return PrepProtein(protein_pdb=str(self.receptor_file_name).replace('_prepared.pdbqt', '.pdb'),
                           root_dir=self.root_dir, docking_dir=self.docking_dir), \
               PrepLigand(ligand_sdf=str(self.ligand_file_name).replace('_prepared.pdbqt', '.sdf'),
                          root_dir=self.root_dir, docking_dir=self.docking_dir)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.root_dir, self.docking_dir, str(self.receptor_file_name.replace('pdbqt', 'gpf'))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)

        command = ' '.join(
            [self.ssh_command, '"', 'cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_gpf4_script, '-r', receptor, '-l', ligand,
             self.ad_parameter_file, self.box_size, '"'])
        print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print('\n')
        print(out)
        print('\n')
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
        # return LigPrepADT(), ReceptorPrepADT()
        return PrepProtein(protein_pdb=str(self.receptor_file_name).replace('_prepared.pdbqt', '.pdb'),
                           root_dir=self.root_dir, docking_dir=self.docking_dir), \
               PrepLigand(ligand_sdf=str(self.ligand_file_name).replace('_prepared.pdbqt', '.sdf'),
                          root_dir=self.root_dir, docking_dir=self.docking_dir)

    def output(self):
        print(os.path.join(self.root_dir, self.docking_dir,
                           str(self.ligand_file_name.replace('.pdbqt', '_') +
                               str(self.receptor_file_name.replace('.pdbqt', '.dpf')))))
        return luigi.LocalTarget(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_file_name.replace('.pdbqt', '_') +
                                                  str(self.receptor_file_name.replace('.pdbqt', '.dpf')))))

    def run(self):
        receptor = os.path.join(self.root_dir, self.docking_dir, self.receptor_file_name)
        ligand = os.path.join(self.root_dir, self.docking_dir, self.ligand_file_name)
        command = ' '.join(
            [self.ssh_command, '"', 'cd', os.path.join(self.root_dir, self.docking_dir), ';', self.pythonsh_executable,
             self.prepare_dpf4_script, '-r', receptor, '-l', ligand, '-L', '-s', '"'])
        print('\n')
        print(command)
        print('\n')
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)

        while not os.path.isfile(os.path.join(self.root_dir, self.docking_dir,
                                              str(self.ligand_file_name.replace('.pdbqt', '_') +
                                                  str(self.receptor_file_name.replace('.pdbqt', '.dpf'))))):
            pass

        infile = open(os.path.join(self.root_dir, self.docking_dir,
                                   str(self.ligand_file_name.replace('.pdbqt', '_') +
                                       str(self.receptor_file_name.replace('.pdbqt', '.dpf')))),
                      'r')
        outfile = open(os.path.join(self.root_dir, self.docking_dir,
                                    str(self.ligand_file_name.replace('.pdbqt', '_') +
                                        str(self.receptor_file_name.replace('.pdbqt', '.dpf.tmp')))),
                       'a')
        for line in infile:
            if 'ga_pop_size' in line:
                outfile.write('set_ga\n')
                outfile.write(line)
            else:
                outfile.write(line)

        shutil.move(os.path.join(self.root_dir, self.docking_dir,
                                 str(self.ligand_file_name.replace('.pdbqt', '_') +
                                     str(self.receptor_file_name.replace('.pdbqt', '.dpf.tmp')))),
                    os.path.join(self.root_dir, self.docking_dir,
                                 str(self.ligand_file_name.replace('.pdbqt', '_') +
                                     str(self.receptor_file_name.replace('.pdbqt', '.dpf')))))


class BatchPrep(luigi.Task):
    def requires(self):
        to_run = {'root_dir': [],
                  'protein_pdb': [],
                  'ligand_sdf': []}
        update_apo_field()
        conn, c = dbf.connectDB()
        c.execute("select root_dir, apo_name, mol_name from proasis_out where apo_name!=''")
        rows = c.fetchall()
        for row in rows:
            to_run['root_dir'].append('/'.join(str(row[0]).split('/')[:-1]))
            to_run['protein_pdb'].append(str(row[1]))
            to_run['ligand_sdf'].append(str(row[2]))

        zipped_list = list(zip(to_run['root_dir'], to_run['protein_pdb'], to_run['ligand_sdf']))

        return [PrepProtein(protein_pdb=protein_pdb, root_dir=root_dir) for (root_dir, protein_pdb, _) in
                zipped_list], [PrepLigand(root_dir=root_dir, ligand_sdf=ligand_sdf) for (root_dir, _, ligand_sdf) in
                               zipped_list]
