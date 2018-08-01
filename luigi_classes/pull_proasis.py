import luigi
import setup_django
import os
from db.models import *
from functions import proasis_api_funcs
import openbabel
from rdkit import Chem
from rdkit.Chem import AllChem


class GetCurated(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '.pdb')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        strucid = proasis_hit.strucid
        curated_pdb = proasis_api_funcs.get_struc_file(strucid, self.output().path, 'curatedpdb')


class CreateApo(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GetCurated(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        pass

    def run(self):
        curated_pdb = self.input().path
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        ligand_list = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligand_list)

        pdb_file = open(curated_pdb, 'r')
        for line in pdb_file:
            if any(lig in line for lig in ligand_list):
                continue
            else:
                with open(self.output().path, 'a') as f:
                    f.write(line)


class GetMaps(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class GetSDF(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateApo(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '.sdf')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        strucid = proasis_hit.strucid
        sdf = proasis_api_funcs.get_struc_file(strucid, self.output().path, 'sdf')


class CreateMolFile(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GetSDF(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '.mol')))

    def run(self):
        ligand = self.input().path

        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('sdf', 'mol')

        mol = openbabel.OBMol()

        # read pdb and write mol2
        obConv.ReadFile(mol, ligand)
        obConv.WriteFile(mol, self.output().path)


class CreateMolTwoFile(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateMolFile(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '.mol2')))

    def run(self):
        mol_file = self.input().path
        rd_mol = Chem.MolFromMolFile(mol_file, removeHs=False)
        net_charge = AllChem.GetFormalCharge(rd_mol)
        out_file = self.output().path
        os.system("antechamber -i " + mol_file + " -fi mdl -o " + out_file +
                  " -fo mol2 -at sybyl  -c bcc -nc "+str(net_charge))


class CreateHMol(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateMolFile(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '_h.mol')))

    def run(self):
        mol_file = self.input().path
        rd_mol = Chem.MolFromMolFile(mol_file, removeHs=False)
        h_rd_mol = AllChem.AddHs(rd_mol, addCoords=True)
        Chem.MolToMolFile(h_rd_mol, self.output().path)


class CreateNoBufAltLocs(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateMolFiles(luigi.Task):

    def requires(self):
        return GetSDF()

    def output(self):
        pass

    def run(self):
        pass


class GetInteractionJSON(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass