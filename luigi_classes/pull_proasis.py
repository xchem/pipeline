import luigi
import setup_django
import os
import json
import subprocess
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
        ligands = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligands)
        curated_pdb = proasis_api_funcs.get_struc_file(strucid, self.output().path, 'curatedpdb')

        for lig in ligand_list:
            proasis_out = ProasisOut.objects.get_or_create(proasis=proasis_hit,
                                                           crystal=proasis_hit.crystal_name,
                                                           ligand=lig,
                                                           root=os.path.join(self.hit_directory,
                                                                             proasis_hit.crystal_name.target.target_name),
                                                           start=proasis_hit.crystal_name.crystal_name,
                                                           curated=str(proasis_hit.crystal_name.crystal_name + '.pdb'))
            proasis_out[0].save()


class CreateApo(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GetCurated(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '_apo.pdb')))

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

        out_entries = ProasisOut.objects.filter(proasis=proasis_hit)

        for entry in out_entries:
            entry.apo = str(self.output().path).split('/')[-1]
            entry.save()


class GetMaps(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class GetSDFS(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        ligs = [o.ligand for o in proasis_out]
        root = [o.root for o in proasis_out]
        start = [o.start for o in proasis_out]
        return [luigi.LocalTarget(os.path.join(r, s, str(s + '_' + l.replace(' ', '') + '.sdf')))
                for (r, s, l) in zip(root, start, ligs)]

    def run(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        for o in proasis_out:
            strucid = o.proasis.strucid
            lig = o.ligand
            outfile = os.path.join(o.root, o.start, str(o.start + '_' + lig.replace(' ', '') + '.sdf'))
            sdf = proasis_api_funcs.get_lig_sdf(strucid, lig, outfile)

            o.sdf = sdf.split('/')[-1]
            o.save()


class CreateMolFile(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return GetSDFS(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        ligs = [o.ligand for o in proasis_out]
        root = [o.root for o in proasis_out]
        start = [o.start for o in proasis_out]
        return [luigi.LocalTarget(os.path.join(r, s, str(s + '_' + l.replace(' ', '') + '.mol')))
                for (r, s, l) in zip(root, start, ligs)]

    def run(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        for o in proasis_out:
            lig = o.ligand
            infile = os.path.join(o.root, o.start, str(o.start + '_' + lig.replace(' ', '') + '.sdf'))
            outfile = infile.replace('sdf', 'mol')

            obConv = openbabel.OBConversion()
            obConv.SetInAndOutFormats('sdf', 'mol')

            mol = openbabel.OBMol()

            # read pdb and write mol2
            obConv.ReadFile(mol, infile)
            obConv.WriteFile(mol, outfile)

            o.mol = outfile.split('/')[-1]
            o.save()


class CreateMolTwoFile(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateMolFile(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        ligs = [o.ligand for o in proasis_out]
        root = [o.root for o in proasis_out]
        start = [o.start for o in proasis_out]
        return [luigi.LocalTarget(os.path.join(r, s, str(s + '_' + l.replace(' ', '') + '.mol2')))
                for (r, s, l) in zip(root, start, ligs)]

    def run(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        for o in proasis_out:
            lig = o.ligand
            infile = os.path.join(o.root, o.start, str(o.start + '_' + lig.replace(' ', '') + '.mol'))
            outfile = infile.replace('mol', 'mol2')

            rd_mol = Chem.MolFromMolFile(infile, removeHs=False)
            net_charge = AllChem.GetFormalCharge(rd_mol)
            command_string = str("ssh uzw12877@nx.diamond.ac.uk; source activate duck; antechamber -i " + infile +
                      " -fi mdl -o " + outfile + " -fo mol2 -at sybyl -c bcc -nc " + str(net_charge))
            process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            out = out.decode('ascii')
            if err:
                err = err.decode('ascii')

            print(out)
            print(err)

            o.mol2 = outfile.split('/')[-1]
            o.save()



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


class GetInteractionJSON(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        pass
            # CreateMolFile(
            # hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + '_contacts.json')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        strucid = proasis_hit.strucid

        url = str("http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/sc/" + str(strucid) +
                  "?strucsource=inh&plain=1")

        json_string_scorpion = proasis_api_funcs.get_json(url)

        with open(self.output().path, 'w') as outfile:
            json.dump(json_string_scorpion, outfile, ensure_ascii=False)