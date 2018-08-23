import luigi
import setup_django
import os
import json
import shutil
import subprocess
from xchem_db.models import *
from functions import proasis_api_funcs
import openbabel
from rdkit import Chem
from rdkit.Chem import AllChem
from duck.steps.chunk import remove_prot_buffers_alt_locs
from . import transfer_proasis
import datetime


class GetCurated(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return transfer_proasis.AddFiles(hit_directory=self.hit_directory, crystal_id=self.crystal_id,
                                         refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        ligands = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligands)

        return [luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name.upper(), str(crystal_name + '_' + str(i)), str(crystal_name + str('_'+ str(i) + '.pdb')))
                                  for i in range(1, len(ligand_list)+1))]

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        strucid = proasis_hit.strucid
        ligands = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligands)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name

        ligid = 0
        for lig in ligand_list:
            ligid += 1

            curated_pdb = proasis_api_funcs.get_struc_file(strucid,
                                                           os.path.join(
                                                               self.hit_directory, target_name.upper(), str(crystal_name +
                                                                                                        '_' +
                                                                                                        str(ligid)),
                                                               str(crystal_name + str('_' + str(ligid) + '.pdb'))),
                                                           'curatedpdb')

            proasis_out = ProasisOut.objects.get_or_create(proasis=proasis_hit,
                                                           crystal=proasis_hit.crystal_name,
                                                           ligand=lig,
                                                           ligid=ligid,
                                                           root=os.path.join(self.hit_directory,
                                                                             proasis_hit.crystal_name.target.target_name.upper()),
                                                           start=str(proasis_hit.crystal_name.crystal_name + '/' +
                                                                     proasis_hit.crystal_name.crystal_name + '_' +
                                                                     ligid),
                                                           curated=curated_pdb.split('/')[-1]
                                                           )
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
        target_name = proasis_hit.crystal_name.target.target_name.upper()
        ligands = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligands)

        return [luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name, crystal_name, str(crystal_name + str(i)), str(crystal_name + '_apo_' + str(i) + '.pdb')))
            for i in range(1, len(ligand_list)+1)]

    def run(self):
        curated_pdb = self.input().path
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        ligand_list = eval(proasis_hit.ligand_list)
        ligand_list = proasis_api_funcs.get_lig_strings(ligand_list)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name.upper()

        pdb_file = open(curated_pdb, 'r')
        ligid = 0
        for l in ligand_list:
            ligid += 1
            for line in pdb_file:
                if any(lig in line for lig in ligand_list):
                    continue
                else:
                    with open(os.path.join(self.hit_directory, target_name,
                                           crystal_name, str(crystal_name + '_' + str(ligid)),
                                           str(crystal_name + '_apo_' + str(ligid) + '.pdb')), 'a') as f:
                        f.write(line)

            out_entry = ProasisOut.objects.filter(proasis=proasis_hit, ligid=ligid)

            out_entry.apo = str(crystal_name + '_apo_' + str(ligid) + '.pdb')
            out_entry.save()


class GetMaps(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


## TODO: wait to see if altconfs works - proasis
class GetSDFS(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateApo(hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

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


## TODO: wait to see if altconfs works - proasis
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


## TODO: wait to see if altconfs works - proasis
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
                for (r, s, l) in zip(root, start, ligs)], \
               [luigi.LocalTarget(os.path.join(r, s, str(s + '_' + l.replace(' ', '') + '_h.mol')))
                for (r, s, l) in zip(root, start, ligs)]

    def run(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        for o in proasis_out:
            lig = o.ligand
            infile = os.path.join(o.root, o.start, str(o.start + '_' + lig.replace(' ', '') + '.mol'))
            outfile = infile.replace('mol', 'mol2')

            rd_mol = Chem.MolFromMolFile(infile, removeHs=False)
            h_rd_mol = AllChem.AddHs(rd_mol, addCoords=True)

            Chem.MolToMolFile(h_rd_mol, outfile.replace('.mol2', '_h.mol'))
            o.h_mol = outfile.replace('.mol2', '_h.mol').split('/')[-1]
            rd_mol = Chem.MolFromMolFile(outfile.replace('.mol2', '_h.mol'), removeHs=False)

            infile = os.path.join(o.root, o.start, str(o.start + '_' + lig.replace(' ', '') + '_h.mol'))

            net_charge = AllChem.GetFormalCharge(rd_mol)
            command_string = str("antechamber -i " + infile + " -fi mdl -o " + outfile +
                                 " -fo mol2 -at sybyl -c bcc -nc " + str(net_charge))
            print(command_string)
            process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            out = out.decode('ascii')
            if err:
                err = err.decode('ascii')
                raise Exception(err)

            print(out)
            print(err)
            o.mol2 = outfile.split('/')[-1]
            o.save()


## TODO: wait to see if altconfs works - proasis
class GetInteractionJSON(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateApo(hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        ligs = [o.ligand for o in proasis_out]
        root = [o.root for o in proasis_out]
        start = [o.start for o in proasis_out]
        return [luigi.LocalTarget(os.path.join(r, s, str(s + '_' + l.replace(' ', '') + '_contacts.json')))
                for (r, s, l) in zip(root, start, ligs)]

    def run(self):
        proasis_out = ProasisOut.objects.filter(proasis=ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                                                                refinement_id=self.refinement_id))
        for o in proasis_out:
            lig = o.ligand
            strucid = o.proasis.strucid
            root = o.root
            start = o.start
            outfile = os.path.join(root, start, str(start + '_' + lig.replace(' ', '') + '_contacts.json'))
            out = proasis_api_funcs.get_lig_interactions(strucid, lig, outfile)
            if out:
                o.contacts = out.split('/')[-1]
            else:
                raise Exception('contacts json not produced!')
            o.save()


class CreateStripped(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()

    def requires(self):
        return CreateApo(hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id)

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)
        crystal_name = proasis_hit.crystal_name.crystal_name
        target_name = proasis_hit.crystal_name.target.target_name
        return luigi.LocalTarget(os.path.join(
            self.hit_directory, target_name.upper(), crystal_name, str(crystal_name + '_no_buffer_altlocs.pdb')))

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id)

        tmp_file = remove_prot_buffers_alt_locs(self.input().path)
        shutil.move(os.path.join(os.getcwd(), tmp_file), self.output().path)

        proasis_out = ProasisOut.objects.filter(proasis=proasis_hit, ligid=ligid)
        for o in proasis_out:
            o.stripped = self.output().path.split('/')[-1]
            o.save()


class GetOutFiles(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/out/proasis_out_%Y%m%d%H.txt'))

    def requires(self):
        proasis_hits = ProasisHits.objects.exclude(strucid=None).exclude(strucid='')
        crys_ids = []
        ref_ids = []

        for h in proasis_hits:
            crys_ids.append(h.crystal_name_id)
            ref_ids.append(h.refinement_id)

        return [CreateMolTwoFile(hit_directory=self.hit_directory,
                                 crystal_id=c,
                                 refinement_id=r)
                for (c, r) in zip(crys_ids, ref_ids)], \
               [GetInteractionJSON(hit_directory=self.hit_directory,
                                   crystal_id=c,
                                   refinement_id=r)
                for (c, r) in zip(crys_ids, ref_ids)], \
               [CreateStripped(hit_directory=self.hit_directory,
                               crystal_id=c,
                               refinement_id=r)
                for (c, r) in zip(crys_ids, ref_ids)]

    def run(self):
        with self.output().open('w') as f:
            f.write('')
