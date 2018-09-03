import luigi
import setup_django
import os
import glob
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


def get_output_file_name(proasis_hit, ligid, hit_directory, extension):
    # get crystal and target name for output path
    crystal_name = proasis_hit.crystal_name.crystal_name
    target_name = proasis_hit.crystal_name.target.target_name

    return luigi.LocalTarget(os.path.join(
        hit_directory,                                          # /dls/science/groups/proasis/LabXChem
        target_name.upper(),                                    # /TARGET
        'output',                                               # /output
        str(crystal_name + '_' + str(ligid)),                   # /CRYSTAL_N
        str(crystal_name + str('_' + str(ligid) + extension))   # /CRYSTAL_N<extension>
    ))

class GetCurated(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        # make sure it's actually in proasis first
        return transfer_proasis.AddFiles(hit_directory=self.hit_directory, crystal_id=self.crystal_id,
                                         refinement_id=self.refinement_id, altconf=self.altconf)

    def output(self):
        # get the specific hit info
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '.pdb')

    def run(self):
        # get the proasis out object created in the kick off task
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                                crystal=proasis_hit.crystal_name,
                                                ligand=self.ligand,
                                                ligid=self.ligid)
        # if the output directories don't exist yet, make them
        if not os.path.isdir('/'.join(self.output().path.split('/')[:-1])):
            os.makedirs('/'.join(self.output().path.split('/')[:-1]))

        # find strucid to pull the right file from proasis
        strucid = proasis_out.proasis.strucid
        # pull the file from proasis
        curated_pdb = proasis_api_funcs.get_struc_file(strucid, self.output().path, 'curatedpdb')

        # if the file is created successfully
        if curated_pdb:
            # change the relevant fields
            proasis_out.curated = str(self.output().path.split('/')[-1])
            proasis_out.root = self.hit_directory
            proasis_out.start = self.output().path.replace(self.hit_directory, '').replace(str(
                self.output().path.split('/')[-1]), '')

            proasis_out.save()


class CreateApo(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return GetCurated(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            ligand=self.ligand, ligid=self.ligid, altconf=self.altconf
        )

    def output(self):
        # get the specific hit info
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '_apo.pdb')

    def run(self):
        # get the relevant proasis hit object
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        # turn it's ligand list into an actual list
        ligands = eval(proasis_hit.ligand_list)

        # remove trailing blank space and altconf letters (not in proasis structure)
        ligand_list = [l[1:] for l in ligands]

        # open the input apo structure as 'pdb_file'
        pdb_file = open(self.input().path, 'r')
        for l in ligand_list:
            for line in pdb_file:
                # if any of the ligands in the ligand list appear in the line, continue
                if any(lig in line for lig in ligand_list):
                    continue
                # otherwise, write the line to the apo structure file
                else:
                    with open(self.output().path, 'a') as f:
                        f.write(line)
            # add the apo file name to the proasis out entry
            out_entry = ProasisOut.objects.get(proasis=proasis_hit, ligid=self.ligid,
                                                  crystal=proasis_hit.crystal_name, ligand=self.ligand)
            out_entry.apo = str(self.output().path.split('/')[-1])
            out_entry.save()


class GetSDFS(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CreateApo(hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
                         altconf=self.altconf, ligand=self.ligand, ligid=self.ligid)

    def output(self):
        # get the specific hit info
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '.sdf')

    def run(self):
        # get hit and out entries
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                                ligid=self.ligid,
                                                ligand=self.ligand,
                                                crystal=proasis_hit.crystal_name)
        # get strucid and ligand string
        strucid = proasis_hit.strucid
        lig = proasis_out.ligand[1:]
        # create sdf file
        sdf = proasis_api_funcs.get_lig_sdf(strucid, lig, self.output().path)
        # add sdf file to out entry
        proasis_out.sdf = sdf.split('/')[-1]
        proasis_out.save()


class CreateMolFile(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return GetSDFS(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            altconf=self.altconf, ligid=self.ligid, ligand=self.ligand
        )

    def output(self):
        # get the specific hit info
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '.mol')

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                                crystal=proasis_hit.crystal_name,
                                                ligand=self.ligand,
                                                ligid=self.ligid)
        # set openbabel to convert from sdf to mol
        obConv = openbabel.OBConversion()
        obConv.SetInAndOutFormats('sdf', 'mol')
        # blank mol for ob
        mol = openbabel.OBMol()
        # read pdb and write mol
        obConv.ReadFile(mol, self.input().path)
        obConv.WriteFile(mol, self.output().path)
        # add mol file to proasis out entry
        proasis_out.mol = self.output().path.split('/')[-1]
        proasis_out.save()


class CreateHMolFile(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CreateMolFile(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            ligand=self.ligand, ligid=self.ligid, altconf=self.altconf
        )

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '_h.mol')

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                                crystal=proasis_hit.crystal_name,
                                                ligand=self.ligand,
                                                ligid=self.ligid)
        # create mol from input mol file
        rd_mol = Chem.MolFromMolFile(self.input().path, removeHs=False)
        # add hydrogens
        h_rd_mol = AllChem.AddHs(rd_mol, addCoords=True)
        # save mol with hydrogens
        Chem.MolToMolFile(h_rd_mol, self.output().path)
        # add h_mol to proasis_out entry
        proasis_out.h_mol = self.output().path.split('/')[-1]
        proasis_out.save()


class CreateMolTwoFile(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CreateHMolFile(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            ligand=self.ligand, ligid=self.ligid, altconf=self.altconf
        )

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '.mol2')

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                                crystal=proasis_hit.crystal_name,
                                                ligand=self.ligand,
                                                ligid=self.ligid)
        # create mol from input mol file
        rd_mol = Chem.MolFromMolFile(self.input().path, removeHs=False)
        # get charge from mol file
        net_charge = AllChem.GetFormalCharge(rd_mol)
        # use antechamber to calculate forcefield, and output a mol2 file
        command_string = str("antechamber -i " + self.input().path + " -fi mdl -o " + self.output().path +
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
        # save mol2 file to proasis_out object
        proasis_out.mol2 = self.output().path.split('/')[-1]
        proasis_out.save()


class GetInteractionJSON(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CreateApo(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            ligand=self.ligand, ligid=self.ligid, altconf=self.altconf
        )

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '_contacts.json')

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                             crystal=proasis_hit.crystal_name,
                                             ligand=self.ligand,
                                             ligid=self.ligid)
        # get the ligand string from proasis out and remove altconf letter or blank space
        lig = proasis_out.ligand[1:]
        # get proasis strucid
        strucid = proasis_out.proasis.strucid
        # get the interaction json and save to output path
        out = proasis_api_funcs.get_lig_interactions(strucid, lig, self.output().path)
        if out:
            # if successful - add json to proasis_out object
            proasis_out.contacts = out.split('/')[-1]
        else:
            raise Exception('contacts json not produced!')
        # save proasis out
        proasis_out.save()


class CreateStripped(luigi.Task):
    hit_directory = luigi.Parameter()
    crystal_id = luigi.Parameter()
    refinement_id = luigi.Parameter()
    ligand = luigi.Parameter()
    ligid = luigi.Parameter()
    altconf = luigi.Parameter()

    def requires(self):
        return CreateApo(
            hit_directory=self.hit_directory, crystal_id=self.crystal_id, refinement_id=self.refinement_id,
            ligand=self.ligand, ligid=self.ligid, altconf=self.altconf
        )

    def output(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '_no_buffer_altlocs.pdb')

    def run(self):
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id,
                                              refinement_id=self.refinement_id,
                                              altconf=self.altconf)
        proasis_out = ProasisOut.objects.get(proasis=proasis_hit,
                                             crystal=proasis_hit.crystal_name,
                                             ligand=self.ligand,
                                             ligid=self.ligid)
        # remove altconfs and buffers from input and save to a temporary path defined by the function
        tmp_file = remove_prot_buffers_alt_locs(self.input().path)
        # move the tmp file to the output path
        shutil.move(os.path.join(os.getcwd(), tmp_file), self.output().path)
        # save the output file to proasis_out model
        proasis_out.stripped = self.output().path.split('/')[-1]
        proasis_out.save()


class GetOutFiles(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem/')
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/proasis/out/proasis_out_%Y%m%d%H.txt'))

    def requires(self):
        # get anything that has been uploaded to proasis
        proasis_hits = ProasisHits.objects.exclude(strucid=None).exclude(strucid='')

        # set up tmp lists to hold values
        crys_ids = []
        ref_ids = []
        ligs = []
        ligids = []
        alts = []

        # for each hit in the list
        for h in proasis_hits:
            # get group of hits - groups of altconfs
            hit_group = ProasisHits.objects.filter(crystal_name=h.crystal_name, refinement=h.refinement)
            # set ligid to 0 - auto assigned by increments of one for each group
            ligid = 0

            # for each hit in the group (all altconfs)
            for hit in hit_group:
                # turn ligand list into actual list
                ligands = eval(hit.ligand_list)

                # for each lig in that list
                for ligand in ligands:
                    # increase ligand id by 1
                    ligid+=1
                    # get or create the proasis out object before pulling begins
                    proasis_out = ProasisOut.objects.get_or_create(proasis=hit, ligand=ligand, ligid=ligid,
                                                                   crystal=hit.crystal_name)
                    # add data needed for pulling files to tmp lists
                    crys_ids.append(hit.crystal_name_id)
                    ref_ids.append(hit.refinement_id)
                    ligs.append(ligand)
                    ligids.append(ligid)
                    alts.append(hit.altconf)

                    # proposal = hit.crystal_name.visit.proposal.proposal
                    # visit1 = str(proposal + '-1')
                    #
                    # glob_string = os.path.join('/dls/labxchem/data/20*', visit1)
                    #
                    # paths = glob.glob(glob_string)
                    #
                    # if len(paths) == 1:
                    #     hit_directory = paths[0]

        return [CreateMolTwoFile(hit_directory=self.hit_directory,
                                 crystal_id=c,
                                 refinement_id=r,
                                 ligand=l,
                                 ligid=lid, altconf=a)
                for (c, r, l, lid, a) in zip(crys_ids, ref_ids, ligs, ligids, alts)], \
               [GetInteractionJSON(hit_directory=self.hit_directory,
                                   crystal_id=c,
                                   refinement_id=r,
                                   ligand=l,
                                   ligid=lid, altconf=a)
                for (c, r, l, lid, a) in zip(crys_ids, ref_ids, ligs, ligids, alts)], \
               [CreateStripped(hit_directory=self.hit_directory,
                               crystal_id=c,
                               refinement_id=r,
                               ligand=l,
                               ligid=lid, altconf=a)
                for (c, r, l, lid, a) in zip(crys_ids, ref_ids, ligs, ligids, alts)]

    def run(self):
        with self.output().open('w') as f:
            f.write('')
