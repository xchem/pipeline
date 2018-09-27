import glob
import json
import os
import shutil
import subprocess

import setup_django

setup_django.setup_django()
import datetime
import luigi
import openbabel
from duck.steps.chunk import remove_prot_buffers_alt_locs
from rdkit import Chem
from rdkit.Chem import AllChem

from functions import proasis_api_funcs
from xchem_db.models import *
from . import transfer_proasis


def get_output_file_name(proasis_hit, ligid, hit_directory, extension):
    # get crystal and target name for output path
    crystal_name = proasis_hit.crystal_name.crystal_name
    target_name = proasis_hit.crystal_name.target.target_name

    return luigi.LocalTarget(os.path.join(
        hit_directory,  # /dls/science/groups/proasis/LabXChem
        target_name.upper(),  # /TARGET
        str(crystal_name + '_' + str(ligid)),  # /CRYSTAL_N
        str(crystal_name + str('_' + str(ligid) + extension))  # /CRYSTAL_N<extension>
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
            proasis_out.start = str(self.output().path.replace(self.hit_directory, '').replace(str(
                self.output().path.split('/')[-1]), ''))[1:]

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
        for _ in ligand_list:
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

        # Boron atom - tmp fix for non paramaterized antechamber forcefield
        boron = Chem.MolFromSmarts('[B]')

        # create mol from input mol file
        rd_mol = Chem.MolFromMolFile(self.input().path, removeHs=False)

        boron_matches = rd_mol.HasSubstructMatch(boron)
        if boron_matches:
            obConv = openbabel.OBConversion()
            obConv.SetInAndOutFormats('mol', 'mol2')
            # blank mol for ob
            mol = openbabel.OBMol()
            # read pdb and write mol
            obConv.ReadFile(mol, self.input().path)
            obConv.WriteFile(mol, self.output().path)
            proasis_out.mol2 = self.output().path.split('/')[-1]
            proasis_out.save()

        else:
            # get charge from mol file
            net_charge = AllChem.GetFormalCharge(rd_mol)
            # use antechamber to calculate forcefield, and output a mol2 file
            command_string = str("antechamber -i " + self.input().path + " -fi mdl -o " + self.output().path +
                                 " -fo mol2 -at sybyl -c bcc -nc " + str(net_charge))
            print(command_string)
            process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            out = out.decode('ascii')
            if 'Error' in out:
                raise Exception(out)
            else:
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
            raise Exception(str('contacts json not produced: ' + ', '.join([lig, self.output().path, strucid])))

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


class CreateProposalFile(luigi.Task):
    proposals = luigi.Parameter()
    out_directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_directory, 'PROPOSALS'))

    def run(self):
        proposals = [pro[2:] for pro in self.proposals]
        out_string = ' '.join(proposals)
        with self.output().open('w') as f:
            f.write(out_string)


class CreateVisitFile(luigi.Task):
    visits = luigi.Parameter()
    out_directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_directory, 'VISITS'))

    def run(self):
        visits = [pro[2:] for pro in self.visits]
        out_string = ' '.join(visits)
        with self.output().open('w') as f:
            f.write(out_string)


class CreateProposalVisitFiles(luigi.Task):
    def requires(self):
        def add_to_dict(dct, value, target):
            if target not in dct.keys():
                dct[target] = []
            dct[target].append(value)

        outs = ProasisOut.objects.all()
        out_dict = {}
        proposal_dict = {}
        visit_dict = {}

        for o in outs:
            if o.root and o.start:
                add_to_dict(visit_dict, o.crystal.visit.visit, o.crystal.target.target_name)
                add_to_dict(proposal_dict, o.crystal.visit.proposal.proposal, o.crystal.target.target_name)
                add_to_dict(out_dict, os.path.join(o.root, o.start.split('/')[0]), o.crystal.target.target_name)

        dicts = [out_dict, proposal_dict, visit_dict]

        for d in dicts:
            for k in d.keys():
                d[k] = list(set(d[k]))

        targets = [k for k in out_dict.keys()]

        return [CreateProposalFile(proposals=proposal_dict[target], out_directory=od)
                for target in targets for od in out_dict[target]], \
               [CreateVisitFile(visits=visit_dict[target], out_directory=od)
                for target in targets for od in out_dict[target]]

    def output(self):
        return luigi.LocalTarget(str('proposals_visits_' + datetime.datetime.now().strftime('%Y-%m-%dT%H')))

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class GetLigConf(luigi.Task):
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
        proasis_hit = ProasisHits.objects.get(crystal_name_id=self.crystal_id, refinement_id=self.refinement_id,
                                              altconf=self.altconf)

        return get_output_file_name(proasis_hit, self.ligid, self.hit_directory, '_lig_conf.json')

    def run(self):
        refinement = Refinement.objects.get(pk=self.refinement_id)
        lig_confidence = {'ligand_confidence_int': refinement.lig_confidence_int,
                          'ligand_confidence_comment': refinement.lig_confidence_string,
                          'refinement_outcome': refinement.outcome}

        with self.output().open('w') as f:
            json.dump(lig_confidence, f)


class GetOutFiles(luigi.Task):
    datestamp = luigi.DateParameter(default=str(datetime.datetime.now().strftime('%Y%m%d%H')))

    def output(self):
        return luigi.LocalTarget(str('logs/proasis/out/proasis_out_' + self.datestamp + '.txt'))

    def requires(self):
        # get anything that has been uploaded to proasis
        proasis_hits = ProasisHits.objects.exclude(strucid=None).exclude(strucid='')

        # set up tmp lists to hold values
        crys_ids = []
        ref_ids = []
        ligs = []
        ligids = []
        alts = []
        hit_dirs = []

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
                    ligid += 1
                    # get or create the proasis out object before pulling begins
                    # if ProasisOut.objects.filter(proasis=hit, ligand=ligand, ligid=ligid,
                    #                                                crystal=hit.crystal_name).exists():

                    ProasisOut.objects.get_or_create(proasis=hit, ligand=ligand, ligid=ligid,
                                                     crystal=hit.crystal_name)

                    # add data needed for pulling files to tmp lists
                    crys_ids.append(hit.crystal_name_id)
                    ref_ids.append(hit.refinement_id)
                    ligs.append(ligand)
                    ligids.append(ligid)
                    alts.append(hit.altconf)

                    proposal = hit.crystal_name.visit.proposal.proposal
                    visit1 = str(proposal + '-1')

                    glob_string = os.path.join('/dls/labxchem/data/20*', visit1)

                    paths = glob.glob(glob_string)

                    # can't always find paths, so if that's the case, use the soakdb filepath:
                    if not paths:
                        paths = [hit.crystal_name.visit.filename.split('database')[0]]

                    if len(paths) == 1:
                        hit_directory = os.path.join(paths[0], 'processing', 'fragalysis')
                        if not os.path.isdir(hit_directory):
                            os.makedirs(hit_directory)
                    else:
                        hit_directory = ''

                    hit_dirs.append(hit_directory)

        return [CreateMolTwoFile(hit_directory=h,
                                 crystal_id=c,
                                 refinement_id=r,
                                 ligand=l,
                                 ligid=lid, altconf=a)
                for (c, r, l, lid, a, h) in zip(crys_ids, ref_ids, ligs, ligids, alts, hit_dirs)], \
               [GetInteractionJSON(hit_directory=h,
                                   crystal_id=c,
                                   refinement_id=r,
                                   ligand=l,
                                   ligid=lid, altconf=a)
                for (c, r, l, lid, a, h) in zip(crys_ids, ref_ids, ligs, ligids, alts, hit_dirs)], \
               [CreateStripped(hit_directory=h,
                               crystal_id=c,
                               refinement_id=r,
                               ligand=l,
                               ligid=lid, altconf=a)
                for (c, r, l, lid, a, h) in zip(crys_ids, ref_ids, ligs, ligids, alts, hit_dirs)], \
               [GetLigConf(hit_directory=h,
                           crystal_id=c,
                           refinement_id=r,
                           ligand=l,
                           ligid=lid, altconf=a)
                for (c, r, l, lid, a, h) in zip(crys_ids, ref_ids, ligs, ligids, alts, hit_dirs)]

    def run(self):
        with self.output().open('w') as f:
            f.write('')
