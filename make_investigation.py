from rdkit.Chem.rdMolDescriptors import CalcMolFormula
from rdkit import Chem
from setup_django import setup_django 
setup_django()
# This line gets moved... move it back...
from xchem_db.xchem_db.models import *
import argparse
import os

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-t", "--target", help = "Target name e.g. Mpro", required=True)
  parser.add_argument("-x", "--exclude", default='', help = "Proposals to Exclude, seperated by commas")
  parser.add_argument("-o", "--output", help = "Output File", required=True)

  args = vars(parser.parse_args())  


  skeleton = {
    "_pdbx_investigation": {
        "id": args["target"],
        "type": "Fragment screening",
        "db": "fragalysis",
        "project": args["target"],
        "url": f'https://fragalysis.diamond.ac.uk/viewer/react/preview/target/{args["target"]}',
        "details": "Fragment screening project"
    },
    "_pdbx_investigation_frag_screening": {
        "investigation_id": args["target"],
        "campaign_id": '1',
        "target": args["target"],
        "common_unp": "FILLME",
        "facility": "DLS",
        "proc_pipeline": "XChem",
        "internal_id": "FILLME"
    },
    "_pdbx_investigation_series": {
        "frag_screening_campaign_id": [1, 1],
        "id": [1, 2],
        "fragment_lib": ["FILLME", 'FILLME'],
        "fragment_batch": ["v1.0", "v1.0"],
        "description": ["Placeholder Description", "Placeholder Description"]
    },
    "_pdbx_investigation_exp": {
        "series_id": [],
        "exp_id": [],
        "exp_acc": [],
        "exp_poly": [],
        "exp_non_poly": [],
        "exp_method": [],
        "db": [],
        "db_ac": [],
        "exp_details": [],
        "exp_url": []
    },
    "_pdbx_investigation_frag_screening_exp": {
        "id": [],
        "exp_id": [],
        "instance_id": [],
        "exp_fragment": [],
        "bound_fragments": [],
        "hit": [],
        "hit_assessment": [],
        "exp_details": []
    },
    "_pdbi_entity_poly_link": {
        "id": [1],
        "entity_id": [1]
    },
    "_pdbi_entity_nonpoly_link": {
        "id": [],
        "entity_id": []
    },
    "_pdbi_entity_poly": {
        "entity_id": "1",
        "type": "polypeptide(L)",
        "src_method": "man",
        "pdbx_seq_one_letter_code": "?",
        "db_name": "?",
        "db_code": "?",
        "pdbx_db_accession": "?"
    },
    "_pdbi_entity_nonpoly": {
        "entity_id": [],
        "name": [],
        "src_method": [],
        "comp_id": [],
        "formula": [],
        "formula_weight": [],
        "inchi_descriptor": []
    },
    "_pdbi_entity_frag_library": {
        "entity_id": [],
        "parent_id": [],
        "series_id": [],
        "name": [],
        "details": [],
        "src_method": [],
        "comp_id": [],
        "formula": [],
        "formula_weight": [],
        "inchi_descriptor": []
    },
    "_pdbx_contact_author": {
        "id": [],
        "address_1": [],
        "address_2": [],
        "address_3": [],
        "legacy_address": [],
        "city": [],
        "state_province": [],
        "postal_code": [],
        "email": [],
        "fax": [],
        "name_first": [],
        "name_last": [],
        "name_mi": [],
        "name_salutation": [],
        "country": [],
        "continent": [],
        "phone": [],
        "role": [],
        "organization_type": [],
        "identifier_ORCID": []
    },
    "_audit_author": {
        "pdbx_ordinal": [],
        "address": [],
        "name": [],
        "identifier_ORCID": []
    },
    "_pdbx_audit_support": {
        "ordinal": [],
        "funding_organization": [],
        "country": [],
        "grant_number": [],
        "details": []
    },
    "_pdbx_database_status": {
        "entry_id": "FILLME",
        "status_code": "FILLME",
        "dep_release_code": "FILLME",
        "author_release_status_code": "FILLME",
        "recvd_initial_deposition_date": "FILLME",
        "date_accepted_terms_and_conditions": "FILLME"
    }
  }

  exclude_list = args["exclude"].split(',')
  print(args["target"])
  crystal_set2 = Refinement.objects.filter(
        crystal_name__crystal_name__contains=args["target"])
  if exclude_list == '':
    crystal_set = crystal_set2
  else :
    crystal_set = [x for x in crystal_set2 if x.crystal_name.visit.proposal.proposal not in exclude_list]

  print(f"Processing {len(crystal_set)} Structures")  

  outcomes = {1: 'Collecting', 2: 'Preprocessing', 3: 'Refinement',
      4: 'Review', 5: 'Accepted', 6: 'Accepted', 7: 'Rejected'}

  for crystal in crystal_set:
    print(crystal.crystal_name.crystal_name)
    if crystal.bound_conf:
        strfil = crystal.bound_conf
    elif crystal.pdb_latest:
        strfil = crystal.pdb_latest
    else:
        strfil = None
    if strfil:
        if not os.path.exists(strfil):
            continue
    if strfil:
        hets = {}
        resids = []
        with open(strfil, 'r') as pdb:
            for line in pdb:
                if line.startswith('HETATM'):
                    resids.append(line[17:20].replace(' ', ''))
        for non_poly in [x for x in set(resids) if 'LIG' not in x]:
            block = []
            oldresnum, resnum = None, None
            with open(strfil, 'r') as pdb:
                for line in pdb:
                    if line.startswith('HETATM'):
                        if non_poly in line:
                            resnum = line[22:26].replace(' ', '')
                            if oldresnum:
                                if not oldresnum == resnum:
                                    continue
                            block.append(line)
                            oldresnum = resnum
            hets[non_poly] = Chem.MolFromPDBBlock(''.join(block))
    smile_mol = [
        Chem.MolFromSmiles(x.compound.smiles) for x in crystal.crystal_name.crystalcompoundpairs_set.all()]
    code = [
        x.compound.compound_string for x in crystal.crystal_name.crystalcompoundpairs_set.all()]
    smile_mol += list(hets.values())
    code_only_ligs = code
    code += list(hets.keys())
    if len(skeleton["_pdbx_investigation_exp"]["exp_id"]) < 1:
        exp_id = 1
    else:
        exp_id = max(skeleton["_pdbx_investigation_exp"]["exp_id"])+1
    entity_ids_used = []
    for mol, cod in zip(smile_mol, code):
        if mol is None:
            continue
        formula = CalcMolFormula(mol)
        inchi = Chem.inchi.MolToInchi(mol)
        wt = Chem.rdMolDescriptors.CalcExactMolWt(mol)
        if cod not in skeleton["_pdbi_entity_nonpoly"]["name"]:
            entity_id = len(skeleton["_pdbi_entity_nonpoly"]["entity_id"])+1
            comp_id = len(skeleton["_pdbi_entity_nonpoly"]["comp_id"])+1
            skeleton["_pdbi_entity_nonpoly"]["entity_id"].append(entity_id)
            skeleton["_pdbi_entity_nonpoly"]["name"].append(cod)
            skeleton["_pdbi_entity_nonpoly"]["src_method"].append('syn')
            skeleton["_pdbi_entity_nonpoly"]["comp_id"].append(comp_id)
            skeleton["_pdbi_entity_nonpoly"]["formula"].append(formula)
            skeleton["_pdbi_entity_nonpoly"]["formula_weight"].append(wt)
            skeleton["_pdbi_entity_nonpoly"]["inchi_descriptor"].append(inchi)
            entity_ids_used.append(entity_id)
        else:
            entity_ids_used.append(
                skeleton["_pdbi_entity_nonpoly"]["entity_id"][skeleton["_pdbi_entity_nonpoly"]["name"].index(cod)])
    for eid in set(entity_ids_used):
        skeleton["_pdbi_entity_nonpoly_link"]["id"].append(exp_id)
        skeleton["_pdbi_entity_nonpoly_link"]["entity_id"].append(eid)
    details = outcomes.get(crystal.outcome)
    if details == 'Review':
        r = ReviewResponses.objects.filter(crystal=crystal.crystal_name)
        if len(r) > 0:
            details = r[0].reason
        else:
            details = 'Refined but Not Reviewed'
    for ex in code_only_ligs:
        try:
          lig_entity = skeleton["_pdbi_entity_nonpoly"]["entity_id"][skeleton["_pdbi_entity_nonpoly"]["name"].index(ex)]
        except ValueError:
          print(f"Skipping {ex}")
          continue
        skeleton["_pdbx_investigation_exp"]["series_id"].append("1")
        skeleton["_pdbx_investigation_exp"]["exp_id"].append(exp_id)
        skeleton["_pdbx_investigation_exp"]["exp_acc"].append(
            crystal.crystal_name.crystal_name)  # Use names for the moment lol
        skeleton["_pdbx_investigation_exp"]["exp_poly"].append(1)
        skeleton["_pdbx_investigation_exp"]["exp_non_poly"].append(lig_entity)
        skeleton["_pdbx_investigation_exp"]["exp_method"].append("X-ray")
        skeleton["_pdbx_investigation_exp"]["db"].append("PDB")
        skeleton["_pdbx_investigation_exp"]["db_ac"].append('XXXX')
        skeleton["_pdbx_investigation_exp"]["exp_details"].append(details)
        skeleton["_pdbx_investigation_exp"]["exp_url"].append(f'https://fragalysis.diamond.ac.uk/viewer/react/preview/direct/target/{args["target"]}/mols/{ex}/L/P/C')
        # skeleton["_pdbx_investigation_exp"]["state"].append("?")

  cif_file = [f'{args["target"]}\n', "#\n"]
  for k, v in skeleton.items():
    loop = False
    lens = []
    for keys, values in v.items():
        if isinstance(values, int) or isinstance(values, str):
            pass
        else:
            lens.append(len(values))
    if len(lens) > 0:
        loop = True
    if not loop:
        for keys, values in v.items():
            if isinstance(values, str):
                cif_file.append(f'{k}.{keys} "{values}"\n')
            else:
                cif_file.append(f'{k}.{keys} {values}\n')
    else:
        cif_file.append('loop_\n')
        loop_data = []
        for keys, values in v.items():
            loop_data.append(values)
            cif_file.append(f'{k}.{keys}\n')
        for tup in zip(*loop_data):
            conv = [f'{x}' if not isinstance(
                x, str) else f'"{x}"' for x in tup]
            cif_file.append(' '.join(conv)+'\n')
    cif_file.append("#\n")

  with open(args["output"], 'w') as f:
    f.write(''.join(cif_file))
