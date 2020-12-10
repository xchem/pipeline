import datetime
import os
import re
import sys
from random import randint

from rdkit import Chem
from rdkit.Chem import AllChem
import openbabel
import subprocess


def get_id_string(out):
    """
    Regex function for finding proasis strucid
    """
    try:
        strucidstr = re.search(r"strucid='.....'", out)
        strucidstr = strucidstr.group()
        strucidstr = strucidstr.replace('strucid=', '')
        strucidstr = strucidstr.replace("'", '')
    except:
        print(sys.exc_info())
        strucidstr = ''
    return strucidstr


def get_mod_date(filename):
    try:
        modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime("%Y%m%d%H%M%S")
    except:
        modification_date = 'None'
    return modification_date


def create_sd_file(name, smiles, save_directory):
    """
    Create a 2D sdf file in the proasis project directory for successfully detected ligands
    """
    # create sdf file for ligand and save to hit directory
    canon_smiles = Chem.CanonSmiles(smiles)
    mol = Chem.MolFromSmiles(canon_smiles)
    AllChem.Compute2DCoords(mol)
    print('Generating sdf file and saving to ' + name + ' directory...\n')
    sd_file = Chem.SDWriter(save_directory)
    sd_file.write(mol)


def lig_sdf_from_pdb(lig_string, pdb_file, sdf_out, smiles=None):
    pdb_ligs = ''.join([x for x in open(pdb_file, 'r').readlines() if lig_string in x])
    mol = Chem.rdmolfiles.MolFromPDBBlock(pdb_ligs, sanitize=False)
    if smiles:
        ref = Chem.MolFromSmiles(smiles)
        m = AllChem.AssignBondOrdersFromTemplate(ref, mol)
    else:
        m = Chem.AddHs(mol)
        Chem.SanitizeMol(m, sanitizeOps=Chem.SANITIZE_ALL ^ Chem.SANITIZE_SETAROMATICITY)
        m = Chem.RemoveHs(m)
    writer = Chem.rdmolfiles.SDWriter(sdf_out)
    writer.write(m)


def randnumb(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def obconv(in_form, out_form, in_file, out_file):
    obconv = openbabel.OBConversion()
    obconv.SetInAndOutFormats(in_form, out_form)
    # blank mol for ob
    mol = openbabel.OBMol()
    # read pdb and write mol
    obconv.ReadFile(mol, in_file)
    obconv.WriteFile(mol, out_file)


def hmol(input, output):
    # create mol from input mol file
    rd_mol = Chem.MolFromMolFile(input, removeHs=False)
    # add hydrogens
    h_rd_mol = AllChem.AddHs(rd_mol, addCoords=True)
    # save mol with hydrogens
    Chem.MolToMolFile(h_rd_mol, output)


def antechamber_mol2(rd_mol, input, output):
    # get charge from mol file
    net_charge = AllChem.GetFormalCharge(rd_mol)
    # use antechamber to calculate forcefield, and output a mol2 file
    command_string = str("antechamber -i " + input + " -fi mdl -o " + output +
                         " -fo mol2 -at sybyl -c bcc -nc " + str(net_charge))
    print(command_string)
    process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')

    return out


def get_filepath_of_potential_symlink(file):
    try:
        path = os.readlink(file)
    except OSError:
        path = file

    return path

