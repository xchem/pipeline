import datetime
import os
import re
import sys
from random import randint

from rdkit import Chem
from rdkit.Chem import AllChem


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
    except FileNotFoundError:
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


def randnumb(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)
