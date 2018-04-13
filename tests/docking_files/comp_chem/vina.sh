#!/bin/bash
    cd /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem
    touch vina.running
    /dls_sw/apps/xchem/autodock_vina_1_1_2_linux_x86/bin/vina --receptor /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem/SHH-x17_apo_prepared.pdbqt --ligand /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem/SHH-x17_mol_prepared.pdbqt --center_x 39.5882 --center_y -10.450600000000001 --center_z -6.408533333333333 --size_x 40 --size_y 40 --size_z 40 --out SHH-x17_mol_prepared_vinaout.pdbqt > vina.log
    rm vina.running
    touch vina.done
    