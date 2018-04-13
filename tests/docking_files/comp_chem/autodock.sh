#!/bin/bash
    cd /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem
    touch autodock.running
    /dls_sw/apps/xchem/autodock/autodock4 -p /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem/SHH-x17_mol_prepared_SHH-x17_apo_prepared.dpf > autodock.log
    rm autodock.running
    touch autodock.done
    