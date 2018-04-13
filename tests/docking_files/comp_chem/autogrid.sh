#!/bin/bash
    cd /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem
    touch autogrid.running
    /dls_sw/apps/xchem/autodock/autogrid4 -p /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem/SHH-x17_apo_prepared.gpf -l /dls/labxchem/data/2015/lb13320-1/processing/analysis/initial_model/SHH-x17/comp_chem/SHH-x17_apo_prepared.glg > autogrid.log
    rm autogrid.running
    touch autogrid.done
    