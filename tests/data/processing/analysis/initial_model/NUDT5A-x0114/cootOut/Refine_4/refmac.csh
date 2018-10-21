#!/bin/bash
#PBS -joe -N XCE_refmac

export XChemExplorer_DIR="/dls/science/groups/i04-1/software/XChemExplorer_new/XChemExplorer"

source /dls/science/groups/i04-1/software/XChemExplorer_new/XChemExplorer/setup-scripts/pandda.setup-sh


cd /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114
module load phenix

$CCP4/bin/ccp4-python $XChemExplorer_DIR/helpers/update_status_flag.py /dls/labxchem/data/2018/lb18145-71/processing/database/soakDBDataFile.sqlite NUDT5A-x0114 RefinementStatus running

giant.quick_refine input.pdb=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/cootOut/Refine_4/multi-state-model.pdb mtz=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/NUDT5A-x0114.free.mtz cif=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/Z1827602749.cif program=refmac params=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/cootOut/Refine_4/multi-state-restraints.refmac.params dir_prefix='Refine_' out_prefix='refine_4' split_conformations='False'
cd /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/Refine_0004
giant.split_conformations input.pdb='refine_4.pdb' reset_occupancies=False suffix_prefix=split
giant.split_conformations input.pdb='refine_4.pdb' reset_occupancies=True suffix_prefix=output 
giant.score_model pdb1=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/Refine_0004/refine_4.pdb mtz1=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/Refine_0004/refine_4.mtz pdb2=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/NUDT5A-x0114-ensemble-model.pdb mtz2=/dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/NUDT5A-x0114-pandda-input.mtz res_names=LIG,UNL,DRG,FRG

phenix.molprobity refine_4.pdb refine_4.mtz
/bin/mv molprobity.out refine_molprobity.log
module load phenix
mmtbx.validate_ligands refine_4.pdb refine_4.mtz LIG > validate_ligands.txt
cd /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114

ln -s Refine_0004/validate_ligands.txt .
ln -s Refine_0004/refine_molprobity.log .
ln -s Refine_0004/refine_4.split.bound-state.pdb ./refine.split.bound-state.pdb
ln -s Refine_0004/refine_4.split.ground-state.pdb ./refine.split.ground-state.pdb
ln -s Refine_0004/refine_4.output.bound-state.pdb ./refine.output.bound-state.pdb
ln -s Refine_0004/refine_4.output.ground-state.pdb ./refine.output.ground-state.pdb
mmtbx.validation_summary refine.pdb > validation_summary.txt

fft hklin refine.mtz mapout 2fofc.map << EOF
labin F1=FWT PHI=PHWT
EOF

fft hklin refine.mtz mapout fofc.map << EOF
labin F1=DELFWT PHI=PHDELWT
EOF

$CCP4/bin/ccp4-python /dls/science/groups/i04-1/software/XChemExplorer_new/XChemExplorer/helpers/update_data_source_after_refinement.py /dls/labxchem/data/2018/lb18145-71/processing/database/soakDBDataFile.sqlite NUDT5A-x0114 /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/Refine_0004

/bin/rm /dls/labxchem/data/2018/lb18145-71/processing/analysis/initial_model/NUDT5A-x0114/REFINEMENT_IN_PROGRESS

