from functions import db_functions
import os
import glob


class RefinementObjectFiles:
    """
    Take a Refinement Object (one row of refinement table) and search for filepaths for:
    - bound_conf: the bound state pdb file
    - mtz: the mtz map file
    - two_fofc: the 2fofc map file
    - fofc: the fofc file

    :param refinement_object: the django object corresponding to the Refinemnt model
    :returns self.refinement_object: the input refinement object
    :returns self.bound_conf: the filepath to the bound state conformation .pdb file
    :returns self.mtz: the filepath to the corresponding .mtz file
    :returns self.two_fofc: the filepath to the corresponding 2fofc .map file
    :returns self.fofc: the filepath to the corresponding fofc .map file
    """
    def __init__(self, refinement_object):
        self.refinement_object = refinement_object
        self.bound_conf = None
        self.mtz = None
        self.two_fofc = None
        self.fofc = None

    def find_bound_file(self):
        # if there is a pdb file named in the bound_conf field, use it as the upload structure for proasis
        if self.refinement_object.bound_conf:
            if os.path.isfile(self.refinement_object.bound_conf):
                self.bound_conf = self.refinement_object.bound_conf

        # otherwise, use the most recent pdb file (according to soakdb)
        elif self.refinement_object.pdb_latest:
            if os.path.isfile(self.refinement_object.pdb_latest):
                # if this is from a refinement folder, find the bound-state pdb file, rather than the ensemble
                if 'Refine' in self.refinement_object.pdb_latest:
                    search_path = '/'.join(self.refinement_object.pdb_latest.split('/')[:-1])
                    files = glob.glob(str(search_path + '/refine*split.bound*.pdb'))
                    if len(files) == 1:
                        self.bound_conf = files[0]
                else:
                    # if can't find bound state, just use the latest pdb file
                    self.bound_conf = self.refinement_object.pdb_latest

    def find_maps(self):
        if not self.bound_conf:
            return ValueError('Bound conf not set! use RefinementObjectFiles().find_bound_file()')
        self.mtz_path = db_functions.check_file_status('refine.mtz', self.bound_conf)
        self.two_fofc_path = db_functions.check_file_status('2fofc.map', self.bound_conf)
        self.fofc_path = db_functions.check_file_status('fofc.map', self.bound_conf)