# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from __future__ import unicode_literals

from django.db import models


class Target(models.Model):
    target_name = models.TextField(blank=False, null=False, unique=True)


class Compounds(models.Model):
    smiles = models.TextField(blank=False, null=False, unique=True)


class Reference(models.Model):
    reference_pdb = models.TextField(blank=False, null=False, unique=True)


class SoakdbFiles(models.Model):
    filename = models.TextField(blank=False, null=False, unique=True)
    modification_date = models.BigIntegerField(blank=False, null=False)
    proposal = models.TextField(blank=False, null=False)
    status = models.IntegerField(blank=True, null=True)


class Crystal(models.Model):
    crystal_name = models.TextField(blank=False, null=False, unique=True)
    target = models.ForeignKey(Target, on_delete=models.CASCADE)
    compound = models.ForeignKey(Compounds, on_delete=models.CASCADE)
    file = models.ForeignKey(SoakdbFiles, on_delete=models.CASCADE)
    reference = models.ForeignKey(Reference, on_delete=models.CASCADE)


class DataProcessing(models.Model):
    auto_assigned = models.TextField(blank=True, null=True)
    cchalf_high = models.TextField(blank=True, null=True)
    cchalf_low = models.TextField(blank=True, null=True)
    cchalf_overall = models.TextField(blank=True, null=True)
    completeness_high = models.TextField(blank=True, null=True)
    completeness_low = models.TextField(blank=True, null=True)
    completeness_overall = models.TextField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE) # changed to foreign key
    dimple_mtz_path = models.TextField(blank=True, null=True)
    dimple_pdb_path = models.TextField(blank=True, null=True)
    dimple_status = models.TextField(blank=True, null=True)
    image_path = models.TextField(blank=True, null=True)
    isig_high = models.TextField(blank=True, null=True)
    isig_low = models.TextField(blank=True, null=True)
    isig_overall = models.TextField(blank=True, null=True)
    lattice = models.TextField(blank=True, null=True)
    log_name = models.TextField(blank=True, null=True)
    logfile_path = models.TextField(blank=True, null=True)
    mtz_name = models.TextField(blank=True, null=True)
    mtz_path = models.TextField(blank=True, null=True)
    multiplicity_high = models.TextField(blank=True, null=True)
    multiplicity_low = models.TextField(blank=True, null=True)
    multiplicity_overall = models.TextField(blank=True, null=True)
    original_directory = models.TextField(blank=True, null=True)
    point_group = models.TextField(blank=True, null=True)
    program = models.TextField(blank=True, null=True)
    r_cryst = models.TextField(blank=True, null=True)
    r_free = models.TextField(blank=True, null=True)
    r_merge_high = models.TextField(blank=True, null=True)
    r_merge_low = models.TextField(blank=True, null=True)
    r_merge_overall = models.TextField(blank=True, null=True)
    res_high = models.TextField(blank=True, null=True)
    res_high_15_sigma = models.TextField(blank=True, null=True)
    res_high_outer_shell = models.TextField(blank=True, null=True)
    res_low = models.TextField(blank=True, null=True)
    res_low_inner_shell = models.TextField(blank=True, null=True)
    res_overall = models.TextField(blank=True, null=True)
    score = models.TextField(blank=True, null=True)
    spacegroup = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    unique_ref_overall = models.TextField(blank=True, null=True)
    unit_cell = models.TextField(blank=True, null=True)
    unit_cell_vol = models.TextField(blank=True, null=True)


class Dimple(models.Model):
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    mtz_path = models.TextField(blank=True, null=True)
    pandda_hit = models.TextField(blank=True, null=True)
    pandda_path = models.TextField(blank=True, null=True)
    pandda_reject = models.TextField(blank=True, null=True)
    pandda_run = models.TextField(blank=True, null=True)
    pdb_path = models.TextField(blank=True, null=True)
    r_free = models.TextField(blank=True, null=True)
    res_high = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)


class Lab(models.Model):
    cryo_frac = models.TextField(blank=True, null=True)
    cryo_status = models.TextField(blank=True, null=True)
    cryo_stock_frac = models.TextField(blank=True, null=True)
    cryo_transfer_vol = models.TextField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    data_collection_visit = models.TextField(blank=True, null=True)
    expr_conc = models.TextField(blank=True, null=True)
    harvest_status = models.TextField(blank=True, null=True)
    library_name = models.TextField(blank=True, null=True)
    library_plate = models.TextField(blank=True, null=True)
    mounting_result = models.TextField(blank=True, null=True)
    mounting_time = models.TextField(blank=True, null=True)
    soak_status = models.TextField(blank=True, null=True)
    soak_time = models.TextField(blank=True, null=True)
    soak_vol = models.TextField(blank=True, null=True)
    solv_frac = models.TextField(blank=True, null=True)
    stock_conc = models.TextField(blank=True, null=True)
    visit = models.TextField(blank=True, null=True)

class Refinement(models.Model):
    bound_conf = models.TextField(blank=False, null=False, unique=True)
    cif = models.TextField(blank=True, null=True)
    cif_prog = models.TextField(blank=True, null=True)
    cif_status = models.TextField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    lig_bound_conf = models.TextField(blank=True, null=True)
    lig_cc = models.TextField(blank=True, null=True)
    lig_confidence = models.TextField(blank=True, null=True)
    matrix_weight = models.TextField(blank=True, null=True)
    molprobity_score = models.TextField(blank=True, null=True)
    mtz_free = models.TextField(blank=True, null=True)
    mtz_latest = models.TextField(blank=True, null=True)
    outcome = models.TextField(blank=True, null=True)
    pdb_latest = models.TextField(blank=True, null=True)
    r_free = models.TextField(blank=True, null=True)
    ramachandran_favoured = models.TextField(blank=True, null=True)
    ramachandran_outliers = models.TextField(blank=True, null=True)
    rcryst = models.TextField(blank=True, null=True)
    refinement_path = models.TextField(blank=True, null=True)
    res = models.TextField(blank=True, null=True)
    rmsd_angles = models.TextField(blank=True, null=True)
    rmsd_bonds = models.TextField(blank=True, null=True)
    spacegroup = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)


class ProasisHits(models.Model):
    bound_pdb = models.ForeignKey(Refinement, to_field='bound_conf', on_delete=models.CASCADE, unique=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    modification_date = models.TextField(blank=True, null=True)
    strucid = models.TextField(blank=True, null=True)
    ligand_list = models.IntegerField(blank=True, null=True)


class CrystalStatus(models.Model):
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)
    exists_proasis_pdb = models.BooleanField()
    exists_proasis_mtz = models.BooleanField()
    exists_proasis_2fofc = models.BooleanField()
    exists_proasis_fofc = models.BooleanField()
    exists_pandda_event_map = models.BooleanField()
    exists_ligand_cif = models.BooleanField()
    exists_bound_state_pdb = models.BooleanField()
    exists_bound_state_mtz = models.BooleanField()
    exists_ground_state_pdb = models.BooleanField()
    exists_ground_state_mtz = models.BooleanField()


class LigandEdstats(models.Model):
    baa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    ccpa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    ccsa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    npa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    rga = models.TextField(blank=True, null=True)  # Field name made lowercase.
    ra = models.TextField(blank=True, null=True)  # Field name made lowercase.
    srga = models.TextField(blank=True, null=True)  # Field name made lowercase.
    zccpa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    zd_a = models.TextField(blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters.
    zd_a_0 = models.TextField(blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters. Field renamed because of name conflict.
    zda = models.TextField(blank=True, null=True)  # Field name made lowercase.
    zoa = models.TextField(blank=True, null=True)  # Field name made lowercase.
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key # changed from crystal
    ligand = models.TextField(blank=True, null=True)
    strucid = models.ForeignKey(ProasisHits, on_delete=models.CASCADE)


class ProasisLeads(models.Model):
    reference_pdb = models.ForeignKey(Reference, to_field='reference_pdb', on_delete=models.CASCADE, unique=True)
    strucid = models.TextField(blank=True, null=True)


class Proposals(models.Model):
    proposal = models.ForeignKey(SoakdbFiles, on_delete=models.CASCADE)
    fedids = models.TextField(blank=True, null=True)


class Pandda(models.Model):
    crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
    event = models.IntegerField(blank=True, null=True)
    event_centroid = models.TextField(blank=True, null=True)
    event_dist_from_site_centroid = models.TextField(blank=True, null=True)
    input_dir = models.TextField(blank=True, null=True)
    lig_centroid = models.TextField(blank=True, null=True)
    lig_dist_event = models.FloatField(blank=True, null=True)
    lig_id = models.TextField(blank=True, null=True)
    pandda_dir = models.TextField(blank=True, null=True)
    pandda_event_map_native = models.TextField(blank=True, null=True)
    pandda_input_mtz = models.TextField(blank=True, null=True)
    pandda_input_pdb = models.TextField(blank=True, null=True)
    pandda_log = models.TextField(blank=True, null=True)
    pandda_model_pdb = models.TextField(blank=True, null=True)
    pandda_version = models.TextField(blank=True, null=True)
    site = models.IntegerField(blank=True, null=True)
    site_aligned_centroid = models.TextField(blank=True, null=True)
    site_native_centroid = models.TextField(blank=True, null=True)

