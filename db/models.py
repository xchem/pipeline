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

    class Meta:
        db_table = 'target'


class Compounds(models.Model):
    smiles = models.TextField(blank=False, null=False, unique=True)

    class Meta:
        db_table = 'compounds'


class Reference(models.Model):
    reference_pdb = models.TextField(null=True, default='not_assigned', unique=True)

    class Meta:
        db_table = 'reference'


class SoakdbFiles(models.Model):
    filename = models.TextField(blank=False, null=False, unique=True)
    modification_date = models.BigIntegerField(blank=False, null=False)
    proposal = models.TextField(blank=False, null=False)
    status = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'soakdb_files'


class Crystal(models.Model):
    crystal_name = models.TextField(blank=False, null=False, unique=True)
    target = models.ForeignKey(Target, on_delete=models.CASCADE)
    compound = models.ForeignKey(Compounds, on_delete=models.CASCADE)
    file = models.ForeignKey(SoakdbFiles, on_delete=models.CASCADE)

    # date

    class Meta:
        db_table = 'crystal'


class DataProcessing(models.Model):
    auto_assigned = models.TextField(blank=True, null=True)
    cchalf_high = models.FloatField(blank=True, null=True)
    cchalf_low = models.FloatField(blank=True, null=True)
    cchalf_overall = models.FloatField(blank=True, null=True)
    completeness_high = models.FloatField(blank=True, null=True)
    completeness_low = models.FloatField(blank=True, null=True)
    completeness_overall = models.FloatField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE) # changed to foreign key
    dimple_mtz_path = models.TextField(blank=True, null=True)
    dimple_pdb_path = models.TextField(blank=True, null=True)
    dimple_status = models.TextField(blank=True, null=True)
    image_path = models.TextField(blank=True, null=True)
    isig_high = models.FloatField(blank=True, null=True)
    isig_low = models.FloatField(blank=True, null=True)
    isig_overall = models.FloatField(blank=True, null=True)
    lattice = models.TextField(blank=True, null=True)
    log_name = models.TextField(blank=True, null=True)
    logfile_path = models.TextField(blank=True, null=True)
    mtz_name = models.TextField(blank=True, null=True)
    mtz_path = models.TextField(blank=True, null=True)
    multiplicity_high = models.FloatField(blank=True, null=True)
    multiplicity_low = models.FloatField(blank=True, null=True)
    multiplicity_overall = models.FloatField(blank=True, null=True)
    original_directory = models.TextField(blank=True, null=True)
    point_group = models.TextField(blank=True, null=True)
    program = models.TextField(blank=True, null=True)
    r_cryst = models.FloatField(blank=True, null=True)
    r_free = models.FloatField(blank=True, null=True)
    r_merge_high = models.FloatField(blank=True, null=True)
    r_merge_low = models.FloatField(blank=True, null=True)
    r_merge_overall = models.FloatField(blank=True, null=True)
    res_high = models.FloatField(blank=True, null=True)
    res_high_15_sigma = models.FloatField(blank=True, null=True)
    res_high_outer_shell = models.FloatField(blank=True, null=True)
    res_low = models.FloatField(blank=True, null=True)
    res_low_inner_shell = models.FloatField(blank=True, null=True)
    res_overall = models.TextField(blank=True, null=True) # range
    score = models.FloatField(blank=True, null=True)
    spacegroup = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    unique_ref_overall = models.IntegerField(blank=True, null=True)
    unit_cell = models.TextField(blank=True, null=True)
    unit_cell_vol = models.FloatField(blank=True, null=True)

    class Meta:
        db_table = 'data_processing'


class Dimple(models.Model):
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    mtz_path = models.TextField(blank=True, null=True)
    pdb_path = models.TextField(blank=True, null=True)
    r_free = models.FloatField(blank=True, null=True)
    res_high = models.FloatField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    reference = models.ForeignKey(Reference, blank=True, null=True, on_delete=models.CASCADE)

    class Meta:
        db_table = 'dimple'
        unique_together = ('pdb_path', 'mtz_path')


class Lab(models.Model):
    cryo_frac = models.FloatField(blank=True, null=True)
    cryo_status = models.TextField(blank=True, null=True)
    cryo_stock_frac = models.FloatField(blank=True, null=True)
    cryo_transfer_vol = models.FloatField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    data_collection_visit = models.TextField(blank=True, null=True)
    expr_conc = models.FloatField(blank=True, null=True)
    harvest_status = models.TextField(blank=True, null=True)
    library_name = models.TextField(blank=True, null=True)
    library_plate = models.TextField(blank=True, null=True)
    mounting_result = models.TextField(blank=True, null=True)
    mounting_time = models.TextField(blank=True, null=True)
    soak_status = models.TextField(blank=True, null=True)
    soak_time = models.TextField(blank=True, null=True)
    soak_vol = models.FloatField(blank=True, null=True)
    solv_frac = models.FloatField(blank=True, null=True)
    stock_conc = models.FloatField(blank=True, null=True)
    visit = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'lab'

class Refinement(models.Model):
    bound_conf = models.TextField(blank=True, null=True, unique=True)
    cif = models.TextField(blank=True, null=True)
    cif_prog = models.TextField(blank=True, null=True)
    cif_status = models.TextField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    lig_bound_conf = models.TextField(blank=True, null=True)
    lig_cc = models.TextField(blank=True, null=True)
    lig_confidence = models.TextField(blank=True, null=True)
    matrix_weight = models.TextField(blank=True, null=True)
    molprobity_score = models.FloatField(blank=True, null=True)
    mtz_free = models.TextField(blank=True, null=True)
    mtz_latest = models.TextField(blank=True, null=True)
    outcome = models.IntegerField(blank=True, null=True)
    pdb_latest = models.TextField(blank=True, null=True)
    r_free = models.FloatField(blank=True, null=True)
    ramachandran_favoured = models.TextField(blank=True, null=True)
    ramachandran_outliers = models.TextField(blank=True, null=True)
    rcryst = models.FloatField(blank=True, null=True)
    refinement_path = models.TextField(blank=True, null=True)
    res = models.FloatField(blank=True, null=True)
    rmsd_angles = models.TextField(blank=True, null=True)
    rmsd_bonds = models.TextField(blank=True, null=True)
    spacegroup = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)


    class Meta:
        db_table = 'refinement'


class ProasisHits(models.Model):
    bound_pdb = models.ForeignKey(Refinement, to_field='bound_conf', on_delete=models.CASCADE, unique=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    modification_date = models.TextField(blank=True, null=True)
    strucid = models.TextField(blank=True, null=True)
    ligand_list = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'proasis_hits'


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

    class Meta:
        db_table = 'crystal_status'


class LigandEdstats(models.Model):
    baa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ccpa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ccsa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    npa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    rga = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ra = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    srga = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zccpa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zd_a = models.FloatField(blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters.
    zd_a_0 = models.FloatField(blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters. Field renamed because of name conflict.
    zda = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zoa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key # changed from crystal
    ligand = models.TextField(blank=True, null=True)
    strucid = models.ForeignKey(ProasisHits, on_delete=models.CASCADE)

    class Meta:
        db_table = 'ligand_edstats'
        unique_together = ('crystal_name', 'ligand', 'strucid')


class ProasisLeads(models.Model):
    reference_pdb = models.ForeignKey(Reference, to_field='reference_pdb', on_delete=models.CASCADE, unique=True)
    strucid = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'proasis_leads'


class Proposals(models.Model):
    proposal = models.ForeignKey(SoakdbFiles, on_delete=models.CASCADE)
    fedids = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'proposals'


class PanddaRun(models.Model):
    input_dir = models.TextField(blank=True, null=True)
    pandda_dir = models.TextField(blank=True, null=True)
    pandda_log = models.TextField(blank=True, null=True, unique=True)
    pandda_version = models.TextField(blank=True, null=True)
    sites_file = models.TextField(blank=True, null=True)
    events_file = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'pandda_run'
        # unique_together = ('crystal', 'pandda_log')


class PanddaSite(models.Model):
    run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
    site = models.IntegerField(blank=True, null=True)
    site_aligned_centroid = models.TextField(blank=True, null=True)
    site_native_centroid = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'pandda_site'
        unique_together = ('run', 'site')


class PanddaEvent(models.Model):
    crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
    site = models.ForeignKey(PanddaSite, on_delete=models.CASCADE)
    run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
    event = models.IntegerField(blank=True, null=True)
    event_centroid = models.TextField(blank=True, null=True)
    event_dist_from_site_centroid = models.TextField(blank=True, null=True)
    lig_centroid = models.TextField(blank=True, null=True)
    lig_dist_event = models.FloatField(blank=True, null=True)
    lig_id = models.TextField(blank=True, null=True)
    pandda_event_map_native = models.TextField(blank=True, null=True)
    pandda_model_pdb = models.TextField(blank=True, null=True)
    pandda_input_mtz = models.TextField(blank=True, null=True)
    pandda_input_pdb = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'pandda_event'
        unique_together = ('site', 'event', 'crystal', 'run')

