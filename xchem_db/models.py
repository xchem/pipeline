# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from __future__ import unicode_literals

from django.db import models

import os


class Target(models.Model):
    target_name = models.CharField(max_length=255, blank=False, null=False, unique=True)

    class Meta:
        app_label = 'xchem_db'
        db_table = 'target'


class Compounds(models.Model):
    smiles = models.CharField(max_length=255, blank=True, null=True, unique=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'compounds'


class Reference(models.Model):
    reference_pdb = models.CharField(max_length=255, null=True, default='not_assigned', unique=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'reference'


class Proposals(models.Model):
    # TODO - can we refactor this for title
    proposal = models.CharField(max_length=255, blank=False, null=False, unique=True)
    title = models.CharField(max_length=10, blank=True, null=True)
    fedids = models.TextField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'proposals'


class SoakdbFiles(models.Model):
    filename = models.CharField(max_length=255, blank=False, null=False, unique=True)
    modification_date = models.BigIntegerField(blank=False, null=False)
    proposal = models.ForeignKey(Proposals, on_delete=models.CASCADE, unique=False)
    visit = models.TextField(blank=False, null=False)
    status = models.IntegerField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'soakdb_files'


class Crystal(models.Model):
    crystal_name = models.CharField(max_length=255, blank=False, null=False)
    target = models.ForeignKey(Target, on_delete=models.CASCADE)
    compound = models.ForeignKey(Compounds, on_delete=models.CASCADE, null=True, blank=True)
    visit = models.ForeignKey(SoakdbFiles, on_delete=models.CASCADE)

    # model types
    PREPROCESSING = 'PP'
    PANDDA = 'PD'
    PROASIS = 'PR'
    REFINEMENT = 'RE'
    COMPCHEM = 'CC'
    DEPOSITION = 'DP'

    CHOICES = (
        (PREPROCESSING, 'preprocessing'),
        (PANDDA, 'pandda'),
        (REFINEMENT, 'refinement'),
        (COMPCHEM, 'comp_chem'),
        (DEPOSITION, 'deposition')
    )

    status = models.CharField(choices=CHOICES, max_length=2, default=PREPROCESSING)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'crystal'
        unique_together = ('crystal_name', 'visit', 'compound')


class DataProcessing(models.Model):
    auto_assigned = models.TextField(blank=True, null=True)
    cchalf_high = models.FloatField(blank=True, null=True)
    cchalf_low = models.FloatField(blank=True, null=True)
    cchalf_overall = models.FloatField(blank=True, null=True)
    completeness_high = models.FloatField(blank=True, null=True)
    completeness_low = models.FloatField(blank=True, null=True)
    completeness_overall = models.FloatField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE, unique=True)  # changed to foreign key
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
    res_overall = models.TextField(blank=True, null=True)  # range
    score = models.FloatField(blank=True, null=True)
    spacegroup = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    unique_ref_overall = models.IntegerField(blank=True, null=True)
    unit_cell = models.TextField(blank=True, null=True)
    unit_cell_vol = models.FloatField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'data_processing'


class Dimple(models.Model):
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE, unique=True)  # changed to foreign key
    mtz_path = models.CharField(max_length=255, blank=True, null=True)
    pdb_path = models.CharField(max_length=255, blank=True, null=True)
    r_free = models.FloatField(blank=True, null=True)
    res_high = models.FloatField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    reference = models.ForeignKey(Reference, blank=True, null=True, on_delete=models.CASCADE)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'dimple'
        unique_together = ('pdb_path', 'mtz_path')


class Lab(models.Model):
    cryo_frac = models.FloatField(blank=True, null=True)
    cryo_status = models.TextField(blank=True, null=True)
    cryo_stock_frac = models.FloatField(blank=True, null=True)
    cryo_transfer_vol = models.FloatField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE, unique=True)  # changed to foreign key
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
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'lab'


class Refinement(models.Model):
    bound_conf = models.CharField(max_length=255, blank=True, null=True, unique=True)
    cif = models.TextField(blank=True, null=True)
    cif_prog = models.TextField(blank=True, null=True)
    cif_status = models.TextField(blank=True, null=True)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE, unique=True)  # changed to foreign key
    lig_bound_conf = models.TextField(blank=True, null=True)
    lig_cc = models.TextField(blank=True, null=True)
    lig_confidence = models.TextField(blank=True, null=True)
    lig_confidence_int = models.IntegerField(blank=True, null=True)
    lig_confidence_string = models.TextField(blank=True, null=True)
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
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'refinement'


class ProasisHits(models.Model):
    refinement = models.ForeignKey(Refinement, on_delete=models.CASCADE)
    pdb_file = models.TextField(blank=False, null=False)
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
    modification_date = models.TextField(blank=True, null=True)
    strucid = models.TextField(blank=True, null=True)
    ligand_list = models.TextField(blank=True, null=True)
    mtz = models.TextField(blank=False, null=False)
    two_fofc = models.TextField(blank=False, null=False)
    fofc = models.TextField(blank=False, null=False)
    sdf = models.TextField(blank=True, null=True)
    altconf = models.CharField(max_length=255, blank=True, null=True)
    added = models.DateTimeField(auto_now_add=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'proasis_hits'
        unique_together = ('refinement', 'crystal_name', 'altconf')


class LigandEdstats(models.Model):
    baa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ccpa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ccsa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    npa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    rga = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    ra = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    srga = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zccpa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zd_a = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zd_a_0 = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zda = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    zoa = models.FloatField(blank=True, null=True)  # Field name made lowercase.
    crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key # changed from crystal
    ligand = models.CharField(max_length=255, blank=True, null=True)
    strucid = models.ForeignKey(ProasisHits, on_delete=models.CASCADE)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'ligand_edstats'
        unique_together = ('crystal_name', 'ligand', 'strucid')


class ProasisLeads(models.Model):
    reference_pdb = models.ForeignKey(Reference, to_field='reference_pdb', on_delete=models.CASCADE, unique=True)
    strucid = models.CharField(max_length=255, blank=True, null=True, unique=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'proasis_leads'


class PanddaAnalysis(models.Model):
    pandda_dir = models.CharField(max_length=255, unique=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_analysis'


class PanddaRun(models.Model):
    input_dir = models.TextField(blank=True, null=True)
    pandda_analysis = models.ForeignKey(PanddaAnalysis, on_delete=models.CASCADE)
    pandda_log = models.CharField(max_length=255, unique=True)
    pandda_version = models.TextField(blank=True, null=True)
    sites_file = models.TextField(blank=True, null=True)
    events_file = models.TextField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_run'


class PanddaStatisticalMap(models.Model):
    resolution_from = models.FloatField(blank=True, null=True)
    resolution_to = models.FloatField(blank=True, null=True)
    dataset_list = models.TextField()
    pandda_run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_statistical_map'
        unique_together = ('resolution_from', 'resolution_to', 'pandda_run')


class PanddaSite(models.Model):
    pandda_run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
    site = models.IntegerField(blank=True, null=True)
    site_aligned_centroid_x = models.FloatField(blank=True, null=True)
    site_aligned_centroid_y = models.FloatField(blank=True, null=True)
    site_aligned_centroid_z = models.FloatField(blank=True, null=True)
    site_native_centroid_x = models.FloatField(blank=True, null=True)
    site_native_centroid_y = models.FloatField(blank=True, null=True)
    site_native_centroid_z = models.FloatField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_site'
        unique_together = ('pandda_run', 'site')


class PanddaEvent(models.Model):
    crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
    site = models.ForeignKey(PanddaSite, on_delete=models.CASCADE)
    pandda_run = models.ForeignKey(PanddaRun, on_delete=models.CASCADE)
    event = models.IntegerField(blank=True, null=True)
    event_centroid_x = models.FloatField(blank=True, null=True)
    event_centroid_y = models.FloatField(blank=True, null=True)
    event_centroid_z = models.FloatField(blank=True, null=True)
    event_dist_from_site_centroid = models.TextField(blank=True, null=True)
    lig_centroid_x = models.FloatField(blank=True, null=True)
    lig_centroid_y = models.FloatField(blank=True, null=True)
    lig_centroid_z = models.FloatField(blank=True, null=True)
    lig_dist_event = models.FloatField(blank=True, null=True)
    lig_id = models.TextField(blank=True, null=True)
    pandda_event_map_native = models.TextField(blank=True, null=True)
    pandda_event_map_cut = models.TextField(blank=True, null=True)
    pandda_model_pdb = models.TextField(blank=True, null=True)
    pandda_input_mtz = models.TextField(blank=True, null=True)
    pandda_input_pdb = models.TextField(blank=True, null=True)
    ligand_confidence_inspect = models.TextField(blank=True, null=True)
    ligand_confidence = models.TextField(blank=True, null=True)

    # model types
    NONE = 'NA'
    SOAKDB = 'SD'
    FRAGSPECT = 'FS'

    CHOICES = (
        (NONE, 'none'),
        (SOAKDB, 'soak_db'),
        (FRAGSPECT, 'fragspect')
    )

    ligand_confidence_source = models.CharField(choices=CHOICES, max_length=2, default=NONE)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_event'
        unique_together = ('site', 'event', 'crystal', 'pandda_run')


class PanddaEventStats(models.Model):
    event = models.ForeignKey(PanddaEvent, on_delete=models.CASCADE)
    one_minus_bdc = models.FloatField(blank=True, null=True)
    cluster_size = models.IntegerField(blank=True, null=True)
    glob_corr_av_map = models.FloatField(blank=True, null=True)
    glob_corr_mean_map = models.FloatField(blank=True, null=True)
    loc_corr_av_map = models.FloatField(blank=True, null=True)
    loc_corr_mean_map = models.FloatField(blank=True, null=True)
    z_mean = models.FloatField(blank=True, null=True)
    z_peak = models.FloatField(blank=True, null=True)
    b_factor_scaled = models.FloatField(blank=True, null=True)
    high_res = models.FloatField(blank=True, null=True)
    low_res = models.FloatField(blank=True, null=True)
    r_free = models.FloatField(blank=True, null=True)
    r_work = models.FloatField(blank=True, null=True)
    ref_rmsd = models.FloatField(blank=True, null=True)
    wilson_scaled_b = models.FloatField(blank=True, null=True)
    wilson_scaled_ln_dev = models.FloatField(blank=True, null=True)
    wilson_scaled_ln_dev_z = models.FloatField(blank=True, null=True)
    wilson_scaled_ln_rmsd = models.FloatField(blank=True, null=True)
    wilson_scaled_ln_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_scaled_below_four_rmsd = models.FloatField(blank=True, null=True)
    wilson_scaled_below_four_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_scaled_above_four_rmsd = models.FloatField(blank=True, null=True)
    wilson_scaled_above_four_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_scaled_rmsd_all = models.FloatField(blank=True, null=True)
    wilson_scaled_rmsd_all_z = models.FloatField(blank=True, null=True)
    wilson_unscaled = models.FloatField(blank=True, null=True)
    wilson_unscaled_ln_dev = models.FloatField(blank=True, null=True)
    wilson_unscaled_ln_dev_z = models.FloatField(blank=True, null=True)
    wilson_unscaled_ln_rmsd = models.FloatField(blank=True, null=True)
    wilson_unscaled_ln_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_unscaled_below_four_rmsd = models.FloatField(blank=True, null=True)
    wilson_unscaled_below_four_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_unscaled_above_four_rmsd = models.FloatField(blank=True, null=True)
    wilson_unscaled_above_four_rmsd_z = models.FloatField(blank=True, null=True)
    wilson_unscaled_rmsd_all = models.FloatField(blank=True, null=True)
    wilson_unscaled_rmsd_all_z = models.FloatField(blank=True, null=True)
    resolution = models.FloatField(blank=True, null=True)
    map_uncertainty = models.FloatField(blank=True, null=True)
    obs_map_mean = models.FloatField(blank=True, null=True)
    obs_map_rms = models.FloatField(blank=True, null=True)
    z_map_kurt = models.FloatField(blank=True, null=True)
    z_map_mean = models.FloatField(blank=True, null=True)
    z_map_skew = models.FloatField(blank=True, null=True)
    z_map_std = models.FloatField(blank=True, null=True)
    scl_map_mean = models.FloatField(blank=True, null=True)
    scl_map_rms = models.FloatField(blank=True, null=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'pandda_event_stats'


class ProasisPandda(models.Model):
    crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)
    hit = models.ForeignKey(ProasisHits, on_delete=models.CASCADE)
    event = models.ForeignKey(PanddaEvent, on_delete=models.CASCADE)
    event_map_native = models.TextField(blank=False, null=False)
    model_pdb = models.TextField(blank=False, null=False)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'proasis_pandda'
        unique_together = ('crystal', 'hit', 'event')


class ProasisOut(models.Model):
    crystal = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # done
    proasis = models.ForeignKey(ProasisHits, on_delete=models.CASCADE)  # done
    ligand = models.CharField(max_length=255, blank=False, null=False)  # done
    ligid = models.IntegerField(blank=True, null=True)
    root = models.TextField(blank=True, null=True)  # root directory for all crystals in this project (target)
    start = models.TextField(blank=True, null=True)  # directory name for this crystal within root
    curated = models.TextField(blank=True, null=True)  # done
    sdf = models.TextField(blank=True, null=True)  # done
    apo = models.TextField(blank=True, null=True)  # done
    mol = models.TextField(blank=True, null=True)  # done
    mol2 = models.TextField(blank=True, null=True)  # done
    h_mol = models.TextField(blank=True, null=True)  # done
    stripped = models.TextField(blank=True, null=True)  # done
    event = models.TextField(blank=True, null=True)
    mtz = models.TextField(blank=True, null=True)
    contacts = models.TextField(blank=True, null=True)  # done
    acc = models.TextField(blank=True, null=True)
    don = models.TextField(blank=True, null=True)
    lip = models.TextField(blank=True, null=True)
    pmap = models.TextField(blank=True, null=True)
    ppdb = models.TextField(blank=True, null=True)
    pjson = models.TextField(blank=True, null=True)
    pmtz = models.TextField(blank=True, null=True)
    added = models.DateTimeField(auto_now_add=True)

    class Meta:
        if os.getcwd() != '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV':
            app_label = 'xchem_db'
        db_table = 'proasis_out'
        unique_together = ('crystal', 'proasis', 'ligand', 'ligid')

