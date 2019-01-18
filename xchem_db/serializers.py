from rest_framework import serializers

from xchem_db.models import Target, Compounds, Reference, SoakdbFiles, Crystal, DataProcessing, Dimple, Lab, Refinement, \
    PanddaAnalysis, PanddaRun, PanddaSite, PanddaEvent, ProasisOut


class TargetSerializer(serializers.ModelSerializer):

    class Meta:
        model = Target
        fields = ("target_name",)


class CompoundsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Compounds
        fields = ("smiles",)


class ReferenceSerializer(serializers.ModelSerializer):

    class Meta:
        model = Reference
        fields = ("reference_pdb",)


class SoakdbFilesSerializer(serializers.ModelSerializer):

    class Meta:
        model = SoakdbFiles
        fields = (
            "filename",
            "modification_date",
            "proposal",
            "visit",
            "status",
        )


class CrystalSerializer(serializers.ModelSerializer):

    class Meta:
        model = Crystal
        fields = (
            "crystal_name",
            "target",
            "compound",
            "visit",
            "status",
        )


class DataProcessingSerializer(serializers.ModelSerializer):

    class Meta:
        model = DataProcessing
        fields = (
            "cchalf_high",
            "cchalf_low",
            "cchalf_overall",
            "completeness_high",
            "completeness_low",
            "completeness_overall",
            "crystal_name",
            "isig_high",
            "isig_low",
            "isig_overall",
            "lattice",
            "multiplicity_high",
            "multiplicity_low",
            "multiplicity_overall",
            "point_group",
            "program",
            "r_cryst",
            "r_free",
            "r_merge_high",
            "r_merge_low",
            "r_merge_overall",
            "res_high",
            "res_high_15_sigma",
            "res_high_outer_shell",
            "res_low",
            "res_low_inner_shell",
            "res_overall",
            "score",
            "spacegroup",
            "unique_ref_overall",
            "unit_cell",
            "unit_cell_vol",
        )


class DimpleSerializer(serializers.ModelSerializer):

    class Meta:
        model = Dimple
        fields = (
            "crystal_name",
            "r_free",
            "res_high",
            "reference",
        )


class LabSerializer(serializers.ModelSerializer):

    class Meta:
        model = Lab
        fields = (
            "cryo_frac",
            "cryo_status",
            "cryo_stock_frac",
            "cryo_transfer_vol",
            "crystal_name",
            "data_collection_visit",
            "expr_conc",
            "harvest_status",
            "library_name",
            "library_plate",
            "mounting_result",
            "mounting_time",
            "soak_status",
            "soak_time",
            "soak_vol",
            "solv_frac",
            "stock_conc",
            "visit",

        )


class RefinementSerializer(serializers.ModelSerializer):

    class Meta:
        model = Refinement
        fields = (
            "bound_conf",
            "cif",
            "cif_prog",
            "cif_status",
            "crystal_name",
            "lig_bound_conf",
            "lig_cc",
            "lig_confidence",
            "matrix_weight",
            "molprobity_score",
            "mtz_free",
            "mtz_latest",
            "outcome",
            "pdb_latest",
            "r_free",
            "ramachandran_favoured",
            "ramachandran_outliers",
            "rcryst",
            "refinement_path",
            "res",
            "rmsd_angles",
            "rmsd_bonds",
            "spacegroup",
        )


class PanddaAnalysisSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaAnalysis
        fields = (
            "pandda_dir",
        )


class PanddaRunSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaRun
        fields = (
            "input_dir",
            "pandda_analysis",
            "pandda_log",
            "pandda_version",
            "sites_file",
            "events_file",
        )


class PanddaSiteSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaSite
        fields = (
            "pandda_run",
            "site",
            "site_aligned_centroid_x",
            "site_aligned_centroid_y",
            "site_aligned_centroid_z",
            "site_native_centroid_x",
            "site_native_centroid_y",
            "site_native_centroid_z",
        )


class PanddaEventSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaEvent
        fields = (
            "crystal",
            "site",
            "pandda_run",
            "event",
            "event_centroid_x",
            "event_centroid_y",
            "event_centroid_z",
            "event_dist_from_site_centroid",
            "lig_centroid_x",
            "lig_centroid_y",
            "lig_centroid_z",
            "lig_dist_event",
            "lig_id",
            "pandda_event_map_native",
            "pandda_model_pdb",
            "pandda_input_mtz",
            "pandda_input_pdb",
        )


class ProasisOutSerializer(serializers.ModelSerializer):

    class Meta:
        model = ProasisOut
        fields = (
            "crystal",
            "proasis",
            "ligand",
            "ligid",
            "root",
            "start",
            "curated",
            "sdf",
            "apo",
            "mol",
            "mol2",
            "h_mol",
            "stripped",
            "event",
            "mtz",
            "contacts",
            "acc",
            "don",
            "lip",
            "pmap",
            "ppdb",
            "pjson",
            "pmtz",
        )
