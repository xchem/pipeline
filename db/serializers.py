from rest_framework import serializers
from .models import Target, Compounds, Reference, SoakdbFiles, Crystal, DataProcessing


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
        fields = ("reference_pdb")


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


