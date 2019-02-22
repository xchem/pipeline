from rest_framework import serializers

from xchem_db.models import Target, Compounds, Reference, SoakdbFiles, Crystal, DataProcessing, Dimple, Lab, \
    Refinement, PanddaAnalysis, PanddaRun, PanddaSite, PanddaEvent, ProasisOut, Proposals, PanddaEventStats


class ProposalsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Proposals
        fields = fields = ('__all__',)


class TargetSerializer(serializers.ModelSerializer):

    class Meta:
        model = Target
        fields = fields = ('__all__',)


class CompoundsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Compounds
        fields = fields = ('__all__',)


class ReferenceSerializer(serializers.ModelSerializer):

    class Meta:
        model = Reference
        fields = fields = ('__all__',)


class SoakdbFilesSerializer(serializers.ModelSerializer):

    class Meta:
        model = SoakdbFiles
        fields = fields = ('__all__',)


class CrystalSerializer(serializers.ModelSerializer):

    class Meta:
        model = Crystal
        fields = fields = ('__all__',)


class DataProcessingSerializer(serializers.ModelSerializer):

    class Meta:
        model = DataProcessing
        fields = fields = ('__all__',)


class DimpleSerializer(serializers.ModelSerializer):

    class Meta:
        model = Dimple
        fields = fields = ('__all__',)


class LabSerializer(serializers.ModelSerializer):

    class Meta:
        model = Lab
        fields = fields = ('__all__',)


class RefinementSerializer(serializers.ModelSerializer):

    class Meta:
        model = Refinement
        fields = fields = ('__all__',)


class PanddaAnalysisSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaAnalysis
        fields = fields = ('__all__',)


class PanddaRunSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaRun
        fields = fields = ('__all__',)


class PanddaSiteSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaSite
        fields = fields = ('__all__',)


class PanddaEventSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaEvent
        fields = fields = ('__all__',)


class ProasisOutSerializer(serializers.ModelSerializer):

    class Meta:
        model = ProasisOut
        fields = fields = ('__all__',)


class PanddaEventStatsSerializer(serializers.ModelSerializer):

    class Meta:
        model = PanddaEventStats
        fields = fields = ('__all__',)


class FragspectCrystalSerializer(serializers.ModelSerializer):

    crystal = serializers.CharField(source='crystal.crystal_name')
    site_number = serializers.IntegerField(source='site.site')
    event_number = serializers.IntegerField(source='event')
    target_name = serializers.CharField(source='crystal.target.target_name')
    crystal_status = serializers.CharField(source='refinement.outcome')
    confidence = serializers.CharField(source='ligand_confidence')
    crystal_resolution = serializers.CharField(source='refinement.res')
    smiles = serializers.CharField(source='crystal.compound.smiles')
    spacegroup = serializers.CharField(source='refinement.spacegroup')
    cell = serializers.CharField(source='data_proc.unit_cell')
    event_comment = serializers.CharField(source='comment')
    event_status = serializers.IntegerField(source='status')

    class Meta:
        model = PanddaEvent
        fields = (
            'id',
            'crystal',
            'site_number',
            'event_number',
            'lig_id',
            'target_name',
            'crystal_status',
            'event_status',
            'confidence',
            'crystal_resolution',
            'smiles',
            'spacegroup',
            'cell',
            'event_comment',
            'interesting',
        )
