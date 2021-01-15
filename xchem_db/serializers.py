from rest_framework import serializers

from xchem_db.models import *


class ProposalsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Proposals
        fields = ('__all__',)


class TargetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Target
        fields = ('__all__',)


class CompoundsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Compounds
        fields = ('__all__',)


class ReferenceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reference
        fields = ('__all__',)


class SoakdbFilesSerializer(serializers.ModelSerializer):
    class Meta:
        model = SoakdbFiles
        fields = ('__all__',)


class CrystalSerializer(serializers.ModelSerializer):
    class Meta:
        model = Crystal
        fields = ('__all__',)


class DataProcessingSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataProcessing
        fields = ('__all__',)


class DimpleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dimple
        fields = ('__all__',)


class LabSerializer(serializers.ModelSerializer):
    class Meta:
        model = Lab
        fields = ('__all__',)


class RefinementSerializer(serializers.ModelSerializer):
    class Meta:
        model = Refinement
        fields = ('__all__',)


class PanddaAnalysisSerializer(serializers.ModelSerializer):
    class Meta:
        model = PanddaAnalysis
        fields = ('__all__',)


class PanddaRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = PanddaRun
        fields = ('__all__',)


class PanddaSiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = PanddaSite
        fields = ('__all__',)


class PanddaEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = PanddaEvent
        fields = ('__all__',)


class PanddaEventStatsSerializer(serializers.ModelSerializer):
    class Meta:
        model = PanddaEventStats
        fields = ('__all__',)


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


class MiscFilesSerializer(serializers.ModelSerializer):
    class Meta:
        model = MiscFiles
        fields = ('__all__',)


class FragalysisTargetSerializer(serializers.ModelSerializer):
    class Meta:
        model = FragalysisTarget
        fields = ('__all__',)


class FragalysisLigandSerializer(serializers.ModelSerializer):
    class Meta:
        model = FragalysisLigand
        fields = ('__all__',)


class LigandSerializer(serializers.ModelSerializer):
    class Meta:
        model = Ligand
        fields = ('__all__',)


class ReviewResponsesSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReviewResponses
        fields = ('__all__',)


class ReviewResponses2Serializer(serializers.ModelSerializer):
    class Meta:
        model = ReviewResponses2
        fields = ('__all__',)


class BadAtomsSerializer(serializers.ModelSerializer):
    class Meta:
        model = BadAtoms
        fields = ('__all__',)


class MetaDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = MetaData
        fields = ('__all__',)
