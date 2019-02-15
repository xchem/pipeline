from rest_framework import viewsets

from .models import Target, Compounds, Reference, SoakdbFiles, Crystal, DataProcessing, Dimple, Lab, Refinement, \
    PanddaAnalysis, PanddaRun, PanddaSite, PanddaEvent, ProasisOut
from .serializers import TargetSerializer, CompoundsSerializer, ReferenceSerializer, SoakdbFilesSerializer, \
    CrystalSerializer, DataProcessingSerializer, DimpleSerializer, LabSerializer, RefinementSerializer, \
    PanddaAnalysisSerializer, PanddaRunSerializer, PanddaSiteSerializer, PanddaEventSerializer, ProasisOutSerializer, FragspectCrystalView


class TargetView(viewsets.ReadOnlyModelViewSet):
    """
    Target: target names
    Filters:
        - target_name
    Returns:
        - target_name
    """
    queryset = Target.objects.filter()
    serializer_class = TargetSerializer
    filter_fields = (
        "target_name",
    )


class CompoundsView(viewsets.ReadOnlyModelViewSet):
    """
    Compounds: smiles strings for compounds
    """
    queryset = Compounds.objects.filter()
    serializer_class = CompoundsSerializer
    filter_fields = (
        "smiles",
    )


class ReferenceView(viewsets.ReadOnlyModelViewSet):
    """
    Reference: reference pdb files
    """
    queryset = Reference.objects.filter()
    serializer_class = ReferenceSerializer
    filter_fields = (
        "reference_pdb",
    )


class SoakdbFilesView(viewsets.ReadOnlyModelViewSet):

    queryset = SoakdbFiles.objects.filter()
    serializer_class = SoakdbFilesSerializer
    filter_fields = (
        "filename",
        "proposal__proposal",
        "visit",
    )


class CrystalView(viewsets.ReadOnlyModelViewSet):
    queryset = Crystal.objects.filter()
    serializer_class = CrystalSerializer
    filter_fields = (
        "crystal_name",
        "target__target_name",
        "compound__smiles",
        "visit__filename",
        "visit__proposal__proposal",
        "visit__visit",
    )


class DataProcessingView(viewsets.ReadOnlyModelViewSet):
    queryset = DataProcessing.objects.filter()
    serializer_class = DataProcessingSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
    )


class DimpleView(viewsets.ReadOnlyModelViewSet):
    queryset = Dimple.objects.filter()
    serializer_class = DimpleSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "reference__reference_pdb"
    )


class LabView(viewsets.ReadOnlyModelViewSet):
    queryset = Lab.objects.filter()
    serializer_class = LabSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "data_collection_visit",
        "library_name",
        "library_plate",
    )


class RefinementView(viewsets.ReadOnlyModelViewSet):
    queryset = Refinement.objects.filter()
    serializer_class = RefinementSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "outcome",
    )


class PanddaAnalysisView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaAnalysis.objects.filter()
    serializer_class = PanddaAnalysisSerializer
    filter_fields = (
        "pandda_dir",
    )


class PanddaRunView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaRun.objects.filter()
    serializer_class = PanddaRunSerializer
    filter_fields = (
        "input_dir",
        "pandda_analysis__pandda_dir",
        "pandda_log",
        "pandda_version",
        "sites_file",
        "events_file",
    )


class PanddaSiteView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaSite.objects.filter()
    serializer_class = PanddaSiteSerializer
    filter_fields = (
        "pandda_run__pandda_analysis__pandda_dir",
        "pandda_run__pandda_log",
        "pandda_run__pandda_sites_file",
        "pandda_run__pandda_events_file",
        "pandda_run__input_dir",
        "site",
    )


class PanddaEventView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaEvent.objects.filter()
    serializer_class = PanddaEventSerializer
    filter_fields = (
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        "pandda_run__pandda_analysis__pandda_dir",
        "pandda_run__pandda_log",
        "pandda_run__pandda_sites_file",
        "pandda_run__pandda_events_file",
        "pandda_run__input_dir",
        "site__site",
        "event",
        "lig_id",
        "pandda_event_map_native",
        "pandda_model_pdb",
        "pandda_input_mtz",
        "pandda_input_pdb",
    )


class ProasisOutView(viewsets.ReadOnlyModelViewSet):
    queryset = ProasisOut.objects.filter()
    serializer_class = ProasisOutSerializer
    filter_fields = (
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        "proasis__strucid",
        "proasis__crystal_name__crystal_name",
        "proasis__crystal_name__target__target_name",
        "proasis__crystal_name__compound__smiles",
        "proasis__crystal_name__visit__filename",
        "proasis__crystal_name__visit__proposal__proposal",
        "proasis__crystal_name__visit__visit",
        "proasis__refinement__crystal_name__crystal_name",
        "proasis__refinement__crystal_name__target__target_name",
        "proasis__refinement__crystal_name__compound__smiles",
        "proasis__refinement__crystal_name__visit__filename",
        "proasis__refinement__crystal_name__visit__proposal__proposal",
        "proasis__refinement__crystal_name__visit__visit",
        "proasis__refinement__outcome",
        "root",
        'start',
    )


class FragspectTestCrystalView(viewsets.ReadOnlyModelViewSet):
    queryset = Refinement.objects.filter()
    serializer_class = FragspectCrystalView
    filter_fields = ('crystal_name__target__target_name',)

