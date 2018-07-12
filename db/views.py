from .serializers import TargetSerializer, CompoundsSerializer, ReferenceSerializer, SoakdbFilesSerializer, \
    CrystalSerializer, DataProcessingSerializer, DimpleSerializer, LabSerializer, RefinementSerializer, \
    PanddaAnalysisSerializer, PanddaRunSerializer, PanddaSiteSerializer, PanddaEventSerializer
from rest_framework import viewsets
from .models import Target, Compounds, Reference, SoakdbFiles, Crystal, DataProcessing, Dimple, Lab, Refinement, \
    PanddaAnalysis, PanddaRun, PanddaSite, PanddaEvent



class TargetView(viewsets.ReadOnlyModelViewSet):
    queryset = Target.objects.filter()
    serializer_class = TargetSerializer
    filter_fields = ('target_name',)


class CompoundView(viewsets.ReadOnlyModelViewSet):
    queryset = Compounds.objects.filter()
    serializer_class = CompoundsSerializer
    filter_fields = ("smiles",)


class ReferenceView(viewsets.ReadOnlyModelViewSet):
    queryset = Reference.objects.filter()
    serializer_class = ReferenceSerializer
    filter_fields = ("reference_pdb",)


class SoakdbFilesView(viewsets.ReadOnlyModelViewSet):
    queryset = SoakdbFiles.objects.filter()
    serializer_class = SoakdbFilesSerializer
    filter_fields = ()


class CrystalView(viewsets.ReadOnlyModelViewSet):
    queryset = Crystal.objects.filter()
    serializer_class = CrystalSerializer
    filter_fields = ()


class DataProcessingView(viewsets.ReadOnlyModelViewSet):
    queryset = DataProcessing.objects.filter()
    serializer_class = DataProcessingSerializer
    filter_fields = ()


class DimpleView(viewsets.ReadOnlyModelViewSet):
    queryset = Dimple.objects.filter()
    serializer_class = DimpleSerializer
    filter_fields = ()


class LabView(viewsets.ReadOnlyModelViewSet):
    queryset = Lab.objects.filter()
    serializer_class = LabSerializer
    filter_fields = ()


class RefinementView(viewsets.ReadOnlyModelViewSet):
    queryset = Refinement.objects.filter()
    serializer_class = RefinementSerializer
    filter_fields = ()


class PanddaAnalysisView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaAnalysis.objects.filter()
    serializer_class = PanddaAnalysisSerializer
    filter_fields = ()


class PanddaRunView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaRun.objects.filter()
    serializer_class = PanddaRunSerializer
    filter_fields = ()


class PanddaSiteView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaSite.objects.filter()
    serializer_class = PanddaSiteSerializer
    filter_fields = ()


class PanddaEventView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaEvent.objects.filter()
    serializer_class = PanddaEventSerializer
    filter_fields = ()