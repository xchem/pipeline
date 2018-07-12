from .serializers import TargetSerializer, CompoundsSerializer, ReferenceSerializer
from rest_framework import viewsets
from .models import Target, Compounds, Reference, Proposals


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


