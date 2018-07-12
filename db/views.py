from django.shortcuts import render
from .serializers import TargetSerializer
from rest_framework import viewsets
from .models import Target


class TargetView(viewsets.ReadOnlyModelViewSet):
    queryset = Target.objects.filter()
    serializer_class = TargetSerializer
    filter_fields = ('target_name')

class CompoundView(viewsets.ReadOnlyModelViewSet)