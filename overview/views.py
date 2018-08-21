from xchem_db.models import Target, Crystal, Refinement

from django.http import HttpResponse, JsonResponse
from django.template import loader
from rest_framework.views import APIView
from rest_framework.response import Response


def targets(request):
    targets = Target.objects.all().order_by('target_name')
    template = loader.get_template('overview/index.html')
    context = {
        'targets': targets,
    }
    return HttpResponse(template.render(context, request))


def get_graph(request):
    queryset = Target.objects.filter()
    filter_fields = ('target_name',)

    submission = request.GET.get('target_name', '')

    crystals = Crystal.objects.filter(target__target_name=submission)

    total_crystals = len(crystals)

    refinement_1 = Refinement.objects.filter(crystal_name__in=crystals, outcome=1)
    count_1 = len(refinement_1)
    refinement_2 = Refinement.objects.filter(crystal_name__in=crystals, outcome=2)
    count_2 = len(refinement_2)
    refinement_3 = Refinement.objects.filter(crystal_name__in=crystals, outcome=3)
    count_3 = len(refinement_3)
    refinement_4 = Refinement.objects.filter(crystal_name__in=crystals, outcome=4)
    count_4 = len(refinement_4)
    refinement_5 = Refinement.objects.filter(crystal_name__in=crystals, outcome=5)
    count_5 = len(refinement_5)

    data = {'values': [count_1, count_2, count_3, count_4, count_5],
            'labels': ['Analysis pending', 'Pandda Model', 'In Refinement', 'CompChem Ready', 'Deposition Ready'],
            'type': 'pie'}

    return JsonResponse(data)
