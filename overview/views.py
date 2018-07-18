from db.models import Target, Crystal, Refinement

from django.http import HttpResponse, JsonResponse
from django.template import loader
from rest_framework.views import APIView
from rest_framework.response import Response


def targets(request):
    targets = Target.objects.all()
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

    data = {'number': total_crystals, 'target':str(submission), 'refinement_1': count_1}

    return JsonResponse(data)
