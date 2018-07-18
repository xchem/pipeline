from db.models import Target, Crystal

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
    data = {'number': len(crystals), 'target':str(submission)}

    return JsonResponse(data)
