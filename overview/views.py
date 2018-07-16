from db.models import Target

from django.http import HttpResponse
from django.template import loader
from rest_framework.views import APIView

def targets(request):
    targets = Target.objects.all()
    template = loader.get_template('overview/index.html')
    context = {
        'targets': targets,
    }
    return HttpResponse(template.render(context, request))

class GetGraph(APIView):

    def get(self, request, format=None):
