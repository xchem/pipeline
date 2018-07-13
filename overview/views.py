from db.models import Crystal

from django.http import HttpResponse
from django.template import loader

def crystals_from_target(request, target):
    crystals = Crystal.objects.filter(target__target_name=target)
    template = loader.get_template('overview/index.html')
    context = {
        'crystals': crystals,
    }
    return HttpResponse(template.render(context, request))