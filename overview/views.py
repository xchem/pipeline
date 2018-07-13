from db.models import Crystal

from django.http import HttpResponse
from django.template import loader

def crystals_from_target(request, target):
    crystals = ', '.join([crystal.crystal_name for crystal in Crystal.objects.filter(target__target_name=target)])
    template = loader.get_template('templates/overview/index.html')
    context = {
        'crystals': crystals,
    }
    return HttpResponse(template.render(context, request))