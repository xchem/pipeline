from db.models import Crystal

from django.http import HttpResponse


def crystals_from_target(request, target):
    crystals = ', '.join([crystal.crystal_name for crystal in Crystal.objects.filter(target__target_name=target)])
    return HttpResponse(crystals)