from django.http import HttpResponse, JsonResponse
from django.template import loader

from xchem_db.models import Target, Crystal, Refinement, SoakdbFiles
from functions.misc_functions import get_mod_date


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
    refinement_6 = Refinement.objects.filter(crystal_name__in=crystals, outcome=6)
    count_6 = len(refinement_6)

    data = {'y': [count_1, count_2, count_3, count_4, count_5, count_6],
            'x': ['Analysis pending', 'Pandda Model', 'In Refinement', 'CompChem Ready', 'Deposition Ready', 'Deposited'],
            'type': 'bar'}

    return JsonResponse(data)


def get_update_times(request):

    queryset = Target.objects.filter()
    filter_fields = ('target_name',)

    submission = request.GET.get('target_name')
    crystals = Crystal.objects.filter(target__target_name=submission)

    files = list(set([c.visit.filename for c in crystals]))
    print(files)

    mod_dates_db = [f.modification_date for f in [SoakdbFiles.objects.get(filename=fn) for fn in files]]
    print(mod_dates_db)

    real_time_mod_dates = [get_mod_date(f) for f in files]
    print(real_time_mod_dates)

    data2 = {'files': files, 'db_mod_dates': mod_dates_db, 'rt_mod_dates': real_time_mod_dates}

    return JsonResponse(data2)


