from django.http import HttpResponse, JsonResponse
from django.template import loader

from xchem_db.models import Target, Crystal, Refinement, SoakdbFiles, PanddaEvent, ProasisHits, ProasisOut
from functions.misc_functions import get_mod_date
import datetime
import os


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
            'x': ['Analysis pending', 'Pandda Model', 'In Refinement', 'CompChem Ready',
                  'Deposition Ready', 'Deposited'],
            'type': 'bar'}

    return JsonResponse(data)


def get_update_times(request):

    queryset = Target.objects.filter()
    filter_fields = ('target_name',)

    submission = request.GET.get('target_name')
    crystals = Crystal.objects.filter(target__target_name=submission)

    files = list(set([c.visit.filename for c in crystals]))

    formt = "%Y-%m-%d %H:%M:%S"

    mod_dates_db = [datetime.datetime.strptime(str(f.modification_date), '%Y%m%d%H%M%S').strftime(formt)
                    for f in [SoakdbFiles.objects.get(filename=fn) for fn in files]]

    real_time_mod_dates = [datetime.datetime.strptime(str(get_mod_date(f)),
                                                      '%Y%m%d%H%M%S').strftime(formt) for f in files]

    data = [{'file': f, 'db_date': dbd, 'rt_date': rtd,
             'difference': str(datetime.datetime.strptime(rtd, formt) - datetime.datetime.strptime(dbd, formt))}
            for (f, dbd, rtd) in zip(files, mod_dates_db, real_time_mod_dates)]

    return JsonResponse(data, safe=False)


def get_crystal_info(request):
    queryset = Target.objects.filter()
    filter_fields = ('target_name',)

    submission = request.GET.get('target_name')
    crystals = Crystal.objects.filter(target__target_name=submission)

    out_dict = {
        'crys': [],
        'refinement_status': [],
        'pandda_model': [],
        'in_proasis': [],
        'proasis_strucids': [],
        'files_out': [],
        'out_directory': [],
        'uploaded_to_verne': [],
    }


    for crys in crystals:
        refinements = Refinement.objects.filter(crystal_name=crys, outcome__gte=1)
        for ref in refinements:
            out_dict['crys'].append(crys.crystal_name)
            out_dict['refinement_status'].append(ref.outcome)

            events = list(set([pr.pandda_run for pr in PanddaEvent.objects.filter(crystal=crys)]))

            if events:
                out_dict['pandda_model'].append(1)
            else:
                out_dict['pandda_model'].append(0)

            proasis = ProasisHits.objects.filter(crystal_name=crys).exclude(strucid=None).exclude(strucid='')
            proasis_strucids = []
            pout = []
            uploaded = 0
            if proasis:
                out_dict['in_proasis'].append(1)
                proasis_strucids = [p.strucid for p in proasis]
                for p in proasis:
                    pout = ProasisOut.objects.filter(crystal=crys, proasis=p)
                if pout:
                    out_dirs = list(set([os.path.join(o.root, o.start) for o in pout]))
                    out_dict['files_out'].append(1)
                    out_dict['out_directory'].append(out_dirs)
                    out_root = list(set([os.path.join(o.root, o.start.split('/')[0]) for o in pout]))

                    if len(out_root) == 1:
                        if os.path.isfile(os.path.join(out_root[0], 'verne.transferred')):
                            uploaded = 1
                else:
                    out_dict['files_out'].append(0)
                    out_dict['files_out'].append([])
            else:
                out_dict['in_proasis'].append(0)
                out_dict['files_out'].append(0)
                out_dict['out_directory'].append([])
                out_dict['uploaded_to_verne'].append(0)

            out_dict['uploaded_to_verne'].append(uploaded)
            out_dict['proasis_strucids'].append(proasis_strucids)


    return JsonResponse(out_dict, safe=False)







