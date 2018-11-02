import glob
import os

import datetime
from django.http import HttpResponse, JsonResponse
from django.template import loader

from functions.db_functions import check_file_status
from functions.misc_functions import get_mod_date
from xchem_db.models import Target, Crystal, Refinement, SoakdbFiles, PanddaEvent, ProasisHits, ProasisOut


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

    data = []

    for crys in crystals:
        refinements = Refinement.objects.filter(crystal_name=crys, outcome__gte=4)
        for ref in refinements:
            out_dict = {
                'crys': '',
                'refinement_status': '✘',
                'pandda_model': '✘',
                'pdb_present': '✘',
                'twofofc_present': '✘',
                'fofc_present': '✘',
                'mtz_present': '✘',
                'in_proasis': '✘',
                'proasis_strucids': '✘',
                'files_out': '✘',
                'out_directory': '✘',
                'uploaded_to_verne': '✘',
            }

            out_dict['crys'] = str(crys.crystal_name)
            out_dict['refinement_status'] = str(ref.outcome)

            events = list(set([pr.pandda_run for pr in PanddaEvent.objects.filter(crystal=crys)]))

            if events:
                out_dict['pandda_model'] = '✓'


            proasis = ProasisHits.objects.filter(crystal_name=crys).exclude(strucid=None).exclude(strucid='')
            pout = []

            bound_conf = ''

            # if there is a pdb file named in the bound_conf field, use it as the upload structure for proasis
            if ref.bound_conf:
                if os.path.isfile(ref.bound_conf):
                    bound_conf = ref.bound_conf
            # otherwise, use the most recent pdb file (according to soakdb)
            elif ref.pdb_latest:
                if os.path.isfile(ref.pdb_latest):
                    # if this is from a refinement folder, find the bound-state pdb file, rather than the ensemble
                    if 'Refine' in ref.pdb_latest:
                        search_path = '/'.join(ref.pdb_latest.split('/')[:-1])
                        print(search_path)
                        files = glob.glob(str(search_path + '/refine*split.bound*.pdb'))
                        print(files)
                        if len(files) == 1:
                            bound_conf = files[0]
                    else:
                        # if can't find bound state, just use the latest pdb file
                        bound_conf = ref.pdb_latest

            if bound_conf:
                out_dict['pdb_present'] = bound_conf.split('/')[-1]

            if 'split.bound-state' in bound_conf:
                mtz = check_file_status('refine.mtz', ref.pdb_latest)
                two_fofc = check_file_status('2fofc.map', ref.pdb_latest)
                fofc = check_file_status('fofc.map', ref.pdb_latest)

            else:
                mtz = check_file_status('refine.mtz', bound_conf)
                two_fofc = check_file_status('2fofc.map', bound_conf)
                fofc = check_file_status('fofc.map', bound_conf)


            if mtz[0]:
                out_dict['mtz_present'] = '✓'
            if two_fofc[0]:
                out_dict['twofofc_present'] = '✓'
            if fofc[0]:
                out_dict['fofc_present'] = '✓'

            if proasis:
                out_dict['in_proasis'] = '✓'
                proasis_strucids = [p.strucid for p in proasis]
                out_dict['proasis_strucids'] = ', '.join(proasis_strucids)
                for p in proasis:
                    pout = ProasisOut.objects.filter(crystal=crys, proasis=p)
                if pout:
                    out_dirs = list(set([os.path.join(o.root, o.start) for o in pout]))
                    out_dict['files_out'] = '✓'
                    out_dict['out_directory'] = ', '.join(out_dirs)
                    out_root = list(set([os.path.join(o.root, o.start.split('/')[0]) for o in pout]))

                    if len(out_root) == 1:
                        if os.path.isfile(os.path.join(out_root[0], 'verne.transferred')):
                            out_dict['uploaded_to_verne'] = '✓'

            data.append(out_dict)

    return JsonResponse(data, safe=False)







