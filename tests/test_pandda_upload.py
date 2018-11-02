import glob
import shutil
import unittest

import setup_django
setup_django.setup_django()

import functions.pandda_functions as pf
from luigi_classes.transfer_pandda import *
from .test_functions import run_luigi_worker
from xchem_db.models import PanddaRun, PanddaAnalysis


class TestFindLogs(unittest.TestCase):
    date = datetime.datetime.now()
    search_path = '/pipeline/tests/data/*'
    sdb_filepath = '/pipeline/tests/data/database/'
    db_file_name = 'soakDBDataFile.sqlite'
    db = os.path.join(sdb_filepath, db_file_name)
    findsoakdb_outfile = date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')
    find_sp_outfile = '/pipeline/tests/data/find_paths.csv'

    @classmethod
    def setUpClass(cls):
        # create log directories
        os.makedirs('/pipeline/logs/soakDBfiles')
        os.makedirs('/pipeline/logs/search_paths')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('/pipeline/logs')

    def tearDown(self):
        PanddaRun.objects.all().delete()
        PanddaAnalysis.objects.all().delete()

    # tasks: FindSoakDBFiles -> FindSearchPaths
    def test_find_search_paths(self):
        # imitate FindSoakDB task
        # emulate soakdb task
        os.system('touch ' + self.findsoakdb_outfile)

        with open(self.findsoakdb_outfile, 'w') as f:
            f.write(self.db)

        with open(self.findsoakdb_outfile, 'r') as f:
            print(f.read())

        find_paths = run_luigi_worker(FindSearchPaths(soak_db_filepath=self.sdb_filepath,
                                                      date_time=self.date.strftime("%Y%m%d%H")))

        self.assertTrue(find_paths)
        self.assertTrue(
            '0,/pipeline/tests/data/database/soakDBDataFile.sqlite,/pipeline/tests/data/,/pipeline/tests/data/database/'
            in open(FindSearchPaths(soak_db_filepath=self.sdb_filepath,
                                    date_time=self.date.strftime("%Y%m%d%H")).output().path, 'r').read())

    def test_find_pandda_info_function(self):
        search_path = '/pipeline/tests/data/'
        file_list = [x for x in pf.find_log_files(search_path).split('\n') if x != '']
        globbed = [x for x in glob.glob('/pipeline/tests/data/processing/analysis/panddas/logs/*.log') if x != '']
        self.assertTrue(sorted(file_list) == sorted(globbed))

    # requires mock outfile, as paths don't exist in test environment
    def test_get_files_from_log_function(self):
        log_file = '/pipeline/tests/data/processing/analysis/panddas/logs/pandda-2018-07-29-1940.log'
        pver, input_dir, output_dir, sites_file, events_file, Error = pf.get_files_from_log(log_file)

        self.assertEqual(pver, '0.2.12-dev')
        self.assertEqual(input_dir, '/pipeline/tests/data/processing/analysis/initial_model/*')
        self.assertEqual(output_dir, '/pipeline/tests/data/processing/analysis/panddas')
        self.assertEqual(sites_file,
                         '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_sites.csv')
        self.assertEqual(events_file,
                         '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_events.csv')
        self.assertEqual(Error, False)

    # tasks: AddPanddaRun
    def test_add_pandda_run(self):
        log_file = '/pipeline/tests/data/processing/analysis/panddas/logs/pandda-2018-07-29-1940.log'
        pver = '0.2.12-dev'
        input_dir = '/pipeline/tests/data/processing/analysis/initial_model/*'
        output_dir = '/pipeline/tests/data/processing/analysis/panddas'
        sites_file = '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_sites.csv'
        events_file = '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_events.csv'

        expected_dict = {'input_dir': input_dir,
                         # ignore this for now
                         # 'pandda_analysis': '',
                         'pandda_log': log_file,
                         'pandda_version': pver,
                         'sites_file': sites_file,
                         'events_file': events_file}

        add_run = run_luigi_worker(AddPanddaRun(log_file=log_file,
                                                pver=pver,
                                                input_dir=input_dir,
                                                output_dir=output_dir,
                                                sites_file=sites_file,
                                                events_file=events_file))

        outfile = AddPanddaRun(log_file=log_file,
                               pver=pver,
                               input_dir=input_dir,
                               output_dir=output_dir,
                               sites_file=sites_file,
                               events_file=events_file).output().path

        print(outfile)

        self.assertTrue(add_run)
        self.assertTrue(os.path.isfile(outfile))

        pandda_run_out = PanddaRun.objects.all()

        print(pandda_run_out)

        # p = pandda_run_out[0].values()
        #
        # p.pop('pandda_analysis_id', None)
        # p.pop('id', None)
        #
        # self.assertDictEqual(p, expected_dict)

        os.remove(outfile)

    # tasks: AddPanddaRun -> AddPanddaSites
    def test_add_pandda_sites(self):
        log_file = '/pipeline/tests/data/processing/analysis/panddas/logs/pandda-2018-07-29-1940.log'
        output_dir = '/pipeline/tests/data/processing/analysis/panddas'
        input_dir = '/pipeline/tests/data/processing/analysis/initial_model/*'
        pver = '0.2.12-dev'
        sites_file = '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_sites.csv'
        events_file = '/pipeline/tests/data/processing/analysis/panddas/analyses/pandda_analyse_events.csv'
        soakdb_filename = '/pipeline/tests/data/database/soakDBDataFile.sqlite'

        add_sites = run_luigi_worker(AddPanddaSites(log_file=log_file, output_dir=output_dir, input_dir=input_dir,
                                                    pver=pver, sites_file=sites_file, events_file=events_file,
                                                    soakdb_filename=soakdb_filename))

        expected_dict_list = [{'site': 1,
                               'site_aligned_centroid_x': 52.909413898078185,
                               'site_aligned_centroid_y': 20.337635571713246,
                               'site_aligned_centroid_z': 74.14146271183921,
                               'site_native_centroid_x': -15.008586101921821,
                               'site_native_centroid_y': -13.92636442828675,
                               'site_native_centroid_z': -9.229537288160785},
                              {'site': 2,
                               'site_aligned_centroid_x': 44.62633606595177,
                               'site_aligned_centroid_y': 61.57702270198425,
                               'site_aligned_centroid_z': 30.21147546335008,
                               'site_native_centroid_x': -23.291663934048238,
                               'site_native_centroid_y': 27.313022701984252,
                               'site_native_centroid_z': -53.15952453664991},
                              {'site': 3,
                               'site_aligned_centroid_x': 63.82825288518422,
                               'site_aligned_centroid_y': 39.14411041241565,
                               'site_aligned_centroid_z': 83.73403465162072,
                               'site_native_centroid_x': -4.089747114815786,
                               'site_native_centroid_y': 4.880110412415654,
                               'site_native_centroid_z': 0.36303465162072257},
                              {'site': 4,
                               'site_aligned_centroid_x': 35.824162276595246,
                               'site_aligned_centroid_y': 41.70498907010381,
                               'site_aligned_centroid_z': 21.66540760968199,
                               'site_native_centroid_x': -32.09383772340476,
                               'site_native_centroid_y': 7.440989070103811,
                               'site_native_centroid_z': -61.705592390318}
                              ]

        self.assertTrue(add_sites)

        pandda_sites = PanddaSite.objects.all()

        for site in pandda_sites:
            for e in expected_dict_list:
                if site.site == e['site']:
                    print('checking site ' + str(site.site))
                    print(site.site_aligned_centroid_x)
                    print(e['site_aligned_centroid_x'])
                    self.assertAlmostEqual(site.site_aligned_centroid_x, e['site_aligned_centroid_x'])
                    self.assertAlmostEqual(site.site_aligned_centroid_y, e['site_aligned_centroid_y'])
                    self.assertAlmostEqual(site.site_aligned_centroid_z, e['site_aligned_centroid_z'])
                    self.assertAlmostEqual(site.site_native_centroid_x, e['site_native_centroid_x'])
                    self.assertAlmostEqual(site.site_native_centroid_y, e['site_native_centroid_y'])
                    self.assertAlmostEqual(site.site_native_centroid_z, e['site_native_centroid_z'])

        os.remove('/pipeline/tests/data/processing/analysis/panddas/logs/pandda-2018-07-29-1940.log.sites.done')
        os.remove('/pipeline/tests/data/processing/analysis/panddas/logs/pandda-2018-07-29-1940.log.run.done')

    # tasks: AddPanddaRun -> AddPanddaSites -> AddPanddaEvents
    def test_add_pandda_events(self):
        pass
