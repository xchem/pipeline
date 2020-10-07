import sqlite3
import traceback
import pandas as pd
from django.db import IntegrityError

from functions import pandda_functions

from xchem_db.models import *


def find_pandda_logs(search_path):
    print('RUNNING')
    # If statement checking if log files have been found
    log_files = pandda_functions.find_log_files(search_path)
    return log_files


def add_pandda_sites(log_file, sites_file):
    run = PanddaRun.objects.get(pandda_log=str(log_file).rstrip())
    sites_frame = pd.DataFrame.from_csv(sites_file, index_col=None)

    for i in range(0, len(sites_frame['site_idx'])):
        site = sites_frame['siteidx'][i]
        aligned_centroid = eval(sites_frame['centroid'][i])
        native_centroid = eval(sites_frame['native_centroid'][i])

        print(f'Adding pandda site: {site}')

        try:
            pandda_site = PanddaSite.objects.get_or_create(pandda_run=run, site=site,
                                                           site_aligned_centroid_x=aligned_centroid[0],
                                                           site_aligned_centroid_y=aligned_centroid[1],
                                                           site_aligned_centroid_z=aligned_centroid[2],
                                                           site_native_centroid_x=native_centroid[0],
                                                           site_native_centroid_y=native_centroid[1],
                                                           site_native_centroid_z=native_centroid[2]
                                                           )[0]
        except IntegrityError:
            pandda_site = PanddaSite.objects.get(pandda_run=run, site=site)
            pandda_site.site_aligned_centroid_x = aligned_centroid[0]
            pandda_site.site_aligned_centroid_y = aligned_centroid[1]
            pandda_site.site_aligned_centroid_z = aligned_centroid[2]
            pandda_site.site_native_centroid_x = native_centroid[0]
            pandda_site.site_native_centroid_y = native_centroid[1]
            pandda_site.site_native_centroid_z = native_centroid[2]

        pandda_site.save()

    return ''


def add_pandda_events(events_file, log_file, sdbfile):
    events_frame = pd.DataFrame.from_csv(events_file, index_col=None)

    error_file = f'{log_file}.transfer.err'

    for i in range(0, len(events_frame['dtag'])):
        event_site = (events_frame['site_idx'][i])

        run = PanddaRun.objects.get(pandda_log=log_file)
        site = PanddaSite.objects.get_or_create(site=int(event_site), pandda_run=run)[0]

        input_directory = run.input_dir

        output_directory = run.pandda_analysis.pandda_dir

        map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path, \
            exists_array = pandda_functions.get_file_names(
                bdc=events_frame['1-BDC'][i],
                crystal=events_frame['dtag'][i],
                input_dir=input_directory,
                output_dir=output_directory,
                event=events_frame['event_idx'][i]
            )

        if False not in exists_array:

            lig_strings = pandda_functions.find_ligands(pandda_model_path)

            try:
                event_ligand, event_ligand_centroid, event_lig_dist, site_event_dist = \
                    pandda_functions.find_ligand_site_event(
                        ex=events_frame['x'][i],
                        ey=events_frame['y'][i],
                        ez=events_frame['z'][i],
                        nx=site.site_native_centroid_x,
                        ny=site.site_native_centroid_y,
                        nz=site.site_native_centroid_z,
                        lig_strings=lig_strings,
                        pandda_model_path=pandda_model_path
                    )
                crystal = Crystal.objects.get_or_create(crystal_name=events_frame['dtag'][i],
                                                        visit=SoakdbFiles.objects.get_or_create(
                                                            filename=sdbfile)[0]
                                                        )[0]

                pandda_event = PanddaEvent.objects.get_or_create(
                    crystal=crystal,
                    site=site,
                    refinement=Refinement.objects.get_or_create(crystal_name=crystal)[0],
                    data_proc=DataProcessing.objects.get_or_create(crystal_name=crystal)[0],
                    pandda_run=run,
                    event=events_frame['event_idx'][i],
                    event_centroid_x=events_frame['x'][i],
                    event_centroid_y=events_frame['y'][i],
                    event_centroid_z=events_frame['z'][i],
                    event_dist_from_site_centroid=site_event_dist,
                    lig_centroid_x=event_ligand_centroid[0],
                    lig_centroid_y=event_ligand_centroid[1],
                    lig_centroid_z=event_ligand_centroid[2],
                    lig_dist_event=event_lig_dist,
                    lig_id=event_ligand,
                    pandda_event_map_native=map_file_path,
                    pandda_model_pdb=pandda_model_path,
                    pandda_input_pdb=input_pdb_path,
                    pandda_input_mtz=input_mtz_path)[0]
                pandda_event.save()

                event_stats_dict = pandda_functions.translate_event_stats(events_file, i)
                event_stats_dict['event'] = pandda_event

                pandda_event_stats = PanddaEventStats.objects.get_or_create(**event_stats_dict)[0]

                pandda_event_stats.save()

                crystal.status = Crystal.PANDDA
                crystal.save()

            except Exception as exc:
                print(traceback.format_exc())
                print(exc)
        else:
            with open(error_file, 'a') as f:
                f.write('CRYSTAL: ' + str(events_frame['dtag'][i]) + ' SITE: ' + str(event_site) +
                        ' EVENT: ' + str(events_frame['event_idx'][i]) + '\n')
                print('FILES NOT FOUND FOR EVENT: ' + str(events_frame['event_idx'][i]))
                f.write('FILES NOT FOUND FOR EVENT: ' + str(events_frame['event_idx'][i]) + '\n')
                print('EXPECTED: ')
                f.write('EXPECTED: ' + '\n')
                print(str([map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path]))
                f.write(str([map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path])
                        + '\n')
                print(exists_array)
                f.write(str(exists_array) + '\n')
                f.write('\n\n')

    return ''


def add_pandda_run(log_file, input_dir, output_dir, pver, sites_file, events_file):
    print('ADDING PANDDA RUN...')
    pandda_run = \
        PanddaRun.objects.get_or_create(pandda_log=log_file, input_dir=input_dir,
                                        pandda_analysis=PanddaAnalysis.objects.get_or_create(
                                            pandda_dir=output_dir)[0],
                                        pandda_version=pver, sites_file=sites_file,
                                        events_file=events_file)[0]
    pandda_run.save()
    return ''


def find_pandda_info(inputs, output, sdbfile):
    # inputs should be self.input()?
    # output should be what is returned from self.output()?
    # sdbfile : self.sbdfile

    # read the list of log files
    with inputs.open('r') as f:
        log_files = [logfile.rstrip() for logfile in f.readlines()]

    out_dict = {
        'log_file': [],
        'pver': [],
        'input_dir': [],
        'output_dir': [],
        'sites_file': [],
        'events_file': [],
        'sdbfile': []
    }

    for log_file in log_files:

        # read information from the log file
        pver, input_dir, output_dir, sites_file, events_file, err = pandda_functions.get_files_from_log(log_file)
        if not err and sites_file and events_file and '0.1.' not in pver:
            # if no error, and sites and events present, add events from events file
            # yield AddPanddaEvents(
            out_dict['log_file'].append(log_file)
            out_dict['pver'].append(pver)
            out_dict['input_dir'].append(input_dir)
            out_dict['output_dir'].append(output_dir)
            out_dict['sites_file'].append(sites_file)
            out_dict['events_file'].append(events_file)
            out_dict['sdbfile'].append(sdbfile)

        else:
            print(pver)
            print(input_dir)
            print(output_dir)
            print(sites_file)
            print(events_file)
            print(err)

    frame = pd.DataFrame.from_dict(out_dict)

    frame.to_csv(output.path)

    return ''


def add_pandda_data():
    # Do nothing?
    return ''


def find_search_paths(inputs, output, soak_db_filepath):
    # inputs: self.input()
    # output: self.output()
    # soak_db_filepath : self.soak_db_filepath
    with inputs.open('r') as f:
        paths = [datafile.rstrip() for datafile in f.readlines()]

    search_paths = []
    soak_db_files = []
    for path in paths:
        if 'database' not in path:
            continue
        else:
            search_path = path.split('database')
        if len(search_path) > 1:
            search_paths.append(search_path[0])
            soak_db_files.append(str('database' + search_path[1]))

    zipped = list(zip(search_paths, soak_db_files))

    to_exclude = []

    for path in list(set(search_paths)):
        count = search_paths.count(path)
        if count > 1:

            while path in search_paths:
                search_paths.remove(path)

            to_exclude.append(path)

    out_dict = {'search_path': [], 'soak_db_filepath': [], 'sdbfile': []}

    for path, sdbfile in zipped:

        if path in to_exclude:
            continue

        else:

            print(path)
            print(sdbfile)
            print(os.path.join(path, sdbfile))

            out_dict['search_path'].append(path)
            out_dict['soak_db_filepath'].append(soak_db_filepath)
            out_dict['sdbfile'].append(os.path.join(path, sdbfile))

    frame = pd.DataFrame.from_dict(out_dict)

    print(output.path)

    frame.to_csv(output.path)
    return ''


def transfer_pandda():
    return ''


def annotate_events(soakdb_filename):
    events = PanddaEvent.objects.filter(crystal__visit__filename=soakdb_filename)
    conn = sqlite3.connect(soakdb_filename)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    for e in events:

        c.execute(
            "select PANDDA_site_confidence, PANDDA_site_InspectConfidence from panddaTable where "
            "CrystalName = ? and PANDDA_site_index = ? and PANDDA_site_event_index = ?",
            (e.crystal.crystal_name, e.site.site, e.event)
        )

        results = c.fetchall()

        if len(results) == 1:
            e.ligand_confidence_inspect = results[0]['PANDDA_site_InspectConfidence']
            e.ligand_confidence = results[0]['PANDDA_site_confidence']
            e.ligand_confidence_source = 'SD'
            e.save()

        elif len(results) > 1:
            raise Exception('too many events found in soakdb!')

    return ''


def annotate_all_events():
    return ''
