import luigi
import os
import setup_django
import datetime
from functions import pandda_functions
from xchem_db.models import *
import pandas as pd
import traceback
from django.db import transaction
from luigi_classes.transfer_soakdb import StartTransfers, FindSoakDBFiles
import setup_django


class FindPanddaLogs(luigi.Task):
    search_path = luigi.Parameter()
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return StartTransfers(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.search_path, str(self.date_time) + '.txt'))

    def run(self):
        print('RUNNING')
        if os.path.isfile(self.output().path.replace(str(self.date_time), str(int(str(self.date_time)) - 1))):
            os.remove(self.output().path.replace(str(self.date_time), str(int(str(self.date_time)) - 1)))
        log_files = pandda_functions.find_log_files(self.search_path)
        with self.output().open('w') as f:
            f.write(log_files)


class AddPanddaSites(luigi.Task):
    log_file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()
    soakdb_filename = luigi.Parameter()

    def requires(self):
        return AddPanddaRun(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir,
                            pver=self.pver, sites_file=self.sites_file, events_file=self.events_file)

    def output(self):
        return luigi.LocalTarget(str(self.log_file + '.sites.done'))

    @transaction.atomic
    def run(self):
        run = PanddaRun.objects.get(pandda_log=self.log_file)
        sites_frame = pd.DataFrame.from_csv(self.sites_file, index_col=None)

        for i in range(0, len(sites_frame['site_idx'])):
            site = sites_frame['site_idx'][i]
            aligned_centroid = eval(sites_frame['centroid'][i])
            native_centroid = eval(sites_frame['native_centroid'][i])

            print('Adding pandda site: ' + str(site))

            pandda_site = PanddaSite.objects.get_or_create(pandda_run=run, site=site,
                                                           site_aligned_centroid_x=aligned_centroid[
                                                               0],
                                                           site_aligned_centroid_y=aligned_centroid[
                                                               1],
                                                           site_aligned_centroid_z=aligned_centroid[
                                                               2],
                                                           site_native_centroid_x=native_centroid[
                                                               0],
                                                           site_native_centroid_y=native_centroid[
                                                               1],
                                                           site_native_centroid_z=native_centroid[
                                                               2])[0]
            pandda_site.save()

        with self.output().open('w') as f:
            f.write('')


class AddPanddaEvents(luigi.Task):
    log_file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()
    sdbfile = luigi.Parameter()

    def requires(self):
        return AddPanddaRun(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir,
                            pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file), \
               AddPanddaSites(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir,
                              pver=self.pver,
                              sites_file=self.sites_file, events_file=self.events_file, soakdb_filename=self.sdbfile)

    def output(self):
        return luigi.LocalTarget(str(self.log_file + '.events.done'))

    def run(self):

        events_frame = pd.DataFrame.from_csv(self.events_file, index_col=None)

        error_file = str(self.log_file + '.transfer.err')

        for i in range(0, len(events_frame['dtag'])):
            event_site = (events_frame['site_idx'][i])

            run = PanddaRun.objects.get(pandda_log=self.log_file)
            site = PanddaSite.objects.get_or_create(site=int(event_site), pandda_run=run)[0]

            input_directory = run.input_dir

            output_directory = run.pandda_analysis.pandda_dir

            map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, \
            pandda_model_path, exists_array = pandda_functions.get_file_names(BDC=events_frame['1-BDC'][i],
                                                                              crystal=events_frame['dtag'][i],
                                                                              input_dir=input_directory,
                                                                              output_dir=output_directory,
                                                                              event=events_frame['event_idx'][i])

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
                                                                filename=self.sdbfile)[0]
                                                            )[0]

                    pandda_event = PanddaEvent.objects.get_or_create(
                        crystal=crystal,
                        site=site,
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
                        pandda_input_mtz=input_mtz_path

                    )[0]

                    pandda_event.save()

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

        with self.output().open('w') as f:
            f.write('')


class AddPanddaRun(luigi.Task):
    log_file = luigi.Parameter()
    output_dir = luigi.Parameter()
    input_dir = luigi.Parameter()
    pver = luigi.Parameter()
    sites_file = luigi.Parameter()
    events_file = luigi.Parameter()

    def requires(self):
        pass

    # def complete(self):
    #     if PanddaRun.objects.filter(pandda_log=self.log_file).exists():
    #         return True
    #     else:
    #         return False

    def output(self):
        return luigi.LocalTarget(str(self.log_file + '.run.done'))

    @transaction.atomic
    def run(self):
        print('ADDING PANDDA RUN...')
        pandda_run = \
            PanddaRun.objects.get_or_create(pandda_log=self.log_file, input_dir=self.input_dir,
                                            pandda_analysis=PanddaAnalysis.objects.get_or_create(
                                                pandda_dir=self.output_dir)[0],
                                            pandda_version=self.pver, sites_file=self.sites_file,
                                            events_file=self.events_file)[0]
        pandda_run.save()

        with self.output().open('w') as f:
            f.write('')


class FindPanddaInfo(luigi.Task):
    search_path = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    sdbfile = luigi.Parameter()

    def requires(self):
        return FindPanddaLogs(search_path=self.search_path, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(str(self.input().path + '.info.csv'))

    def run(self):
        # read the list of log files
        with self.input().open('r') as f:
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
                out_dict['sdbfile'].append(self.sdbfile)

            else:
                print(pver)
                print(input_dir)
                print(output_dir)
                print(sites_file)
                print(events_file)
                print(err)

        frame = pd.DataFrame.from_dict(out_dict)

        frame.to_csv(self.output().path)

        # with self.output().open('w') as f:
        #     f.write('')


class AddPanddaData(luigi.Task):
    search_path = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    sdbfile = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.search_path, 'transfer_pandda_data.done'))

    def requires(self):
        if os.path.isfile(self.output().path):
            os.remove(self.output().path)

        if not os.path.isfile(FindPanddaInfo(search_path=self.search_path, soak_db_filepath=self.soak_db_filepath,
                                             sdbfile=self.sdbfile).output().path):
            return FindPanddaInfo(search_path=self.search_path, soak_db_filepath=self.soak_db_filepath,
                                  sdbfile=self.sdbfile)
        else:
            frame = pd.DataFrame.from_csv(
                FindPanddaInfo(search_path=self.search_path, soak_db_filepath=self.soak_db_filepath,
                               sdbfile=self.sdbfile).output().path)

            return [AddPanddaEvents(log_file=log_file, pver=pver, input_dir=input_dir, output_dir=output_dir,
                                    sites_file=sites_file, events_file=events_file, sdbfile=sdbfile) for
                    log_file, pver, input_dir, output_dir, sites_file, events_file, sdbfile in
                    list(zip(
                        frame['log_file'], frame['pver'], frame['input_dir'], frame['output_dir'], frame['sites_file'],
                        frame['events_file'], frame['sdbfile']
                    ))]

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class FindSearchPaths(luigi.Task):
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(str('logs/search_paths/search_paths_' + str(self.date_time) + '.csv'))

    def run(self):
        with self.input().open('r') as f:
            paths = [datafile.rstrip() for datafile in f.readlines()]

        search_paths = []
        soak_db_files = []

        for path in paths:
            search_path = path.split('database')
            if len(search_path) > 1:
                search_paths.append(search_path[0])
                soak_db_files.append(str('database' + search_path[1]))

        zipped = list(zip(search_paths, soak_db_files))

        to_exclude = []

        for path in list(set(search_paths)):
            count = search_paths.count(path)
            if count > 1:
                # print(path)
                # print([i for (x, i) in zipped if x == path])

                while path in search_paths:
                    search_paths.remove(path)

                to_exclude.append(path)

        out_dict = {'search_path': [], 'soak_db_filepath': [], 'sdbfile': []}

        for path, sdbfile in zipped:
            print(path)
            print(sdbfile)
            print(os.path.join(path, sdbfile))
            # yield AddPanddaTables(
            out_dict['search_path'].append(path)
            out_dict['soak_db_filepath'].append(self.soak_db_filepath)
            out_dict['sdbfile'].append(os.path.join(path, sdbfile))

        frame = pd.DataFrame.from_dict(out_dict)

        print(self.output().path)

        frame.to_csv(self.output().path)

        if to_exclude:
            raise Exception('Multiple soakdb files were found in the following paths, and these will not'
                            ' be included in data upload, as it is impossible to link data back to the correct'
                            ' soakdbfiles when there are multiple per project:\n' + ', '.join(to_exclude))


class TransferPandda(luigi.Task):
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    date_time = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))

    def requires(self):
        in_file = FindSearchPaths(soak_db_filepath=self.soak_db_filepath, date_time=self.date_time).output().path
        print(in_file)
        if not os.path.isfile(in_file):
            return FindSearchPaths(soak_db_filepath=self.soak_db_filepath, date_time=self.date_time)
        else:
            frame = pd.DataFrame.from_csv(in_file)
            return [AddPanddaData(search_path=search_path, soak_db_filepath=filepath, sdbfile=sdbfile) for
                    search_path, filepath, sdbfile in list(
                    zip(frame['search_path'], frame['soak_db_filepath'], frame['sdbfile']))]

    def output(self):
        return luigi.LocalTarget(str('logs/search_paths/search_paths_' + str(self.date_time) + '_transferred.txt'))

    def run(self):
        with self.output().open('w') as f:
            f.write('')