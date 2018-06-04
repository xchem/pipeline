import luigi
import os
import setup_django
import subprocess
import datetime
from functions import db_functions, misc_functions, pandda_functions
from db.models import *
import pandas as pd
import traceback


class FindSoakDBFiles(luigi.Task):
    # date parameter - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())

    # filepath parameter can be changed elsewhere
    filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):

        # maybe change to *.sqlite to find renamed files? - this will probably pick up a tonne of backups
        command = str('''find ''' + self.filepath +  ''' -maxdepth 5 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -path "*TeXRank*" -prune -o -name "soakDBDataFile.sqlite" -print''')
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print(command)

        # run process to find sqlite files
        out, err = process.communicate()
        out = out.decode('ascii')
        print('OUTPUT:')
        print(out)

        # write filepaths to file as output
        with self.output().open('w') as f:
            f.write(str(out))


class CheckFiles(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        print('Finding soakdb files via CheckFiles')
        soakdb = list(SoakdbFiles.objects.all())

        if not soakdb:
            return [TransferAllFedIDsAndDatafiles(soak_db_filepath=self.soak_db_filepath),
                    FindSoakDBFiles(filepath=self.soak_db_filepath)]
        else:
            return [FindSoakDBFiles(filepath=self.soak_db_filepath), FindSoakDBFiles(filepath=self.soak_db_filepath)]

    def output(self):
        return luigi.LocalTarget('logs/checked_files/files_' + str(self.date) + '.checked')

    def run(self):
        soakdb = SoakdbFiles.objects.all()
        print('SOAKDB:')
        for item in soakdb:
            print(item)

        # a list to hold filenames that have been checked
        checked = []

        # Status codes:-
        # 0 = new
        # 1 = changed
        # 2 = not changed

        print('INPUT NAME:')
        print(self.input()[1].path)

        with open(self.input()[1].path, 'r') as f:
            files = f.readlines()
            print('FILES:')
            print(files)

        for filename in files:
            # remove any newline characters
            filename_clean = filename.rstrip('\n')
            # find the relevant entry in the soakdbfiles table
            soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))

            print(len(soakdb_query))

            # raise an exception if the file is not in the soakdb table
            if len(soakdb_query) == 0:
                raise Exception(str('No entry found for ' + str(filename_clean)))

            # only one entry should exist per file
            if len(soakdb_query) == 1:
                # get the filename back from the query
                data_file = soakdb_query[0].filename
                # add the file to the list of those that have been checked
                checked.append(data_file)
                # get the modification date as stored in the db
                old_mod_date = soakdb_query[0].modification_date
                # get the current modification date of the file
                current_mod_date = misc_functions.get_mod_date(data_file)
                # get the id of the entry to write to
                id_number = soakdb_query[0].id

                print(old_mod_date)
                print(current_mod_date)

                # if the file has changed since the db was last updated for the entry, change status to indicate this
                if int(current_mod_date) > int(old_mod_date):
                    update_status = SoakdbFiles.objects.get(id=id_number)
                    update_status.status = 1
                    update_status.save()

                else:
                    update_status = SoakdbFiles.objects.get(id=id_number)
                    update_status.status = 0
                    update_status.save()

            # if there is more than one entry, raise an exception (should never happen - filename field is unique)
            if len(soakdb_query) > 1:
                raise Exception('More than one entry for file! Something has gone wrong!')

            # if the file is not in the database at all
            if filename_clean not in checked:
                # add the file to soakdb
                out, err, proposal = db_functions.pop_soakdb(filename_clean)
                # add the proposal to proposal
                db_functions.pop_proposals(proposal)
                # retrieve the new db entry
                soakdb_query = list(SoakdbFiles.objects.filter(filename=filename_clean))
                # get the id to update
                id_number = soakdb_query[0].id
                # update the relevant status to 0, indicating it as a new file
                update_status = SoakdbFiles.objects.get(id=id_number)
                update_status.status = 0
                update_status.save()


        # if the lab table is empty, no data has been transferred from the datafiles, so set status of everything to 0
        lab = list(Lab.objects.all())
        if not lab:
            # this is to set all file statuses to 0 (new file)
            soakdb = SoakdbFiles.objects.all()
            soakdb.status = 0
            soakdb.update()

        # write output to signify job done
        with self.output().open('w') as f:
            f.write('')


class TransferAllFedIDsAndDatafiles(luigi.Task):
    # date parameter for daily run - needs to be changed
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    # needs a list of soakDB files from the same day
    def requires(self):
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    # output is just a log file
    def output(self):
        return luigi.LocalTarget(self.date.strftime('logs/transfer_logs/fedids_%Y%m%d.txt'))

    # transfers data to a central postgres db
    def run(self):

        # use list from previous step as input to write to postgres
        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')

                # populate the soakdb table for each db file found by FindSoakDBFiles
                out, err, proposal = db_functions.pop_soakdb(database_file)

        # return a list of all proposals from db
        proposal_list = list(SoakdbFiles.objects.values_list('proposal', flat=True))

        # add fedid permissions via proposals table
        for proposal_number in set(proposal_list):
            db_functions.pop_proposals(proposal_number)

        # write output to show job done
        with self.output().open('w') as f:
            f.write('TransferFeDIDs DONE')


class TransferChangedDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")
    # no longer need this - assigned by django
    # file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles(soak_db_filepath=self.data_file)

    def output(self):
        pass

    def run(self):
        # delete all fields from soakdb filename
        soakdb_query = SoakdbFiles.objects.get(filename=self.data_file)
        soakdb_query.delete()

        db_functions.pop_soakdb(self.data_file)

        db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=self.data_file,
                                    model=Crystal)
        db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=self.data_file,
                                    model=Lab)
        db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=self.data_file,
                                    model=Refinement)
        db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=self.data_file,
                                    model=Dimple)
        db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                    filename=self.data_file, model=DataProcessing)

        # retrieve the new db entry

        soakdb_query = list(SoakdbFiles.objects.filter(filename=self.data_file))
        # get the id to update
        id_number = soakdb_query[0].id
        # update the relevant status to 0, indicating it as a new file
        update_status = SoakdbFiles.objects.get(id=id_number)
        update_status.status = 2
        update_status.save()


class TransferNewDataFile(luigi.Task):
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    # no longer need this - assigned by django
    # file_id = luigi.Parameter()

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def run(self):
        db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), filename=self.data_file,
                                    model=Crystal)
        db_functions.transfer_table(translate_dict=db_functions.lab_translations(), filename=self.data_file,
                                    model=Lab)
        db_functions.transfer_table(translate_dict=db_functions.refinement_translations(), filename=self.data_file,
                                    model=Refinement)
        db_functions.transfer_table(translate_dict=db_functions.dimple_translations(), filename=self.data_file,
                                    model=Dimple)
        db_functions.transfer_table(translate_dict=db_functions.data_processing_translations(),
                                    filename=self.data_file, model=DataProcessing)

        # retrieve the new db entry
        soakdb_query = list(SoakdbFiles.objects.filter(filename=self.data_file))
        # get the id to update
        id_number = soakdb_query[0].id
        # update the relevant status to 0, indicating it as a new file
        update_status = SoakdbFiles.objects.get(id=id_number)
        update_status.status = 2
        update_status.save()


class StartTransfers(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now().strftime("%Y%m%d%H"))
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def get_file_list(self, status_code):

        status_query = SoakdbFiles.objects.filter(status=status_code)
        datafiles = [object.filename for object in status_query]

        return datafiles

    def requires(self):
        return CheckFiles(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget('logs/transfer_logs/transfers_' + str(self.date) + '.done')

    def run(self):
        new_list = self.get_file_list(0)
        changed_list = self.get_file_list(1)
        yield [TransferNewDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                for datafile in new_list], \
               [TransferChangedDataFile(data_file=datafile, soak_db_filepath=self.soak_db_filepath)
                for datafile in changed_list]

        with self.output().open('w') as f:
            f.write('')


class FindProjects(luigi.Task):

    def requires(self):
        pass
        # return CheckFiles(), StartTransfers()

    def output(self):
        return luigi.LocalTarget('logs/findprojects.done')

    def run(self):
        # all data necessary for uploading hits
        hits_dict = {'crystal_name': [], 'protein': [], 'smiles': [], 'bound_conf': [],
                                  'modification_date': [], 'strucid':[]}

        # all data necessary for uploading leads
        leads_dict = {'protein': [], 'pandda_path': [], 'reference_pdb': [], 'strucid':[]}

        # class ProasisHits(models.Model):
        #     bound_pdb = models.ForeignKey(Refinement, to_field='bound_conf', on_delete=models.CASCADE, unique=True)
        #     crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
        #     modification_date = models.TextField(blank=True, null=True)
        #     strucid = models.TextField(blank=True, null=True)
        #     ligand_list = models.IntegerField(blank=True, null=True)

        ref_or_above = Refinement.objects.filter(outcome__in=[3, 4, 5, 6])

        for entry in ref_or_above:
            print(entry.crystal_name.crystal_name)
            if entry.bound_conf:
                print(entry.bound_conf)
            else:
                print(entry.pdb_latest)


        # lab_table_select = Lab.objects.filter(crystal_name=ref_or_above.crystal_name)
        #
        # for row in rows:
        #
        #     c.execute('''SELECT smiles, protein FROM lab WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     lab_table = c.fetchall()
        #
        #     if len(str(row[0])) < 3:
        #         continue
        #
        #     if len(lab_table) > 1:
        #         print(('WARNING: ' + str(row[0]) + ' has multiple entries in the lab table'))
        #         # print lab_table
        #
        #     for entry in lab_table:
        #         if len(str(entry[1])) < 2 or 'None' in str(entry[1]):
        #             protein_name = str(row[0]).split('-')[0]
        #         else:
        #             protein_name = str(entry[1])
        #
        #
        #         crystal_data_dump_dict['protein'].append(protein_name)
        #         crystal_data_dump_dict['smiles'].append(entry[0])
        #         crystal_data_dump_dict['crystal_name'].append(row[0])
        #         crystal_data_dump_dict['bound_conf'].append(row[1])
        #         crystal_data_dump_dict['strucid'].append('')
        #
        #         try:
        #             modification_date = misc_functions.get_mod_date(str(row[1]))
        #
        #         except:
        #             modification_date = ''
        #
        #         crystal_data_dump_dict['modification_date'].append(modification_date)
        #
        #     c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     pandda_info = c.fetchall()
        #
        #     for pandda_entry in pandda_info:
        #         project_data_dump_dict['protein'].append(protein_name)
        #         project_data_dump_dict['pandda_path'].append(pandda_entry[0])
        #         project_data_dump_dict['reference_pdb'].append(pandda_entry[1])
        #         project_data_dump_dict['strucid'].append('')
        #
        # project_table = pandas.DataFrame.from_dict(project_data_dump_dict)
        # crystal_table = pandas.DataFrame.from_dict(crystal_data_dump_dict)
        #
        # protein_list = set(list(project_data_dump_dict['protein']))
        # print(protein_list)
        #
        # for protein in protein_list:
        #
        #     self.add_to_postgres(project_table, protein, ['reference_pdb'], project_data_dump_dict, 'proasis_leads')
        #
        #     self.add_to_postgres(crystal_table, protein, ['crystal_name', 'smiles', 'bound_conf'],
        #                          crystal_data_dump_dict, 'proasis_hits')
        #
        # with self.output().open('wb') as f:
        #     f.write('')


class FindPanddaLogs(luigi.Task):
    search_path = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return StartTransfers(soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.search_path, self.date.strftime('pandda_logs_%Y%m%d.txt')))

    def run(self):
        print('RUNNING')
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

    def requires(self):
        return AddPanddaRun(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir, pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file)

    def output(self):
        return luigi.LocalTarget(str(self.log_file + '.sites.done'))

    def run(self):
        run = PanddaRun.objects.get(pandda_log=self.log_file)
        sites_frame = pd.DataFrame.from_csv(self.sites_file, index_col=None)

        for i in range(0, len(sites_frame['site_idx'])):
            site = sites_frame['site_idx'][i]
            aligned_centroid = eval(sites_frame['centroid'][i])
            native_centroid = eval(sites_frame['native_centroid'][i])

            print('Adding pandda site: ' + str(site))

            pandda_site = PanddaSite.objects.get_or_create(run=run, site=site,
                                                           site_aligned_centroid_x=aligned_centroid[0],
                                                           site_aligned_centroid_y=aligned_centroid[1],
                                                           site_aligned_centroid_z=aligned_centroid[2],
                                                           site_native_centroid_x=native_centroid[0],
                                                           site_native_centroid_y=native_centroid[1],
                                                           site_native_centroid_z=native_centroid[2])[0]
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

    def requires(self):
        return AddPanddaRun(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir, pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file), \
               AddPanddaSites(log_file=self.log_file, output_dir=self.output_dir, input_dir=self.input_dir, pver=self.pver,
                            sites_file=self.sites_file, events_file=self.events_file)

    def output(self):
        return luigi.LocalTarget(str(self.log_file + '.events.done'))

    def run(self):

        events_frame = pd.DataFrame.from_csv(self.events_file, index_col=None)

        error_file = str(self.log_file + '.transfer.err')

        for i in range(0, len(events_frame['dtag'])):
            event_site = (events_frame['site_idx'][i])

            run = PanddaRun.objects.get(pandda_log=self.log_file)
            site = PanddaSite.objects.get(site=int(event_site), run=run.pk)

            input_directory = run.input_dir

            output_directory = run.analysis_folder.pandda_dir

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

                    crystal = Crystal.objects.get(crystal_name=events_frame['dtag'][i])

                    pandda_event = PanddaEvent.objects.get_or_create(
                        crystal=crystal,
                        site=site,
                        run=run,
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
                    f.write('CRYSTAL: ' + str(events_frame['dtag'][i]) +' SITE: ' + str(event_site) +
                            ' EVENT: ' + str(events_frame['event_idx'][i]) + '\n')
                    print('FILES NOT FOUND FOR EVENT: ' + str(events_frame['event_idx'][i]))
                    f.write('FILES NOT FOUND FOR EVENT: ' + str(events_frame['event_idx'][i]) + '\n')
                    print('EXPECTED: ')
                    f.write('EXPECTED: ' + '\n')
                    print(str([map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path]))
                    f.write(str([map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path])
                            + '\n')
                    print(exists_array)
                    f.write(str(exists_array)+ '\n')
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

    def complete(self):
        if PanddaRun.objects.filter(pandda_log=self.log_file).exists():
            return True
        else:
            return False

    def run(self):
        print('ADDING PANDDA RUN...')
        pandda_run = PanddaRun.objects.get_or_create(pandda_log=self.log_file, input_dir=self.input_dir,
                                                     analysis_folder=PanddaAnalysis.objects.get_or_create(
                                                         pandda_dir=self.output_dir)[0],
                                                     pandda_version=self.pver, sites_file=self.sites_file,
                                                     events_file=self.events_file)[0]
        pandda_run.save()


class AddPanddaTables(luigi.Task):
    search_path = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return FindPanddaLogs(search_path=self.search_path, soak_db_filepath=self.soak_db_filepath)

    def output(self):
        return luigi.LocalTarget(str(self.input().path + '.tables.done'))

    def run(self):
        # read the list of log files
        with self.input().open('r') as f:
            log_files = [logfile.rstrip() for logfile in f.readlines()]

        # dict to hold info for subsequent things
        # results_dict = {'log_file': [], 'pver': [], 'input_dir': [], 'output_dir': [], 'sites_file': [],
        #                 'events_file': [], 'err': []}

        for log_file in log_files:
            # results_dict['log_file'].append(file)
            # read information from the log file
            pver, input_dir, output_dir, sites_file, events_file, err = pandda_functions.get_files_from_log(log_file)
            if not err and sites_file and events_file and '0.1.' not in pver:
                yield AddPanddaEvents(log_file=log_file, pver=pver, input_dir=input_dir, output_dir=output_dir,
                                   sites_file=sites_file, events_file=events_file)

        with self.output().open('w') as f:
            f.write('')


class FindSearchPaths(luigi.Task):
    soak_db_filepath = luigi.Parameter(default="/dls/labxchem/data/*/lb*/*")

    def requires(self):
        return FindSoakDBFiles(filepath=self.soak_db_filepath)

    def output(self):
        pass

    def run(self):
        with self.input().open('r') as f:
            paths = [datafile.rstrip() for datafile in f.readlines()]

        search_paths=[]
        soak_db_files=[]

        for path in paths:
            search_path = path.split('database')
            if len(search_path)>1:
                if search_path[0] in search_paths:
                    print('already an entry for this...')
                search_paths.append(search_path[0])
                soak_db_files.append(str('database/' + search_path[1]))

        if len(set(search_paths))==len(search_paths):
            print('HOORAY!')

        print(search_paths)

        for path in search_paths:
            print(path)
            yield AddPanddaTables(search_path=path, soak_db_filepath=self.soak_db_filepath)






