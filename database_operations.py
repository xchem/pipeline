import luigi
import psycopg2
import sqlite3
import subprocess
import sys
import os
import datetime
import pandas
from sqlalchemy import create_engine


class FindSoakDBFiles(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.LocalTarget(self.date.strftime('soakDBfiles/soakDB_%Y%m%d.txt'))

    def run(self):
        process = subprocess.Popen('''find /dls/labxchem/data/*/lb*/* -maxdepth 4 -path "*/lab36/*" -prune -o -path "*/initial_model/*" -prune -o -path "*/beamline/*" -prune -o -path "*/analysis/*" -prune -o -path "*ackup*" -prune -o -path "*old*" -prune -o -name "soakDBDataFile.sqlite" -print''',
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        out, err = process.communicate()

        print out
        print err

        with self.output().open('w') as f:
            f.write(out)

        f.close()


class WriteWhitelists(luigi.Task):
    pass

class WriteBlacklist(luigi.Task):
    pass

class TransferFedIDs(luigi.Task):
    def requires(self):
        return FindSoakDBFiles()

    def output(self):
        pass

    def run(self):
        conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS soakdb_files (filename TEXT, modification_date BIGINT, proposal TEXT)'''
                  )
        conn.commit()

        with self.input().open('r') as database_list:
            for database_file in database_list.readlines():
                database_file = database_file.replace('\n', '')
                proposal = database_file.split('/')[5].split('-')[0]
                proc = subprocess.Popen(str('getent group ' + str(proposal)), stdout=subprocess.PIPE, shell=True)
                out, err = proc.communicate()
                fedids_forsqltable = str(out.split(':')[3].replace('\n', ''))
                modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(database_file)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                modification_date = modification_date.replace('-', '')
                modification_date = modification_date.replace(':', '')
                modification_date = modification_date.replace(' ', '')
                c.execute('''INSERT INTO soakdb_files (filename, modification_date, proposal) SELECT %s,%s,%s WHERE NOT EXISTS (SELECT filename, modification_date FROM soakdb_files WHERE filename = %s AND modification_date = %s)''', (database_file, int(modification_date), proposal, database_file, int(modification_date)))
                conn.commit()

        c.execute('CREATE TABLE IF NOT EXISTS proposals (proposal TEXT, fedids TEXT)')

        proposal_list = []
        c.execute('SELECT proposal FROM soakdb_files')
        rows = c.fetchall()
        for row in rows:
            proposal_list.append(str(row[0]))

        for proposal_number in set(proposal_list):
            proc = subprocess.Popen(str('getent group ' + str(proposal_number)), stdout=subprocess.PIPE, shell=True)
            out, err = proc.communicate()
            append_list = out.split(':')[3].replace('\n', '')

            c.execute(str('''INSERT INTO proposals (proposal, fedids) SELECT %s, %s WHERE NOT EXISTS (SELECT proposal, fedids FROM proposals WHERE proposal = %s AND fedids = %s);'''), (proposal_number, append_list, proposal_number, append_list))
            conn.commit()

        c.close()


class TransferExperiment(luigi.Task):
    def requires(self):
        return FindSoakDBFiles()

    def output(self):
        pass

    def run(self):

        def create_list_from_ind(row, array, numbers_list):
            for ind in numbers_list:
                array.append(row[ind])

        def pop_dict(array, dictionary, dictionary_keys):
            for i in range(0, len(dictionary_keys)):
                dictionary[dictionary_keys[i]].append(array[i])
            return dictionary

        def add_keys(dictionary, keys):
            for key in keys:
                dictionary[key] = []

        lab_dict = {}
        crystal_dict = {}
        data_collection_dict = {}
        data_processing_dict = {}
        dimple_dict = {}
        refinement_dict = {}
        #pandda_dict = {}

        lab_dictionary_keys = ['visit', 'library_plate', 'library_name', 'smiles', 'compound_code', 'protein',
                               'stock_conc', 'expr_conc',
                               'solv_frac', 'soak_vol', 'soak_status', 'cryo_Stock_frac', 'cryo_frac',
                               'cryo_transfer_vol', 'cryo_status',
                               'soak_time', 'harvest_status', 'crystal_name', 'mounting_result', 'mounting_time',
                               'data_collection_visit']

        crystal_dictionary_keys = ['tag', 'name', 'spacegroup', 'point_group', 'a', 'b', 'c', 'alpha',
                                   'beta', 'gamma', 'volume']

        data_collection_dictionary_keys = ['date', 'outcome', 'wavelength']

        data_processing_dictionary_keys = ['image_path', 'program', 'spacegroup', 'unit_cell', 'auto_assigned',
                                           'res_overall',
                                           'res_low', 'res_low_inner_shell', 'res_high', 'res_high_15_sigma',
                                           'res_high_outer_shell',
                                           'r_merge_overall', 'r_merge_low', 'r_merge_high', 'isig_overall', 'isig_low',
                                           'isig_high', 'completeness_overall', 'completeness_low', 'completeness_high',
                                           'multiplicity_overall', 'multiplicity_low', 'multiplicity_high',
                                           'cchalf_overall',
                                           'cchalf_low', 'cchalf_high', 'logfile_path', 'mtz_path', 'log_name',
                                           'mtz_name',
                                           'original_directory', 'unique_ref_overall', 'lattice', 'point_group',
                                           'unit_cell_vol',
                                           'alert', 'score', 'status', 'r_cryst', 'r_free', 'dimple_pdb_path',
                                           'dimple_mtz_path',
                                           'dimple_status']

        dimple_dictionary_keys = ['res_high', 'pdb_path', 'mtz_path', 'reference_pdb', 'status', 'pandda_run',
                                  'pandda_hit',
                                  'pandda_reject', 'pandda_path']

        refinement_dictionary_keys = ['res', 'res_TL', 'rcryst', 'rcryst_TL', 'r_free', 'rfree_TL', 'spacegroup',
                                      'lig_cc', 'rmsd_bonds',
                                      'rmsd_bonds_TL', 'rmsd_angles', 'rmsd_angles_TL', 'outcome', 'mtz_free', 'cif',
                                      'cif_status', 'cif_prog',
                                      'pdb_latest', 'mtz_latest', 'matrix_weight', 'refinement_path', 'lig_confidence',
                                      'lig_bound_conf', 'bound_conf', 'molprobity_score', 'molprobity_score_TL',
                                      'ramachandran_outliers', 'ramachandran_outliers_TL', 'ramachandran_favoured',
                                      'ramachandran_favoured_TL', 'status']

        # pandda_dictionary_keys = ['CrystalName', 'path', 'site_index', 'site_name', 'site_comment', 'site_event_index',
        #                           'site_event_comment', 'site_confidence', 'site_InspectConfidence',
        #                           'site_ligand_placed',
        #                           'site_viewed', 'site_interesting', 'site_z_peak', 'site_x', 'site_y', 'site_z',
        #                           'site_ligand_id',
        #                           'site_ligand_resname', 'site_ligand_chain', 'site_ligand_sequence_number',
        #                           'site_ligand_altLoc', 'site_event_map', 'site_event_map_mtz', 'site_initial_model',
        #                           'site_initial_mtz', 'site_spider_plot', 'site_occupancy', 'site_B_average',
        #                           'site_B_ratio_residue_surroundings', 'site_RSCC', 'site_RSR', 'site_RSZD',
        #                           'site_rmsd',
        #                           'RefinementOutcome', 'ApoStructures', 'LastUpdated']

        dictionaries = [[lab_dict, lab_dictionary_keys], [crystal_dict, crystal_dictionary_keys],
                        [data_collection_dict, data_collection_dictionary_keys],
                        [data_processing_dict, data_processing_dictionary_keys],
                        [dimple_dict, dimple_dictionary_keys], [refinement_dict, refinement_dictionary_keys]]

                        #[pandda_dict, pandda_dictionary_keys]]

        for dictionary in dictionaries:
            add_keys(dictionary[0], dictionary[1])

        count = 0

        soakstatus = 'done'
        cryostatus = 'pending'
        mountstatus = '%Mounted%'
        collectionstatus = '%success%'
        compsmiles = '%None%'

        lab_table_numbers = range(0, 21)
        #project_directory = [21]
        crystal_table_numbers = range(22, 33)
        data_collection_table_numbers = range(33, 36)
        data_processing_table_numbers = range(36, 79)
        dimple_table_numbers = range(79, 89)
        #temp_pandda_numbers = range(89, 91)
        refinement_table_numbers = range(91, 122)

        # connect to master postgres db
        conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
        c = conn.cursor()

        # get all soakDB file names and close postgres connection
        c.execute('select filename from soakdb_files')
        rows = c.fetchall()
        c.close()

        # set database filename from postgres query
        for row in rows:
            database_file = str(row[0])

            # connect to soakDB
            conn2 = sqlite3.connect(str(database_file))
            c2 = conn2.cursor()

            try:
                # checks: SoakStatus=Done, CryoStatus!=pending or fail, CrystalName exists, MountingResult contains Mounted
                # columns with issues: ProjectDirectory, DatePANDDAModelCreated
                # depo columns
                # 104/145 currently work... Apply filters, see which ones are actually useful, count again!

                # use dictionaries for tables!

                for row in c2.execute('''select LabVisit, LibraryPlate, LibraryName, CompoundSMILES, CompoundCode,
                                        ProteinName, CompoundStockConcentration, CompoundConcentration, SolventFraction, 
                                        SoakTransferVol, SoakStatus, CryoStockFraction, CryoFraction, CryoTransferVolume, 
                                        CryoStatus, SoakingTime, HarvestStatus, CrystalName, MountingResult, MountingTime, 
                                        DataCollectionVisit, 

                                        ProjectDirectory, 

                                        CrystalTag, CrystalFormName, CrystalFormSpaceGroup,
                                        CrystalFormPointGroup, CrystalFormA, CrystalFormB, CrystalFormC,CrystalFormAlpha, 
                                        CrystalFormBeta, CrystalFormGamma, CrystalFormVolume, 

                                        DataCollectionDate, 
                                        DataCollectionOutcome, DataCollectionWavelength, 

                                        DataProcessingPathToImageFiles,
                                        DataProcessingProgram, DataProcessingSpaceGroup, DataProcessingUnitCell,
                                        DataProcessingAutoAssigned, DataProcessingResolutionOverall, DataProcessingResolutionLow,
                                        DataProcessingResolutionLowInnerShell, DataProcessingResolutionHigh, 
                                        DataProcessingResolutionHigh15Sigma, DataProcessingResolutionHighOuterShell, 
                                        DataProcessingRMergeOverall, DataProcessingRMergeLow, DataProcessingRMergeHigh, 
                                        DataProcessingIsigOverall, DataProcessingIsigLow, DataProcessingIsigHigh, 
                                        DataProcessingCompletenessOverall, DataProcessingCompletenessLow,
                                        DataProcessingCompletenessHigh, DataProcessingMultiplicityOverall, 
                                        DataProcessingMultiplicityLow, DataProcessingMultiplicityHigh, 
                                        DataProcessingCChalfOverall, DataProcessingCChalfLow, DataProcessingCChalfHigh, 
                                        DataProcessingPathToLogFile, DataProcessingPathToMTZfile, DataProcessingLOGfileName, 
                                        DataProcessingMTZfileName, DataProcessingDirectoryOriginal,
                                        DataProcessingUniqueReflectionsOverall, DataProcessingLattice, DataProcessingPointGroup,
                                        DataProcessingUnitCellVolume, DataProcessingAlert, DataProcessingScore,
                                        DataProcessingStatus, DataProcessingRcryst, DataProcessingRfree, 
                                        DataProcessingPathToDimplePDBfile, DataProcessingPathToDimpleMTZfile,
                                        DataProcessingDimpleSuccessful, 

                                        DimpleResolutionHigh, DimpleRfree, DimplePathToPDB,
                                        DimplePathToMTZ, DimpleReferencePDB, DimpleStatus, DimplePANDDAwasRun, 
                                        DimplePANDDAhit, DimplePANDDAreject, DimplePANDDApath, 

                                        PANDDAStatus, DatePANDDAModelCreated, 

                                        RefinementResolution, RefinementResolutionTL, 
                                        RefinementRcryst, RefinementRcrystTraficLight, RefinementRfree, 
                                        RefinementRfreeTraficLight, RefinementSpaceGroup, RefinementLigandCC, 
                                        RefinementRmsdBonds, RefinementRmsdBondsTL, RefinementRmsdAngles, RefinementRmsdAnglesTL,
                                        RefinementOutcome, RefinementMTZfree, RefinementCIF, RefinementCIFStatus, 
                                        RefinementCIFprogram, RefinementPDB_latest, RefinementMTZ_latest, RefinementMatrixWeight, 
                                        RefinementPathToRefinementFolder, RefinementLigandConfidence, 
                                        RefinementLigandBoundConformation, RefinementBoundConformation, RefinementMolProbityScore,
                                        RefinementMolProbityScoreTL, RefinementRamachandranOutliers, 
                                        RefinementRamachandranOutliersTL, RefinementRamachandranFavored, 
                                        RefinementRamachandranFavoredTL, RefinementStatus

                                        from mainTable 
                                        where SoakStatus = ?
                                        and CryoStatus not like ? 
                                        and MountingResult like ? 
                                        and DataCollectionOutcome like ?
                                        and CompoundSMILES not like ? ''',
                                      (soakstatus, cryostatus, mountstatus, collectionstatus, compsmiles)):

                    lab_table_list = []
                    crystal_table_list = []
                    data_collection_table_list = []
                    data_processing_table_list = []
                    dimple_table_list = []
                    refinement_table_list = []

                    lists = [lab_table_list, crystal_table_list, data_collection_table_list, data_processing_table_list,
                             dimple_table_list, refinement_table_list]

                    numbers = [lab_table_numbers, crystal_table_numbers, data_collection_table_numbers,
                               data_processing_table_numbers, dimple_table_numbers, refinement_table_numbers]
                    listref = 0

                    for listname in lists:
                        create_list_from_ind(row, listname, numbers[listref])
                        listref += 1

                    pop_dict(lab_table_list, lab_dict, lab_dictionary_keys)
                    pop_dict(crystal_table_list, crystal_dict, crystal_dictionary_keys)
                    pop_dict(refinement_table_list, refinement_dict, refinement_dictionary_keys)
                    pop_dict(dimple_table_list, dimple_dict, dimple_dictionary_keys)
                    pop_dict(data_collection_table_list, data_collection_dict,
                             data_collection_dictionary_keys)
                    pop_dict(data_processing_table_list, data_processing_dict,
                             data_processing_dictionary_keys)

                # for row in c2.execute('''select CrystalName,
                #                         PANDDApath,
                #                         PANDDA_site_index,
                #                         PANDDA_site_name,
                #                         PANDDA_site_comment,
                #                         PANDDA_site_event_index,
                #                         PANDDA_site_event_comment,
                #                         PANDDA_site_confidence,
                #                         PANDDA_site_InspectConfidence,
                #                         PANDDA_site_ligand_placed,
                #                         PANDDA_site_viewed,
                #                         PANDDA_site_interesting,
                #                         PANDDA_site_z_peak,
                #                         PANDDA_site_x,
                #                         PANDDA_site_y,
                #                         PANDDA_site_z,
                #                         PANDDA_site_ligand_id,
                #                         PANDDA_site_ligand_resname,
                #                         PANDDA_site_ligand_chain,
                #                         PANDDA_site_ligand_sequence_number,
                #                         PANDDA_site_ligand_altLoc,
                #                         PANDDA_site_event_map,
                #                         PANDDA_site_event_map_mtz,
                #                         PANDDA_site_initial_model,
                #                         PANDDA_site_initial_mtz,
                #                         PANDDA_site_spider_plot,
                #                         PANDDA_site_occupancy,
                #                         PANDDA_site_B_average,
                #                         PANDDA_site_B_ratio_residue_surroundings,
                #                         PANDDA_site_RSCC,
                #                         PANDDA_site_RSR,
                #                         PANDDA_site_RSZD,
                #                         PANDDA_site_rmsd,
                #                         RefinementOutcome,
                #                         ApoStructures,
                #                         LastUpdated
                #                         from panddaTable'''):
                #
                #     pandda_array = []
                #
                #     for i in range(0, len(row)):
                #         pandda_array.append(row[i])
                #
                #     pop_dict(pandda_array, pandda_dict, pandda_dictionary_keys)
                #
                # count += 1

            except:
                print database_file
                print sys.exc_info()
                print ' '
                c2.close()

        labdf = pandas.DataFrame.from_dict(lab_dict)
        crystaldf = pandas.DataFrame.from_dict(crystal_dict)
        dataprocdf = pandas.DataFrame.from_dict(data_processing_dict)
        datacoldf = pandas.DataFrame.from_dict(data_collection_dict)
        refdf = pandas.DataFrame.from_dict(refinement_dict)
        dimpledf = pandas.DataFrame.from_dict(dimple_dict)
        # panddadf = pandas.DataFrame.from_dict(pandda_dict)

        # create an engine to postgres database and populate tables - ids are straight from dataframe index,
        # but link all together
        # TODO:
        # find way to do update, rather than just create table each time
        
        engine = create_engine('postgresql://uzw12877@localhost:5432/xchem')
        labdf.to_sql('lab', engine)
        crystaldf.to_sql('crystal', engine)
        dataprocdf.to_sql('data_processing', engine)
        datacoldf.to_sql('data_collection', engine)
        refdf.to_sql('refinement', engine)
        dimpledf.to_sql('dimple', engine)
        panddadf.to_sql('pandda', engine)






