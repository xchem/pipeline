import psycopg2

def connectDB():
    conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
    c = conn.cursor()

    return conn, c

def table_exists(c, tablename):
    c.execute('''select exists(select * from information_schema.tables where table_name=%s);''', (tablename,))
    exists = c.fetchone()[0]
    return exists

def soakdb_query(cursor):
    compsmiles = '%None%'
    cursor.execute('''select LabVisit, LibraryPlate, LibraryName, CompoundSMILES, CompoundCode,
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
                                            where CrystalName NOT LIKE ?
                                            and CrystalName IS NOT NULL 		
                                            and CompoundSMILES not like ? 
                                            and CompoundSMILES IS NOT NULL''',
               ('None', compsmiles))


def create_insert_query(keys, indicies):
    pass


def define_dicts_and_keys():
    lab_dict = {}
    crystal_dict = {}
    data_collection_dict = {}
    data_processing_dict = {}
    dimple_dict = {}
    refinement_dict = {}

    # define keys for xchem postgres DB
    lab_dictionary_keys = ['visit', 'library_plate', 'library_name', 'smiles', 'compound_code', 'protein',
                           'stock_conc', 'expr_conc',
                           'solv_frac', 'soak_vol', 'soak_status', 'cryo_stock_frac', 'cryo_frac',
                           'cryo_transfer_vol', 'cryo_status',
                           'soak_time', 'harvest_status', 'crystal_name', 'mounting_result', 'mounting_time',
                           'data_collection_visit', 'crystal_id']

    crystal_dictionary_keys = ['tag', 'name', 'spacegroup', 'point_group', 'a', 'b', 'c', 'alpha',
                               'beta', 'gamma', 'volume', 'crystal_name', 'crystal_id']

    data_collection_dictionary_keys = ['date', 'outcome', 'wavelength', 'crystal_name', 'crystal_id']

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
                                       'dimple_status', 'crystal_name', 'crystal_id']

    dimple_dictionary_keys = ['res_high', 'r_free', 'pdb_path', 'mtz_path', 'reference_pdb', 'status', 'pandda_run',
                              'pandda_hit',
                              'pandda_reject', 'pandda_path', 'crystal_name', 'crystal_id']

    refinement_dictionary_keys = ['res', 'res_TL', 'rcryst', 'rcryst_TL', 'r_free', 'rfree_TL', 'spacegroup',
                                  'lig_cc', 'rmsd_bonds',
                                  'rmsd_bonds_TL', 'rmsd_angles', 'rmsd_angles_TL', 'outcome', 'mtz_free', 'cif',
                                  'cif_status', 'cif_prog',
                                  'pdb_latest', 'mtz_latest', 'matrix_weight', 'refinement_path', 'lig_confidence',
                                  'lig_bound_conf', 'bound_conf', 'molprobity_score', 'molprobity_score_TL',
                                  'ramachandran_outliers', 'ramachandran_outliers_TL', 'ramachandran_favoured',
                                  'ramachandran_favoured_TL', 'status', 'crystal_name', 'crystal_id']

    dictionaries = [[lab_dict, lab_dictionary_keys], [crystal_dict, crystal_dictionary_keys],
                    [data_collection_dict, data_collection_dictionary_keys],
                    [data_processing_dict, data_processing_dictionary_keys],
                    [dimple_dict, dimple_dictionary_keys], [refinement_dict, refinement_dictionary_keys]]

    return lab_dictionary_keys, crystal_dictionary_keys, data_processing_dictionary_keys, dimple_dictionary_keys, \
           refinement_dictionary_keys, dictionaries

# from https://www.ryanbaumann.com/blog/2016/4/30/python-pandas-tosql-only-insert-new-rows
def clean_df_db_dups(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
    """
    Remove rows from a dataframe that already exist in a database
    Required:
        df : dataframe to remove duplicate rows from
        engine: SQLAlchemy engine object
        tablename: tablename to check duplicates in
        dup_cols: list or tuple of column names to check for duplicate row values
    Optional:
        filter_continuous_col: the name of the continuous data column for BETWEEEN min/max filter
                               can be either a datetime, int, or float data type
                               useful for restricting the database table size to check
        filter_categorical_col : the name of the categorical data column for Where = value check
                                 Creates an "IN ()" check on the unique values in this column
    Returns
        Unique list of values from dataframe compared to database table
    """
    args = 'SELECT %s FROM %s' %(', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
    args_contin_filter, args_cat_filter = None, None
    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s')
                                          AND Convert(datetime, '%s')""" %(filter_continuous_col,
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())


    if filter_categorical_col is not None:
        args_cat_filter = ' "%s" in(%s)' %(filter_categorical_col,
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    df = pd.merge(df, pd.read_sql(args, engine), how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df
