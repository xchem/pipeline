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