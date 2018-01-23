import db_functions
import os

# set filepaths
hit_directory = '/dls/science/groups/proasis/LabXChem/'

conn, c = db_functions.connectDB()
c.execute("SELECT bound_conf, crystal_name, protein, strucid FROM proasis_hits WHERE strucid !=''")
rows = c.fetchall()

for row in rows:
    bound_pdb = str(row[0])
    protein_name = str(row[2])
    crystal = str(row[1])
    strucid = str(row[3])

    # set up directory paths for where files will be stored (for proasis)
    proasis_protein_directory = str(str(hit_directory) + '/' + str(protein_name) + '/')
    proasis_crystal_directory = str(str(hit_directory) + '/' + str(protein_name) + '/'
                                    + str(crystal) + '/')

    pdb_file_name = str(bound_pdb).split('/')[-1]

    # if the bound pdb is in a refinement folder, change the path to find the map files
    if 'Refine' in bound_pdb.replace(pdb_file_name, ''):
        remove_string = str(str(bound_pdb).split('/')[-2] + '/' + pdb_file_name)
        map_directory = str(bound_pdb).replace(remove_string, '')
    else:
        map_directory = str(bound_pdb).replace(pdb_file_name, '')

    # copy the 2fofc and fofc maps over to the proasis directories
    if os.path.isfile(str(map_directory + '/2fofc.map')):
        if not os.path.isfile(str(proasis_crystal_directory + '/2fofc.map')):
            os.system(str('cp ' + str(map_directory + '/2fofc.map ' + proasis_crystal_directory)))
            submit_2fofc = str('/usr/local/Proasis2/utils/addnewfile.py -i 2fofc_c -f '
                               + proasis_crystal_directory + '/2fofc.map -s ' + strucid + ' -t ' + "'" + str(
                crystal) + "_2fofc'")
            os.system(submit_2fofc)

    if os.path.isfile(str(map_directory + '/fofc.map')):
        if not os.path.isfile(str(proasis_crystal_directory + '/fofc.map')):
            os.system(str('cp ' + str(map_directory + '/fofc.map ' + proasis_crystal_directory)))
            submit_fofc = str('/usr/local/Proasis2/utils/addnewfile.py -i fofc_c -f '
                               + proasis_crystal_directory + '/fofc.map -s ' + strucid + ' -t ' + "'" + str(
                crystal) + "_fofc'")
            os.system(submit_fofc)

    if os.path.isfile(str(map_directory + '/refine.mtz')):
        if not os.path.isfile(str(proasis_crystal_directory + '/refine.mtz')):
            os.system(str('cp ' + str(map_directory + '/refine.mtz ' + proasis_crystal_directory)))
            submit_mtz = str('/usr/local/Proasis2/utils/addnewfile.py -i mtz -f '
                               + proasis_crystal_directory + '/refine.mtz -s ' + strucid + ' -t ' + "'" + str(
                crystal) + "_mtz'")
            os.system(submit_mtz)

    print(map_directory)

