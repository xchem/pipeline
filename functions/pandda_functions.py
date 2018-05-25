import os
import re
import subprocess
import pandas as pd
import datetime
import subprocess
import os
import re
from rdkit.Chem import rdMolTransforms
from rdkit import Chem
import numpy as np


def find_log_files(path):
    command = ' '.join(['find',
                        path,
                        '-maxdepth 5 -path "*/lab36/*" -prune -o',
                        '-path "*/initial_model/*" -prune -o',
                        '-path "*/beamline/*" -prune -o',
                        '-path "*ackup*" -prune -o',
                        '-path "*old*" -prune -o',
                        '-path "*TeXRank*" -prune -o',
                        '-name "pandda-*.log"',
                        '-print'])

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = process.communicate()

    files_list = out.decode('ascii')

    return files_list


def get_files_from_log(log):
    # return blank strings if info not found - to be handled in task:
    #
    # if Error:
    #     with open(self.output().path, 'a') as f:
    #         f.write('Error found in log file, will not run...')
    #     raise Exception('Error found in log file, will not run...')
    #
    # if '0.1.' in pver:
    #     with open(self.output().path, 'a') as f:
    #         f.write('Pandda analyse run with old version (' + pver + '), please rerun!')
    #     raise Exception('Pandda analyse run with old version (' + pver + '), please rerun!')
    #
    # if sites_file == '':
    #     raise Exception('No sites file found in log: log=' + str(self.log_file) + ' pandda_version=' + pver)
    #
    # if events_file == '':
    #     raise Exception('No events file found in log: log=' + str(self.log_file) + ' pandda_version=' + pver)
    
    pver = ''
    input_dir = ''
    output_dir = ''
    sites_file = ''
    events_file = ''
    Error = False

    for line in open(log, 'r'):
        # get pandda version from log file
        if 'Pandda Version' in line:
            pver = str(line.split()[-1])
        # get input directory from log file
        if 'data_dirs' in line:
            input_dir = re.sub('\s+', '', line.split('=')[-1]).replace('"', '')
        # get output dir from log file
        if 'out_dir' in line:
            output_dir = re.sub('\s+', '', line.split('=')[-1]).replace('"', '')
        # get sites file from log file
        if 'pandda_analyse_sites.csv' in line:
            to_check = re.sub('\s+', '', line)
            if os.path.isfile(to_check):
                sites_file = to_check
        # get events file from log file
        if 'pandda_analyse_events.csv' in line:
            to_check = re.sub('\s+', '', line)
            if os.path.isfile(to_check):
                events_file = to_check
        # check if pandda ran successfully
        if 'exited with an error' in line:
            Error = True

    return pver, input_dir, output_dir, sites_file, events_file, Error


def get_sites_from_events(events_file):
    print(events_file)
    # read events file as dataframe
    events_frame = pd.read_csv(events_file)

    # holders for crystal, event and site
    crystals = []
    events = []
    sites = []
    bdc = []

    for i in range(0, len(events_frame)):
        # get crystal name and event and site numbers from frame
        crystals.append(str(events_frame['dtag'][i]))
        events.append(int(events_frame['event_idx'][i]))
        sites.append(int(events_frame['site_idx'][i]))
        bdc.append(str(events_frame['1-BDC'][i]))

    return crystals, events, sites, bdc


def centroid_from_sites(sites_list, sites_file):
    # get dataframe from sites file
    sites_frame = pd.read_csv(sites_file)

    # get indicies of sites from sites list, to decide where to pull centroids from
    indicies = [i for i, x in enumerate(sites_frame['site_idx']) if x in sites_list]
    sites = [x for i, x in enumerate(sites_frame['site_idx']) if x in sites_list]
    native_centroids = []
    aligned_centroids = []

    for ind in indicies:
        native_centroids.append(sites_frame['native_centroid'][ind])
        aligned_centroids.append(sites_frame['centroid'][ind])

    return native_centroids, aligned_centroids, indicies, sites


def get_file_names(BDC, crystal, input_dir, output_dir):

    map_file_name = ''.join([crystal, '-event_', event, '_1-BDC_', BDC, '_map.native.ccp4'])
    map_file_path = os.path.join(input_dir.replace('*', ''), crystal, map_file_name)

    # input_pdb_name = ''.join([crystal, '-pandda-input.pdb'])
    # input_pdb_path = os.path.join(input_dir.replace('*', ''), crystal, input_pdb_name)
    #
    # input_mtz_name = input_pdb_name.replace('.pdb', '.mtz')
    # input_mtz_path = input_pdb_path.replace(input_pdb_name, input_mtz_name)

    aligned_pdb_name = ''.join([crystal, '-aligned.pdb'])
    aligned_pdb_path = os.path.join(output_dir, 'aligned_structures', aligned_pdb_name)

    pandda_model_name = input_pdb_name.replace('-input', '-model')
    pandda_model_path = input_pdb_path.replace(input_pdb_name, pandda_model_name)

    exists_array = [os.path.isfile(filepath) for filepath in [map_file_path, input_pdb_path, input_mtz_path,
                                                                          aligned_pdb_path, pandda_model_path]]

    return map_file_path, input_pdb_path, input_mtz_path, aligned_pdb_path, pandda_model_path, exists_array


def find_ligands(pandda_model_path):
    lig_strings = []
    for line in open(pandda_model_path, 'r'):
        if 'LIG' in line:
            lig_string = re.search(r"LIG.......", line).group()
            lig_strings.append(lig_string)
    lig_strings = list(set(lig_strings))

    return lig_strings


def find_ligand_esite_event(events_frame, lig_strings, pandda_model_path):
    event_centroid = [events_frame['x'][i], events_frame['y'][i], events_frame['z'][i]]
    event_displacement = np.linalg.norm([native_centroid, event_centroid])
    lig_distances = []
    lig_centres = []
    for lig in lig_strings:
        lig_pdb = []

        for line in open(pandda_model_path):
            if lig in line:
                lig_pdb.append(line)
        lig_pdb = (''.join(lig_pdb))
        mol = Chem.MolFromPDBBlock(lig_pdb)
        conf = mol.GetConformer()
        centre = rdMolTransforms.ComputeCentroid(conf)
        lig_centre = [centre.x, centre.y, centre.z]
        lig_centres.append(lig_centre)

        matrix = [lig_centre, event_centroid]
        dist = np.linalg.norm(matrix)

        lig_event_dist = abs(event_displacement-dist)
        lig_distances.append(lig_event_dist)

    min_dist = min(lig_distances)
    for j in range(0, len(lig_distances)):
        if lig_distances[j]==min_dist:
            ind = j

    ligand = lig_strings[ind]
    lig_centroid = lig_centres[ind]

#         results_dict = {'pandda_version':[],
#                         'crystal':[],
#                         'event':[],
#                         'site':[],
#                         'site_native_centroid':[],
#                         'site_aligned_centroid':[],
#                         'event_centroid':[],
#                         'event_dist_from_site_centroid':[],
#                         'lig_id':[],
#                         'lig_centroid':[],
#                         'pandda_event_map_native':[],
#                         'pandda_model_pdb':[],
#                         'pandda_input_pdb':[],
#                         'pandda_input_mtz':[],
#                         'lig_dist_event_centroid': [],
#                         'pandda_dir':[],
#                         'pandda_log':[],
#                         'input_dir':[]}

#             for j in range(0, len(sites_frame)):
#                 if int(sites_frame['site_idx'][j]) == int(site):
#                     native_centroid = sites_frame['native_centroid'][j]
#                     aligned_centroid = sites_frame['centroid'][j]
#
#             native_centroid = list(eval(native_centroid))
#             aligned_centroid = list(eval(aligned_centroid))
#
#             with open(self.output().path, 'a') as f:
#                 f.write('----Parsing file system for ' + crystal + ' (event: ' + event + ' site:' + site + ')\n')
#
#             BDC = str(events_frame['1-BDC'][i])
#
#             map_file_name = ''.join([crystal, '-event_', event, '_1-BDC_', BDC, '_map.native.ccp4'])
#             map_file_path = os.path.join(input_dir.replace('*', ''), crystal, map_file_name)
#
#             input_pdb_name = ''.join([crystal, '-pandda-input.pdb'])
#             input_pdb_path = os.path.join(input_dir.replace('*', ''), crystal, input_pdb_name)
#
#             input_mtz_name = input_pdb_name.replace('.pdb', '.mtz')
#             input_mtz_path = input_pdb_path.replace(input_pdb_name, input_mtz_name)
#
#             aligned_pdb_name = ''.join([crystal, '-aligned.pdb'])
#             aligned_pdb_path = os.path.join(output_dir, 'aligned_structures', aligned_pdb_name)
#
#             pandda_model_name = input_pdb_name.replace('-input', '-model')
#             pandda_model_path = input_pdb_path.replace(input_pdb_name, pandda_model_name)
#
#             with open(self.output().path, 'a') as f:
#                 f.write('    ----Checking for files: event map, input pdb/mtz, aligned pdb, model pdb\n')
#
#             exists_array = [os.path.isfile(filepath) for filepath in [map_file_path, input_pdb_path, input_mtz_path,
#                                                                       aligned_pdb_path, pandda_model_path]]
#
#             if False in exists_array:
#                 with open(self.output().path, 'a') as f:
#                     f.write('        ----Missing expected files for ' + crystal + '(event:' + event + ' site: ' +
#                             site + ') : SKIPPING!\n\n')
#
#             else:
#
#                 with open(self.output().path, 'a') as f:
#                     f.write('        ----All files found: OK!\n\n')
#                 with open(self.output().path, 'a') as f:
#                     f.write('    ----Finding ligand string (for this event) from model pdb\n')
#                 lig_strings = []
#                 for line in open(pandda_model_path, 'r'):
#                     if 'LIG' in line:
#                         lig_string = re.search(r"LIG.......", line).group()
#                         lig_strings.append(lig_string)
#                 lig_strings = list(set(lig_strings))
#                 event_centroid = [events_frame['x'][i], events_frame['y'][i], events_frame['z'][i]]
#                 event_displacement = np.linalg.norm([native_centroid, event_centroid])
#
#                 if len(lig_strings) == 0:
#                     with open(self.output().path, 'a') as f:
#                         f.write('        ----Missing ligand in pandda model pdb: ' + pandda_model_name +
#                                 ': SKIPPING!\n\n')
#
#                 else:
#                     with open(self.output().path, 'a') as f:
#                         f.write('        ----Aligning ligand(s) to event centroid: ' + str(event_centroid) + '\n')
#                     lig_distances = []
#                     lig_centres = []
#                     for lig in lig_strings:
#                         lig_pdb = []
#
#                         for line in open(pandda_model_path):
#                             if lig in line:
#                                 lig_pdb.append(line)
#                         lig_pdb = (''.join(lig_pdb))
#                         mol = Chem.MolFromPDBBlock(lig_pdb)
#                         conf = mol.GetConformer()
#                         centre = rdMolTransforms.ComputeCentroid(conf)
#                         lig_centre = [centre.x, centre.y, centre.z]
#                         lig_centres.append(lig_centre)
#
#                         matrix = [lig_centre, event_centroid]
#                         dist = np.linalg.norm(matrix)
#
#                         lig_event_dist = abs(event_displacement-dist)
#                         lig_distances.append(lig_event_dist)
#
#                     min_dist = min(lig_distances)
#                     for j in range(0, len(lig_distances)):
#                         if lig_distances[j]==min_dist:
#                             ind = j
#
#                     ligand = lig_strings[ind]
#                     lig_centroid = lig_centres[ind]
#
#                     with open(self.output().path, 'a') as f:
#                         f.write('            ----Event Ligand ID: ' + str(ligand) + '\n\n')
#
#                     results_dict['pandda_version'].append(pver)
#                     results_dict['crystal'].append(crystal)
#                     results_dict['event'].append(event)
#                     results_dict['site'].append(site)
#                     results_dict['site_native_centroid'].append(native_centroid)
#                     results_dict['site_aligned_centroid'].append(aligned_centroid)
#                     results_dict['event_centroid'].append(event_centroid)
#                     results_dict['event_dist_from_site_centroid'].append(event_displacement)
#                     results_dict['lig_id'].append(ligand)
#                     results_dict['lig_centroid'].append(lig_centroid)
#                     results_dict['pandda_event_map_native'].append(map_file_path)
#                     results_dict['pandda_model_pdb'].append(pandda_model_path)
#                     results_dict['pandda_input_pdb'].append(input_pdb_path)
#                     results_dict['pandda_input_mtz'].append(input_mtz_path)
#                     results_dict['lig_dist_event_centroid'].append(min_dist)
#                     results_dict['pandda_dir'].append(output_dir)
#                     results_dict['input_dir'].append(input_dir)
#                     results_dict['pandda_log'].append(str(self.log_file))
#
#         out_frame = pd.DataFrame.from_dict(results_dict)
#         with open(self.output().path, 'a') as f:
#             f.write('----Writing to output csv: ' + str(self.out_file) +'\n')
#
#         if os.path.isfile(self.out_file):
#             out_frame.to_csv(self.out_file, mode='a', header=False, index=False)
#         else:
#             out_frame.to_csv(self.out_file, index=False)
#
#         with open(self.output().path, 'a') as f:
#             f.write('----END \n')
