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
                        '-maxdepth 10 -path "*/lab36/*" -prune -o',
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

    print(files_list)

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


# def centroid_from_sites(sites_list, sites_file):
#     # get dataframe from sites file
#     sites_frame = pd.read_csv(sites_file)
#
#     # get indicies of sites from sites list, to decide where to pull centroids from
#     indicies = [i for i, x in enumerate(sites_frame['site_idx']) if x in sites_list]
#     sites = [x for i, x in enumerate(sites_frame['site_idx']) if x in sites_list]
#     native_centroids = []
#     aligned_centroids = []
#
#     for ind in indicies:
#         native_centroids.append(sites_frame['native_centroid'][ind])
#         aligned_centroids.append(sites_frame['centroid'][ind])
#
#     return native_centroids, aligned_centroids, indicies, sites


def get_file_names(BDC, crystal, input_dir, output_dir, event):

    map_file_name = ''.join([crystal, '-event_', str(event), '_1-BDC_', str(BDC), '_map.native.ccp4'])
    map_file_path = os.path.join(input_dir.replace('*', ''), crystal, map_file_name)

    input_pdb_name = ''.join([crystal, '-pandda-input.pdb'])
    input_pdb_path = os.path.join(input_dir.replace('*', ''), crystal, input_pdb_name)

    input_mtz_name = input_pdb_name.replace('.pdb', '.mtz')
    input_mtz_path = input_pdb_path.replace(input_pdb_name, input_mtz_name)

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


def find_ligand_site_event(nx, ny, nz, ex, ey, ez, lig_strings, pandda_model_path):
    # nn = native_centroid n, en = event_centroid n
    event_centroid = [ex,ey,ez]
    native_centroid = [nx, ny, nz]
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

    return ligand, lig_centroid, min_dist, event_displacement
