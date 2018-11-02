import os
import shutil
import subprocess

import requests

from functions import misc_functions
import json


def get_json(url):
    # send API request and pull output as json
    data = '''{"username":"uzw12877","password":"uzw12877"}'''
    r = requests.get(url, data=data)
    json_string = r.json()

    return json_string


def dict_from_string(json_string):
    # empty dict and counter to enumerate json
    s_dict = {}
    counter = -1

    # reformat json into python dictionary
    for key in list(json_string.keys()):
        try:
            counter += 1
            s_dict[key] = list(json_string.values())[counter].split(',')
        except:
            s_dict[key] = list(json_string.values())[counter]

    return s_dict


def get_strucids_from_project(project):
    url = str("http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projectlookup/" + project +
              "?strucsource=inh&idonly=1")

    json_string_strucids = get_json(url)
    dict_strucids = dict_from_string(json_string_strucids)
    try:
        strucids = list(dict_strucids['strucids'])

        return strucids
    except:
        return None


def delete_structure(strucid):
    delete_string = str('/usr/local/Proasis2/utils/removestruc.py -s ' + str(strucid))
    process = subprocess.Popen(delete_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')


def delete_project(name):
    delete_string = str('/usr/local/Proasis2/utils/removeoldproject.py -p ' + str(name))
    process = subprocess.Popen(delete_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')


def delete_all_inhouse(exception_list=None):
    if exception_list is None:
        exception_list = ['Zitzmann', 'Ali', 'CMGC_Kinases']
    all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

    json_string_projects = get_json(all_projects_url)
    dict_projects = dict_from_string(json_string_projects)

    all_projects = dict_projects['ALLPROJECTS']

    for project in all_projects:
        project_name = str(project['project'])
        print(project_name)
        if project_name in exception_list:
            continue
        else:
            strucids = get_strucids_from_project(str(project_name))
            for strucid in strucids:
                print(strucid)
                delete_structure(strucid)
            delete_project(project_name)


def count_all_inhouse(exception_list=None):
    if exception_list is None:
        exception_list = ['Zitzmann', 'Ali', 'CMGC_Kinases']
    count = 0
    all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

    json_string_projects = get_json(all_projects_url)
    dict_projects = dict_from_string(json_string_projects)

    all_projects = dict_projects['ALLPROJECTS']

    ids = []

    for project in all_projects:
        project_name = str(project['project'])
        print(project_name)
        if project_name in exception_list:
            continue
        else:
            strucids = get_strucids_from_project(str(project_name))
            for struc in strucids:
                ids.append(struc)
            count += len(strucids)

    return ids, count


def get_struc_mtz(strucid, out_dir):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/listfiles/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    filename = None
    for entry in file_dict['allfiles']:
        if 'STRUCFACMTZFILE' in entry['filetype']:
            if len(entry['filename']) < 2:
                raise Exception(str("The filename for mtz was not right... " + str(file_dict)))
            filename = str(entry['filename'])
        # else:
            # raise Exception("No mtz file was found by proasis: " + str(file_dict['allfiles']))
    if filename:
        print('moving stuff...')
        shutil.copy2(filename, out_dir)
        mtz_zipped = filename.split('/')[-1]
        command_string = ('gzip -d ' + out_dir + '/' + mtz_zipped)
        process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        out = out.decode('ascii')
        if err:
            err = err.decode('ascii')
        saved_to = str(mtz_zipped.replace('.gz', ''))
    else:
        saved_to = None

    return saved_to


def get_struc_map(strucid, out_dir, mtype):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/listfiles/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    filename = None
    for entry in file_dict['allfiles']:
        if mtype == 'fofc' and 'CCP4:Fo-Fc' in entry['filetype']:
            if len(entry['filename']) < 2:
                raise Exception(str("The filename for fofc was not right... " + str(file_dict)))
            filename = str(entry['filename'])
        if mtype == '2fofc' and 'CCP4:2Fo-Fc' in entry['filetype']:
            if len(entry['filename']) < 2:
                raise Exception(str("The filename for 2fofc was not right... " + str(file_dict)))
            filename = str(entry['filename'])
        # else:
            # raise Exception("No mtz file was found by proasis: " + str(file_dict['allfiles']))
    if filename:
        print('moving stuff...')
        shutil.copy2(filename, out_dir)
        mtz_zipped = filename.split('/')[-1]
        command_string = ('gzip -d ' + out_dir + '/' + mtz_zipped)
        process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        out = out.decode('ascii')
        if err:
            err = err.decode('ascii')
        saved_to = str(mtz_zipped.replace('.gz', ''))
    else:
        saved_to = None

    return saved_to


def get_struc_pdb(strucid, outfile):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/fetchfile/originalpdb/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    print(file_dict)
    if os.path.isfile(outfile):
        os.remove(outfile)

    command_string = ('touch ' + outfile)
    process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')

    with open(outfile, 'a') as f:
        try:
            for line in file_dict['output']:
                f.write(line)
        except:
            outfile = None
    return outfile


def submit_proasis_job_string(substring):
    process = subprocess.Popen(substring, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')

    strucidstr = misc_functions.get_id_string(out)

    return strucidstr, err, out


def add_proasis_file(file_type, filename, strucid, title):
    add_file = str(
        '/usr/local/Proasis2/utils/addnewfile.py' + ' ' + '-i' + ' ' + file_type + ' ' + '-f' + ' ' + filename + ' ' +
        '-s' + ' ' + strucid + ' ' + '-t' + ' ' + title)

    process = subprocess.Popen(add_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()

    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')

    return out, err


def get_lig_strings(lig_list):
    strings_list = []
    for ligand in lig_list:
        if len(ligand) == 3:
            strings_list.append(str(
                "{:>3}".format(ligand[0]) + "{:>2}".format(ligand[1]) + "{:>4}".format(ligand[2]) + ' '))
        else:
            raise Exception('ligand list wrong length!')
    return strings_list


def get_struc_file(strucid, outfile, ftype):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/fetchfile/' + ftype + '/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    # print file_dict
    if os.path.isfile(outfile):
        os.remove(outfile)

    command_string = ('touch ' + outfile)
    process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')

    print(out)

    with open(outfile, 'a') as f:
        try:
            for line in file_dict['output']:
                f.write(line)
        except:
            outfile = None
    return outfile


def get_strucid_json(strucid):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/idlookup/' + strucid)
    json_string = get_json(url)
    out_dict = dict_from_string(json_string)

    return out_dict


def get_lig_sdf(strucid, ligand, outfile):
    url = str(str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/fetchfile/sdf/' + strucid))
    data = str('{"username":"uzw12877","password":"uzw12877","ligand":"' + ligand + '"}')
    r = requests.get(url, data=data)
    json_string = r.json()
    file_dict = dict_from_string(json_string)
    if 'errorMessage' in file_dict.keys():
        data = str('{"username":"uzw12877","password":"uzw12877"}')
        r = requests.get(url, data=data)
        json_string = r.json()
        print(json_string)

        file_dict = dict_from_string(json_string)
    if os.path.isfile(outfile):
        os.remove(outfile)

    command_string = ('touch ' + outfile)
    process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('ascii')
    if err:
        err = err.decode('ascii')

    print(out)
    print(err)

    with open(outfile, 'a') as f:
        try:
            for line in file_dict['output']:
                f.write(line)
        except:
            outfile = None
    return outfile


def get_lig_interactions(strucid, ligand, outfile):
    url = str(str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/sc/' + strucid))
    data = str('{"username":"uzw12877","password":"uzw12877","ligand":"' + ligand + '"}')
    r = requests.get(url, data=data)
    json_string = r.json()
    print(json_string)
    file_dict = dict_from_string(json_string)

    if 'errorMessage' in file_dict.keys():
        data = str('{"username":"uzw12877","password":"uzw12877"}')
        r = requests.get(url, data=data)
        json_string = r.json()
        print(json_string)

    if os.path.isfile(outfile):
        os.remove(outfile)

    with open(outfile, 'w') as f:
        try:
            json.dump(json_string, f)
        except:
            outfile = None
    return outfile
