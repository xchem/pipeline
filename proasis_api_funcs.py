import requests
import os

def get_json(url):
    # send API request and pull output as json
    data = '''{"username":"uzw12877","password":"uzw12877"}'''
    r = requests.get(url, data=data)
    json_string = r.json()

    return json_string


def dict_from_string(json_string):
    # empty dict and counter to enumerate json
    dict = {}
    counter = -1

    # reformat json into python dictionary
    for key in json_string.keys():
        try:
            counter += 1
            dict[key] = json_string.values()[counter].split(',')
        except:
            dict[key] = json_string.values()[counter]

    #print dict

    return dict


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
    os.system(delete_string)

def delete_project(name):
    delete_string = str('/usr/local/Proasis2/utils/removeoldproject.py -p ' + str(name))
    os.system(delete_string)


def delete_all_inhouse(exception_list=['Zitzmann', 'Ali', 'CMGC_Kinases']):
    all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

    json_string_projects = get_json(all_projects_url)
    dict_projects = dict_from_string(json_string_projects)

    all_projects = dict_projects['ALLPROJECTS']

    for project in all_projects:
        project_name = str(project['project'])
        print project_name
        if project_name in exception_list:
            continue
        else:
            strucids = get_strucids_from_project(str(project_name))
            for strucid in strucids:
                print strucid
                delete_structure(strucid)
            delete_project(project_name)

def get_struc_mtz(strucid, out_dir):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/listfiles/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    for entry in file_dict['allfiles']:
        if 'STRUCFACMTZFILE' in entry['filetype']:
            filename = str(entry['filename'])
        elif 'STRUCFACMTZFILE' not in entry['filetype']:
            # print('NO MTZ file found for ' + str(strucid))
            filename = None
    if filename:
        os.system('cp ' + filename + ' ' + out_dir)
        mtz_zipped = filename.split('/')[-1]
        os.system('gzip -d ' + out_dir + '/' + mtz_zipped)
        saved_to = str(mtz_zipped.replace('.gz',''))
    else:
        saved_to = None

    return saved_to

def get_struc_pdb(strucid, outfile):
    url = str('http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/fetchfile/originalpdb/' + strucid)
    json_string = get_json(url)
    file_dict = dict_from_string(json_string)
    if os.path.isfile(outfile):
        os.system('rm ' + outfile)

    os.system('touch ' + outfile)

    with open(outfile, 'a') as f:
        for line in file_dict['output']:
            f.write(line)
    return outfile

def run_edstats(strucid):
    os.system('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh')
    if not os.path.isdir('temp'):
        os.mkdir('temp')
    os.chdir('temp')
    mtz_file = get_struc_mtz(strucid, '.')
    if mtz_file:
        pdb_file = get_struc_pdb(strucid, str(strucid + '.pdb'))
        os.system('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh; '
                  'edstats.pl -hklin=' + mtz_file + ' -xyzin=' + pdb_file + ' -out=edstats.out')
    else:
        pdb_file = None
        print('No mtz file found for ' + strucid + ' so not running edstats!')

    return mtz_file, pdb_file

