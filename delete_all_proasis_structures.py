import requests
import pandas
from rdkit import Chem
from rdkit.Chem import AllChem


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

    return dict


def get_strucids_from_project(project):
    url = str("http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projectlookup/" + project +
              "?strucsource=inh&idonly=1")

    json_string_strucids = get_json(url)
    dict_strucids = dict_from_string(json_string_strucids)

    strucids = list(dict_strucids['strucids'])

    return strucids


get_all_projects_string = '''curl -H 'Accept: text/plain' --data '{"username":"uzw12877","password":"uzw12877"}' 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/' '''


all_projects_url = 'http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projects/'

json_string_projects = get_json(all_projects_url)
dict_projects = dict_from_string(json_string_projects)

all_projects = dict_projects['ALLPROJECTS']

for project in all_projects:
    project_name = project['project']

    if 'Ali' in project_name or 'Zitzmann' in project_name or 'CMGC_Kinases' in project_name:
        continue
    else:
        print project_name

        print get_strucids_from_project(project_name)