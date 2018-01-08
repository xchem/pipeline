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

    return dict


def get_strucids_from_project(project):
    url = str("http://cs04r-sc-vserv-137.diamond.ac.uk/proasisapi/v1.4/projectlookup/" + project +
              "?strucsource=inh&idonly=1")

    json_string_strucids = get_json(url)
    dict_strucids = dict_from_string(json_string_strucids)

    strucids = list(dict_strucids['strucids'])

    return strucids

def delete_structure(strucid):
    delete_string = str('/usr/local/Proasis2/utils/removestruc.py -s ' + str(strucid))
    os.system(delete_string)