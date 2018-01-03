import os, datetime

def get_id_string(out):
    """
    Regex function for finding proasis strucid
    """
    try:
        strucidstr = re.search(r"strucid='.....'", out)
        strucidstr = strucidstr.group()
        strucidstr = strucidstr.replace('strucid=', '')
        strucidstr = strucidstr.replace("'", '')
    except:
        strucidstr = ''
    return strucidstr

def get_mod_date(filename):
    modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(filename)).strftime(
                        "%Y-%m-%d %H:%M:%S")
    modification_date = modification_date.replace('-', '')
    modification_date = modification_date.replace(':', '')
    modification_date = modification_date.replace(' ', '')

    return modification_date