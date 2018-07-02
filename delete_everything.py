import setup_django
from db.models import *
import os
import glob
import pandas as pd

soakdb_rows = SoakdbFiles.objects.all()

for row in soakdb_rows:
    path = str(row.filename + '_' + str(row.modification_date) + '.transferred')
    if not os.path.isfile(path):
        print(path)
    else:
        os.remove(path)


list_of_files = glob.glob('logs/search_paths_*')
latest_file = max(list_of_files, key=os.path.getctime)

search_paths = [path for path in pd.DataFrame.from_csv(latest_file)['search_path']]

for path in search_paths:
    if os.path.isfile(os.path.join(path, 'transfer_pandda_data.done')):
        os.remove(os.path.join(path, 'transfer_pandda_data.done'))
    files_to_check = glob.glob(os.path.join(path,'*201806*'))
    for f in files_to_check:
        if 'txt' in f:
            os.remove(f)

pandda_runs = PanddaRun.objects.all()
logfiles = [run.pandda_log for run in pandda_runs]

for logfile in logfiles:
    if os.path.isfile(logfile + '.run.done'):
        print(str(logfile + '.run.done'))
        os.remove(logfile + '.run.done')
    if os.path.isfile(logfile + '.events.done'):
        print(str(logfile + '.events.done'))
        os.remove(logfile + '.events.done')
    if os.path.isfile(logfile + '.sites.done'):
        print(str(logfile + '.sites.done'))
        os.remove(logfile + '.sites.done')




