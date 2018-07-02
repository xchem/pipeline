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

