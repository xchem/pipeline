#!/bin/bash
source ~/.bashrc
source activate pipeline
PYTHONPATH='.'
initdb db_files
nohup luigid >/dev/null 2>&1 &
pg_ctl -D db_files -l logfile start
createdb test_xchem
python manage.py makemigrations xchem_db
python manage.py migrate xchem_db

