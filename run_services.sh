#!/bin/bash
source ~/.bashrc
source activate pipeline
initdb db_files
nohup luigid >/dev/null 2>&1 &
pg_ctl -D db_files -l logfile start -A
createdb test_xchem
python manage.py makemigrations db
python manage.py migrate db

