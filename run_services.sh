#!/bin/bash
source ~/.bashrc
conda activate pipeline
initdb db_files
nohup luigid >/dev/null 2>&1 &
pg_ctl -D db_files -l logfile start
createdb test_xchem
cd pipeline
python manage.py makemigrations db
python manage.py migrate db

