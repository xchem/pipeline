#!/bin/bash

source ~/.bashrc
source activate pipeline
initdb db_files
nohup luigid >/dev/null 2>&1 &
pg_ctl -D db_files -l logfile start
createdb test_xchem
python manage.py makemigrations
python manage.py migrate
coverage run -m unittest -v tests/test_transfer_soakdb.py tests/test_pandda_upload.py
coverage xml -o coverage.xml
./cc-test-reporter after-build -p /pipeline -t coverage.py -r a26c0fd57d06e8e2dd6bb51e00550f80d786a791ef308b6e850a87b95f03d041
