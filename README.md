[![Build Status](https://travis-ci.org/xchem/pipeline.svg?branch=master)](https://travis-ci.org/xchem/pipeline)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/xchem/pipeline.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/xchem/pipeline/context:python)

## For developers...

### Changing models

There are some basic tests that run tasks from the pipeline and populate data into my existing tables, although these won't test your own models. 

You will need to test manually the migration of your models into an empty postgres database. 

There is a docker container that you can build, based on the Dockerfile in this repo. 

On local machine, from within the cloned repo::
```
docker build --no-cache -t pipeline .
# Optional, for debugging purposes. Mount the git repo into the docker container
repo=$(pwd)
docker run --mount type=bind,source=$repo,target=/pipeline -it pipeline /bin/bash 
```
Within the docker container:
```
source activate pipeline
python manage.py makemigrations
python manage.py migrate
```

If, during the `python manage.py makemigrations` or `python manage.py migrate` errors are thrown you need to troubleshoot 
based on these errors before the code will be successfully pushed into the master branch. Hence why we mount the repo 
when testing so we can edit the python code without rebuilding the docker container.

I will update my TravisCI tests to make sure the integration tests fail if the migrate tasks listed above don't work. 
