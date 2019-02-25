[![Build Status](https://travis-ci.org/xchem/pipeline.svg?branch=master)](https://travis-ci.org/xchem/pipeline)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/xchem/pipeline.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/xchem/pipeline/context:python)

## For developers...

### Changing models

There are some basic tests that run tasks from the pipeline and populate data into my existing tables, although these won't test your own models. 

You will need to test manually the migration of your models into an empty postgres database. 

There is a docker container that you can build, based on the Dockerfile in this repo. From within the cloned repo:

1. docker build -t pipeline .
2. docker run --user root -it pipeline /bin/bash
3. source activate pipeline
4. python manage.py makemigrations
5. python manage.py migrate

If, in step 4 or 5, any errors are thrown, you will need to troubleshoot based on those errors, before the code makes it into the master branch. 

I will update my TravisCI tests to make sure the integration tests fail if the migrate tasks listed above don't work. 
