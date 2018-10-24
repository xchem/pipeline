# pull base container with conda environment 'pipeline' in it:
# github = xchem/django-luigi-docker (linked to autobuild on docker hub)
FROM reskyner/django-luigi-docker

# use bash from now on
SHELL ["/bin/bash", "-c"]

# expose 5432 for postgres, and 8082 for luigi
EXPOSE 5432
EXPOSE 8082

RUN touch /coverage.xml

RUN adduser postgres
RUN chown postgres /coverage.xml
#USER postgres

# create a 'pipeline' directory, and add everything from the current repo into it
RUN mkdir /pipeline
RUN chown -R /pipeline postgres
USER postgres
WORKDIR /pipeline
COPY . /pipeline/
RUN chmod 777 /pipeline/run_services.sh

# add a new user (postgres for database, but could be changed if changed in settings'
# RUN adduser postgres

# change permissions on all files needed (tmp needed by postgres later)
# RUN chown -R postgres /pipeline/
RUN mkdir /tmp
# RUN chown -R postgres /tmp

# move django settings to correct file name
RUN mv settings_docker_django.py settings.py

# change to postgres user and make sure we start in the pipeline directory
# USER postgres
WORKDIR /pipeline
