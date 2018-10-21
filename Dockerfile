# pull base container with conda environment 'pipeline' in it:
# github = xchem/django-luigi-docker (linked to autobuild on docker hub)
FROM reskyner/django-luigi-docker

# use bash from now on
SHELL ["/bin/bash", "-c"]

# incase /tmp doesn't exist - make it
RUN mkdir /tmp

# expose 5432 for postgres, and 8082 for luigi
EXPOSE 5432
EXPOSE 8082

# create a 'pipeline' directory, and add everything from the current repo into it
RUN mkdir /pipeline
WORKDIR /pipeline
COPY . /pipeline/
RUN chmod 777 /pipeline/run_services.sh

# add a new user (postgres for database, but could be changed if changed in settings'
RUN adduser postgres

# change permissions on all files needed (tmp needed by postgres later)
RUN chown -R postgres /pipeline/
RUN chown -R postgres /tmp

# move django settings to correct file name
RUN mv settings_docker_django.py settings.py

# change to postgres user and make sure we start in the pipeline directory
USER postgres
WORKDIR /pipeline
