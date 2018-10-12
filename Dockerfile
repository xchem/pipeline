FROM reskyner/django-luigi-docker

SHELL ["/bin/bash", "-c"]

EXPOSE 5432
EXPOSE 8082

RUN mkdir /pipeline
RUN chmod -R 777 /pipeline
WORKDIR /pipeline
ADD /api/ /pipeline/api/
ADD /functions/ /pipeline/functions/
ADD /luigi_classes/ /pipeline/luigi_classes
ADD /overview/ /pipeline/overview/
ADD /tests/ /pipeline/tests/
COPY run_services.sh .
COPY manage.py .
COPY settings_docker_django.py .
COPY setup_django.py .
COPY urls.py .
COPY wsgi.py .

RUN adduser postgres

# add settings file for django
RUN chown postgres settings_docker_django.py
RUN chmod 777 settings_docker_django.py
RUN mv settings_docker_django.py settings.py
RUN chmod 777 settings.py
RUN chown postgres run_services.sh
RUN chmod 777 run_services.sh

# mkdir for database files
RUN mkdir database/
RUN mkdir database/db_files
RUN chown postgres database/
RUN chown postgres database/db_files
RUN chmod 777 /tmp

# Run the rest of the commands as the 'postgres' user
USER postgres
WORKDIR /pipeline
