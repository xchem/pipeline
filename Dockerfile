FROM reskyner/django-luigi-docker

SHELL ["/bin/bash", "-c"]

# Git pull pipeline
RUN git clone https://github.com/xchem/pipeline.git
RUN chmod -R 777 pipeline/
WORKDIR pipeline/

RUN adduser pipeline

# add settings file for django
#COPY settings_docker_django.py .
RUN chown pipeline settings_docker_django.py
RUN chmod 777 settings_docker_django.py
RUN mv settings_docker_django.py settings.py
#RUN chmod 777 settings.py
#COPY run_services.sh .
#RUN chown postgres run_services.sh
#RUN chmod 777 run_services.sh

# mkdir for database files
RUN mkdir database/
RUN mkdir database/db_files
#RUN chown postgres database/
#RUN chown postgres database/db_files

# Run the rest of the commands as the 'postgres' user
USER pipeline
WORKDIR /pipeline
#ENV PATH /opt/conda/envs/pipeline
RUN ./run_services.sh

