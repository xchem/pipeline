FROM continuumio/miniconda3

RUN apt-get update && apt-get install -y libxrender-dev

# Python packages from conda environment.yml file
ADD environment.yml /tmp/environment.yml
RUN chmod 777 /tmp/environment.yml

# mkdir for database files
RUN mkdir database/
RUN mkdir database/db_files

# Add pipeline user and change permissions
RUN adduser postgres
RUN chown postgres database/
RUN chown postgres database/db_files
WORKDIR /database
COPY run_services.sh .
RUN chown postgres run_services.sh
RUN chmod 777 run_services.sh

# add settings file for django
COPY settings_docker_django.py .
RUN chown postgres settings_docker_django.py
RUN chmod 777 settings_docker_django.py

# Run the rest of the commands as the ``postgres`` user
USER postgres

# add conda to bashrc
RUN echo 'export PATH="/opt/conda/bin:$PATH"' >> ~/.bashrc
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc

# source bashrc
RUN /bin/bash -c "source ~/.bashrc"

# create conda env from environment.yml
ADD environment.yml /tmp/environment.yml
WORKDIR /tmp
RUN conda env create
WORKDIR /database

#RUN /bin/bash -c "source activate pipeline; initdb db_files"

# Start postgres
EXPOSE 5432
EXPOSE 8082

# Git pull pipeline
RUN git clone https://github.com/xchem/pipeline.git
RUN chmod -R 777 pipeline/
WORKDIR pipeline/
RUN cp ../settings_docker_django.py settings.py
RUN chmod 777 settings.py

WORKDIR /database
CMD /bin/bash -c "source activate pipeline"

