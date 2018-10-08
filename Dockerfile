FROM continuumio/miniconda3

# Python packages from conda
RUN conda install -y -c rdkit rdkit
RUN conda install -y -c anaconda pandas
RUN conda install -y -c anaconda luigi
RUN conda install -y -c anaconda numpy
RUN conda install -y -c anaconda psycopg2
RUN conda install -y -c anaconda postgresql
RUN conda install -y -c anaconda django
RUN conda install -y -c conda-forge django-extensions

#RUN echo 'export PATH="/opt/conda/bin:$PATH"' >> ~/.bashrc
#RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc
#RUN /bin/bash -c "source ~/.bashrc"
#ADD environment.yml /tmp/environment.yml
#WORKDIR /tmp
#RUN conda env create
#RUN /bin/bash -c "source activate pipeline"

# mkdir for database files
RUN mkdir database/
RUN mkdir database/db_files

# Add pipeline user
RUN adduser postgres
RUN chown postgres database/
RUN chown postgres database/db_files
WORKDIR /database
COPY run_services.sh .
RUN chown postgres run_services.sh
RUN chmod 777 run_services.sh

COPY settings_docker_django.py .
RUN chown postgres settings_docker_django.py
RUN chmod 777 settings_docker_django.py

# Run the rest of the commands as the ``postgres`` user
USER postgres
#RUN echo 'export PATH="/opt/conda/bin:$PATH"' >> ~/.bashrc
#RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc
#RUN /bin/bash -c "source ~/.bashrc"
#RUN /bin/bash -c "source activate pipeline"

# Start postgres
RUN initdb db_files
EXPOSE 5432
EXPOSE 8082

# Git pull pipeline
RUN git clone https://github.com/xchem/pipeline.git
WORKDIR /database/pipeline/
RUN cp ../settings_docker_django.py settings.py
RUN chmod 777 settings.py

WORKDIR /database
CMD ./run_services.sh

# start luigid
# RUN nohup luigid >/dev/null 2>&1 &
