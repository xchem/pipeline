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

# Running in container locally to test things

Once you're in the container, you can run a local luigi daemon so that you can run luigi tasks to test them.

To start up the luigi daemon in the docker container:

```nohup luigid >/dev/null 2>&1 &``` 

You can then run any task you wish, providing you have the data you need. There should be some example data in `tests/data`.

## Example for soakdb files tasks

### 1. Getting some example data together

First, we need a soakdb file to use as an example. In `tests/data/soakdbfiles` there is a json file containing some data
 that can be converted into an sqlite file of the correct format. We can use `make_sdbfile.py` to do it: `python make_sbdfile.py`
 
 We can look in the `testa/test_transfer_soakdb.py` file to see how data is usually set up for running tests for luigi in CI/CD.
 
 ```python
class TestTransferSoakDBTasks(unittest.TestCase):
    # filepath where test data is (in docker container) and filenames for soakdb
    filepath = '/pipeline/tests/data/soakdb_files/'
    db_filepath = '/pipeline/tests/data/processing/database/'
    db_file_name = 'soakDBDataFile.sqlite'
    json_file_name = 'soakDBDataFile.json'

    # date for tasks
    date = datetime.datetime.now()

    # variables to check
    findsoakdb_outfile = date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')
    transfer_outfile = date.strftime('logs/transfer_logs/fedids_%Y%m%d%H.txt')
    checkfiles_outfile = date.strftime('logs/checked_files/files_%Y%m%d%H.checked')

    @classmethod
    def setUpClass(cls):
        # remove any previous soakDB file
        if os.path.isfile(os.path.join(cls.filepath, cls.db_file_name)):
            os.remove(os.path.join(cls.filepath, cls.db_file_name))

        # initialise db and json objects
        cls.db = os.path.join(cls.filepath, cls.db_file_name)
        json_file = json.load(open(os.path.join(cls.filepath, cls.json_file_name)))

        # write json to sqlite file
        conn = sqlite3.connect(cls.db)
        df = pandas.DataFrame.from_dict(json_file)
        df.to_sql("mainTable", conn, if_exists='replace')
        conn.close()

        cls.modification_date = get_mod_date(os.path.join(cls.filepath, cls.db_file_name))
        print(str('mdate: ' + cls.modification_date))


        # create log directories
        os.makedirs('/pipeline/logs/soakDBfiles')
        os.makedirs('/pipeline/logs/transfer_logs')
        os.makedirs('/pipeline/tests/data/processing/database/')

        shutil.copy(cls.db, f"/pipeline/tests/data/processing/database/{cls.db.split('/')[-1]}")

        cls.db = os.path.join('/pipeline/tests/data/processing/database/', cls.db_file_name)

        cls.newfile_outfile = str(cls.db + '_' + str(cls.modification_date) + '.transferred')
``` 

And then look at the specific task we want to run locally, to see what variables we need to give to the task for it to 
run. For example, if I want to test transferring a new soakdb file to XCDB using luigi in my local dev env 
(docker container), we can look at `luigi_classes/transfer_soakdb.py`:

```python
class TransferNewDataFile(luigi.Task):
    """Luigi Class to transfer changed files into XCDB
    Requires :class:`CheckFiles` to be completed

    :param luigi.Task: Task representation containing necessary input to perform function
    :type luigi.Task: :class:`luigi.task.Task`
    """

    resources = {'django': 1}
    data_file = luigi.Parameter()
    soak_db_filepath = luigi.Parameter(default=SoakDBConfig().default_path)

...
```

We can see that we need a path for `data_file`, which is the soakdb file we created earlier from the .json file, and a 
`soak_db_filepath` which is the filepath that the upstream `FindSoakDBFiles(luigi.Task)` task uses to search for soakdb 
files. From the existing tests for CI/CD, we can use the same filepath used in `db_filepath`. We just need to copy our 
newly generated soakdb file there:

```
mkdir /pipeline/tests/data/lb13385-1/processing/database/
cp /pipeline/tests/data/soakdb_files/soakDBDataFile.sqlite /pipeline/tests/data/lb13385-1/processing/database/soakDBDataFile.sqlite
```

And now we have the two variables we need to test the `TransferNewDataFile(luigi.Task)` task:

```
data_file = '/pipeline/tests/data/lb13385-1/processing/database/soakDBDataFile.sqlite'
soak_db_filepath = '/pipeline/tests/data/lb13385-1/processing/database/'
```
### 2. Setting up database (only need to do this once each time you're in the container)

Run `./run_services.sh`

### 3. Running a luigi task locally
We started a luigi daemon ages ago. Now. we can run a task using the local scheduler. The docs are here: 
https://luigi.readthedocs.io/en/stable/running_luigi.html. Using the `TransferNewDataFile(luigi.Task)` task as an 
example, we would run:

```
PYTHONPATH='.' luigi --module luigi_classes.transfer_soakdb TransferNewDataFile \
--data-file='/pipeline/tests/data/lb13385-1/processing/database/soakDBDataFile.sqlite' \
--soak-db-filepath='/pipeline/tests/data/lb13385-1/processing/database/' \
--local-scheduler
```

Note that we're providing the `PYTHONPATH` variable as the local `pipeline` directory that we're working, to give the 
scheduler context of where to look for the `luigi_classes` modules. We're also changing `_` to `-` in our variable names 
for the task.