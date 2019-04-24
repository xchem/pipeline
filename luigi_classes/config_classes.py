import luigi


class VerneConfig(luigi.Config):
    username = luigi.Parameter()
    hostname = luigi.Parameter()
    remote_root = luigi.Parameter()
    target_list = luigi.Parameter()
    update_user = luigi.Parameter()
    update_token = luigi.Parameter()
    rand_string = luigi.Parameter()
    open_target_list = luigi.Parameter()


class SentryConfig(luigi.Config):
    key = luigi.Parameter()
    ident = luigi.Parameter()


class SoakDBConfig(luigi.Config):
    # "/dls/labxchem/data/*/lb*/*"
    default_path = luigi.Parameter()


class DirectoriesConfig(luigi.Config):
    # '/dls/science/groups/proasis/LabXChem/'
    hit_directory = luigi.Parameter()
    # '/dls/science/groups/i04-1/software/luigi_pipeline/pipelineDEV/logs/'
    log_directory = luigi.Parameter()
