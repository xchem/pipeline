import luigi


class VerneConfig(luigi.Config):
    username = luigi.Parameter()
    hostname = luigi.Parameter()
    remote_root = luigi.Parameter()
