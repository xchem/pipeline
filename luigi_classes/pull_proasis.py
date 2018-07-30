import luigi
import setup_django


class GetCurated(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateApo(luigi.Task):

    def requires(self):
        return GetCurated()

    def output(self):
        pass

    def run(self):
        pass


class GetMaps(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class GetSDF(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateMolFile(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateMolTwoFile(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateHMol(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateNoBufAltLocs(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass


class CreateMolFiles(luigi.Task):

    def requires(self):
        return GetSDF()

    def output(self):
        pass

    def run(self):
        pass


class GetInteractionJSON(luigi.Task):

    def requires(self):
        pass

    def output(self):
        pass

    def run(self):
        pass