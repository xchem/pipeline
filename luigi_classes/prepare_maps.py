import luigi


class CutOutEvent(luigi.Task):
    ssh_command = 'ssh uzw12877@nx.diamond.ac.uk'
    mapin = luigi.Parameter()
    mapout = luigi.Parameter()
    xyzin = luigi.Parameter()


    def run(self):
        mapmask = '''mapmask mapin %s mapout %s xyzin %s << eof
            border 12
            end
        eof
        ''' % (self.mapin, self.mapout, self.xyzin)

        print(mapmask)
