import luigi
import os
import subprocess


class CutOutEvent(luigi.Task):
    ssh_command = 'ssh uzw12877@nx.diamond.ac.uk'
    directory = luigi.Parameter()
    mapin = luigi.Parameter()
    mapout = luigi.Parameter()
    xyzin = luigi.Parameter()


    def run(self):
        mapmask = '''module load ccp4 && mapmask mapin %s mapout %s xyzin %s << eof
            border 12
            end
        eof
        ''' % (self.mapin, self.mapout, self.xyzin)

        process = subprocess.Popen(str(self.ssh_command + '"' + 'cd ' + self.directory + ';' + mapmask + '"'),
                                   shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = process.communicate()
        print(out)
        print(err)
