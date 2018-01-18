import luigi, os
import data_in_proasis

class KickOff(luigi.Task):

    def run(self):
        os.system('./pg_backup.sh')
        os.system('rm hits.done')
        os.system('rm leads.done')
        os.system('rm transfer.txt')
        os.system('rm hits.done')
        os.system('rm findprojects.done')
        os.system('rm blacklist.done')

        return data_in_proasis.WriteBlackLists()