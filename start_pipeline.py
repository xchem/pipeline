import luigi, os
import data_in_proasis

class KickOff(luigi.Task):

    def requires(self):
        os.system('/usr/local/Proasis2/utils/createrefalign.py')
        os.system('./pg_backup.sh')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/hits.done')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/leads.done')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/transfer.txt')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/hits.done')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/findprojects.done')
        os.system('rm /dls/science/groups/i04-1/software/luigi_pipeline/blacklist.done')

        return data_in_proasis.WriteBlackLists()