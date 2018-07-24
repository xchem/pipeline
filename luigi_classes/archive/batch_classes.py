import luigi
import functions.db_functions as db_functions
from luigi_classes.archive import data_in_proasis, database_operations
from luigi_classes.data_out_proasis import *
import functions.docking_functions as dock


class StartLeadTransfers(luigi.Task):

    def get_list(self):
        path_list = []
        protein_list = []
        reference_list = []
        conn, c = db_functions.connectDB()
        c.execute(
            "SELECT pandda_path, protein, reference_pdb FROM proasis_leads WHERE pandda_path !='' and pandda_path !='None' and reference_pdb !='' and reference_pdb !='None' ")
        rows = c.fetchall()
        for row in rows:
            # if not os.path.isfile(str('logs/leads/' + str(row[1]) + '_' + misc_functions.get_mod_date(str(row[1])) + '.added')):
            path_list.append(str(row[0]))
            protein_list.append(str(row[1]))
            reference_list.append(str(row[2]))

        out_list = list(zip(path_list, protein_list, reference_list))

        return out_list

    def requires(self):
        try:
            run_list = self.get_list()

            return database_operations.FindProjects(), database_operations.CheckFiles(), \
                   [data_in_proasis.LeadTransfer(pandda_directory=path, name=protein, reference_structure=reference)
                    for (path, protein, reference) in run_list], database_operations.FindProjects()
        except:
            return database_operations.FindProjects()

    def output(self):
        return luigi.LocalTarget('logs/leads.done')

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


class StartLigandSearches(luigi.Task):
    def requires(self):
        conn, c = db_functions.connectDB()
        exists = db_functions.column_exists('proasis_hits', 'ligand_list')
        if not exists:
            conn, c = db_functions.connectDB()
            c.execute('ALTER TABLE proasis_hits ADD COLUMN ligand_list text;')
            conn.commit()
        c.execute("select bound_conf from proasis_hits where ligand_list is NULL and bound_conf is not NULL")

        rows = c.fetchall()
        conf_list = []
        for row in rows:
            conf_list.append(str(row[0]))
            print((str(row[0])))
        return database_operations.FindProjects(), database_operations.CheckFiles(), [
            data_in_proasis.FindLigands(bound_conf=conf) for conf in conf_list]

    def output(self):
        return luigi.LocalTarget('logs/ligand_search.done')

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


class StartHitTransfers(luigi.Task):
    hit_directory = luigi.Parameter(default='/dls/science/groups/proasis/LabXChem')

    def get_list(self):
        bound_list = []

        crystal_list = []
        protein_list = []
        smiles_list = []
        modification_list = []
        ligand_list = []

        conn, c = db_functions.connectDB()
        c.execute("SELECT bound_conf, crystal_name, protein, smiles, modification_date, ligand_list, exists_2fofc, exists_fofc, exists_pdb, exists_mtz FROM proasis_hits WHERE modification_date not like '' and ligand_list not like 'None' and bound_conf not like ''")
        rows = c.fetchall()
        for row in rows:
            if '0' in [str(row[6]), str(row[7]), str(row[8]), str(row[9])]:
                continue
            #if not os.path.isfile(str('./hits/' + str(row[1]) + '_' + str(row[4]) + '.added')):
            bound_list.append(str(row[0]))
            crystal_list.append(str(row[1]))
            protein_list.append(str(row[2]))
            smiles_list.append(str(row[3]))
            modification_list.append(str(row[4]))
            ligand_list.append(str(row[5]))

        run_list = list(zip(bound_list, crystal_list, protein_list, smiles_list, modification_list, ligand_list))
        return run_list

    def requires(self):
        exists = db_functions.column_exists('proasis_hits', 'ligand_list')
        if not exists:
            try:
                conn, c = db_functions.connectDB()
                c.execute('ALTER TABLE proasis_hits ADD COLUMN ligand_list text;')
                conn.commit()
                return StartLigandSearches()
            except:
                pass

        try:
            run_list = self.get_list()
            return database_operations.FindProjects(), database_operations.CheckFiles(), StartLigandSearches(), [
                data_in_proasis.HitTransfer(bound_pdb=pdb, crystal=crystal_name,
                                            protein_name=protein_name, smiles=smiles_string,
                                            mod_date=modification_string, ligands=ligand_list) for
                (pdb, crystal_name, protein_name, smiles_string, modification_string, ligand_list) in run_list], database_operations.FindProjects()
        except:
            return data_in_proasis.CleanUpHits()

    def output(self):
        return luigi.LocalTarget('logs/hits.done')

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


class FindCompChemReady(luigi.Task):
    def requires(self):
        run_list = dock.get_comp_chem_ready()
        out_dict = dock.get_strucids(run_list)

        return [PullMol(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [PullMtz(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [Pull2Fofc(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [CreateApo(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])], \
               [PullFofc(strucid=strucid, root_dir=root_dir, crystal=crystal, ligands=ligands) for
                (strucid, root_dir, crystal, ligands) in
                zip(out_dict['strucid'], out_dict['directory'], out_dict['crystal'], out_dict['ligands'])]

    def output(self):
        return luigi.LocalTarget(os.path.join('logs', 'findcc.done'))

    def run(self):
        with self.output().open('wb') as f:
            f.write('')


