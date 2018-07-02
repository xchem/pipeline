


class FindProjects(luigi.Task):

    def requires(self):
        pass
        # return CheckFiles(), StartTransfers()

    def output(self):
        return luigi.LocalTarget('logs/findprojects.done')

    def run(self):
        # all data necessary for uploading hits
        hits_dict = {'crystal_name': [], 'protein': [], 'smiles': [], 'bound_conf': [],
                                  'modification_date': [], 'strucid':[]}

        # all data necessary for uploading leads
        leads_dict = {'protein': [], 'pandda_path': [], 'reference_pdb': [], 'strucid':[]}

        # class ProasisHits(models.Model):
        #     bound_pdb = models.ForeignKey(Refinement, to_field='bound_conf', on_delete=models.CASCADE, unique=True)
        #     crystal_name = models.ForeignKey(Crystal, on_delete=models.CASCADE)  # changed to foreign key
        #     modification_date = models.TextField(blank=True, null=True)
        #     strucid = models.TextField(blank=True, null=True)
        #     ligand_list = models.IntegerField(blank=True, null=True)

        ref_or_above = Refinement.objects.filter(outcome__in=[3, 4, 5, 6])

        for entry in ref_or_above:
            print(entry.crystal_name.crystal_name)
            if entry.bound_conf:
                print(entry.bound_conf)
            else:
                print(entry.pdb_latest)


        # lab_table_select = Lab.objects.filter(crystal_name=ref_or_above.crystal_name)
        #
        # for row in rows:
        #
        #     c.execute('''SELECT smiles, protein FROM lab WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     lab_table = c.fetchall()
        #
        #     if len(str(row[0])) < 3:
        #         continue
        #
        #     if len(lab_table) > 1:
        #         print(('WARNING: ' + str(row[0]) + ' has multiple entries in the lab table'))
        #         # print lab_table
        #
        #     for entry in lab_table:
        #         if len(str(entry[1])) < 2 or 'None' in str(entry[1]):
        #             protein_name = str(row[0]).split('-')[0]
        #         else:
        #             protein_name = str(entry[1])
        #
        #
        #         crystal_data_dump_dict['protein'].append(protein_name)
        #         crystal_data_dump_dict['smiles'].append(entry[0])
        #         crystal_data_dump_dict['crystal_name'].append(row[0])
        #         crystal_data_dump_dict['bound_conf'].append(row[1])
        #         crystal_data_dump_dict['strucid'].append('')
        #
        #         try:
        #             modification_date = misc_functions.get_mod_date(str(row[1]))
        #
        #         except:
        #             modification_date = ''
        #
        #         crystal_data_dump_dict['modification_date'].append(modification_date)
        #
        #     c.execute('''SELECT pandda_path, reference_pdb FROM dimple WHERE crystal_id = %s''', (str(row[0]),))
        #
        #     pandda_info = c.fetchall()
        #
        #     for pandda_entry in pandda_info:
        #         project_data_dump_dict['protein'].append(protein_name)
        #         project_data_dump_dict['pandda_path'].append(pandda_entry[0])
        #         project_data_dump_dict['reference_pdb'].append(pandda_entry[1])
        #         project_data_dump_dict['strucid'].append('')
        #
        # project_table = pandas.DataFrame.from_dict(project_data_dump_dict)
        # crystal_table = pandas.DataFrame.from_dict(crystal_data_dump_dict)
        #
        # protein_list = set(list(project_data_dump_dict['protein']))
        # print(protein_list)
        #
        # for protein in protein_list:
        #
        #     self.add_to_postgres(project_table, protein, ['reference_pdb'], project_data_dump_dict, 'proasis_leads')
        #
        #     self.add_to_postgres(crystal_table, protein, ['crystal_name', 'smiles', 'bound_conf'],
        #                          crystal_data_dump_dict, 'proasis_hits')
        #
        # with self.output().open('wb') as f:
        #     f.write('')














