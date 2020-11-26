from rest_framework import viewsets

from .serializers import *


class TargetView(viewsets.ReadOnlyModelViewSet):
    """
    Target: target names
    Filters:
        - target_name
    Returns:
        - target_name
    """
    queryset = Target.objects.filter()
    serializer_class = TargetSerializer
    filter_fields = (
        "target_name",
    )


class CompoundsView(viewsets.ReadOnlyModelViewSet):
    """
    Compounds: smiles strings for compounds
    """
    queryset = Compounds.objects.filter()
    serializer_class = CompoundsSerializer
    filter_fields = (
        "smiles",
    )


class ReferenceView(viewsets.ReadOnlyModelViewSet):
    """
    Reference: reference pdb files
    """
    queryset = Reference.objects.filter()
    serializer_class = ReferenceSerializer
    filter_fields = (
        "reference_pdb",
    )


class SoakdbFilesView(viewsets.ReadOnlyModelViewSet):
    queryset = SoakdbFiles.objects.filter()
    serializer_class = SoakdbFilesSerializer
    filter_fields = (
        "filename",
        "proposal__proposal",
        "visit",
    )


class CrystalView(viewsets.ReadOnlyModelViewSet):
    queryset = Crystal.objects.filter()
    serializer_class = CrystalSerializer
    filter_fields = (
        "crystal_name",
        "target__target_name",
        "compound__smiles",
        "visit__filename",
        "visit__proposal__proposal",
        "visit__visit",
    )


class DataProcessingView(viewsets.ReadOnlyModelViewSet):
    queryset = DataProcessing.objects.filter()
    serializer_class = DataProcessingSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
    )


class DimpleView(viewsets.ReadOnlyModelViewSet):
    queryset = Dimple.objects.filter()
    serializer_class = DimpleSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "reference__reference_pdb"
    )


class LabView(viewsets.ReadOnlyModelViewSet):
    queryset = Lab.objects.filter()
    serializer_class = LabSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "data_collection_visit",
        "library_name",
        "library_plate",
    )


class RefinementView(viewsets.ReadOnlyModelViewSet):
    queryset = Refinement.objects.filter()
    serializer_class = RefinementSerializer
    filter_fields = (
        "crystal_name__crystal_name",
        "crystal_name__target__target_name",
        "crystal_name__compound__smiles",
        "crystal_name__visit__filename",
        "crystal_name__visit__proposal__proposal",
        "crystal_name__visit__visit",
        "outcome",
    )


class PanddaAnalysisView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaAnalysis.objects.filter()
    serializer_class = PanddaAnalysisSerializer
    filter_fields = (
        "pandda_dir",
    )


class PanddaRunView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaRun.objects.filter()
    serializer_class = PanddaRunSerializer
    filter_fields = (
        "input_dir",
        "pandda_analysis__pandda_dir",
        "pandda_log",
        "pandda_version",
        "sites_file",
        "events_file",
    )


class PanddaSiteView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaSite.objects.filter()
    serializer_class = PanddaSiteSerializer
    filter_fields = (
        "pandda_run__pandda_analysis__pandda_dir",
        "pandda_run__pandda_log",
        "pandda_run__pandda_sites_file",
        "pandda_run__pandda_events_file",
        "pandda_run__input_dir",
        "site",
    )


class PanddaEventView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaEvent.objects.filter()
    serializer_class = PanddaEventSerializer
    filter_fields = (
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        "pandda_run__pandda_analysis__pandda_dir",
        "pandda_run__pandda_log",
        "pandda_run__pandda_sites_file",
        "pandda_run__pandda_events_file",
        "pandda_run__input_dir",
        "site__site",
        "event",
        "lig_id",
        "pandda_event_map_native",
        "pandda_model_pdb",
        "pandda_input_mtz",
        "pandda_input_pdb",
    )


class FragspectCrystalView(viewsets.ReadOnlyModelViewSet):
    queryset = PanddaEvent.objects.filter().prefetch_related('crystal', 'site', 'refinement', 'data_proc')
    serializer_class = FragspectCrystalSerializer
    filter_fields = {'crystal__target__target_name': ['iexact']}


class MiscFilesView(viewsets.ReadOnlyModelViewSet):
    queryset = MiscFiles.objects.filter()
    serializer_class = MiscFilesSerializer
    filter_fields = (
        'file',
        'description'
    )


class FragalysisTargetView(viewsets.ReadOnlyModelViewSet):
    queryset = FragalysisTarget.objects.filter()
    serializer_class = FragalysisTargetSerializer
    filter_fields = (
        'open',
        'target',
        'metadata_file',
        'input_root',
        'staging_root',
        'biomol',
        'additional_files__file',
        'additional_files__description'
    )


class FragalysisLigandView(viewsets.ReadOnlyModelViewSet):
    queryset = FragalysisLigand.objects.filter()
    serializer_class = FragalysisLigandSerializer
    filter_fields = (
        'ligand_name',
        'fragalysis_target__open',
        'fragalysis_target__target',
        'crystallographic_bound',
        'lig_mol_file',
        'apo_pdb',
        'bound_pdb',
        'smiles_file',
        'desolvated_pdb',
        'solvated_pdb',
        'pandda_event',
        'two_fofc',
        'fofc',
        'modification_date'
    )


class LigandView(viewsets.ReadOnlyModelViewSet):
    queryset = Ligand.objects.filter()
    serializer_class = LigandSerializer
    filter_fields = (
        'fragalysis_ligand__ligand_name',
        'fragalysis_ligand__fragalysis_target__open',
        'fragalysis_ligand__fragalysis_target__target',
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        'target__target_name',
        'compound__smiles'
    )

class ReviewResponsesView(viewsets.ReadOnlyModelViewSet):
    queryset = ReviewResponses.objects.filter()
    serializer_class = ReviewResponsesSerializer
    filter_fields = (
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        'fedid',
        'decision_int',
        'decision_str',
        'reason',
        'time_submitted'
    )

class ReviewResponses2View(viewsets.ReadOnlyModelViewSet):
    queryset = ReviewResponses2.objects.filter()
    serializer_class = ReviewResponses2Serializer
    filter_fields = (
        "crystal__crystal_name",
        "crystal__target__target_name",
        "crystal__compound__smiles",
        "crystal__visit__filename",
        "crystal__visit__proposal__proposal",
        "crystal__visit__visit",
        'Ligand_name__fragalysis_ligand__ligand_name',
        'Ligand_name__fragalysis_ligand__fragalysis_target__open',
        'Ligand_name__fragalysis_ligand__fragalysis_target__target',
        'fedid',
        'decision_int',
        'decision_str',
        'reason',
        'time'
    )

class BadAtomsView(viewsets.ReadOnlyModelViewSet):
    queryset = BadAtoms.objects.filter()
    serializer_class = BadAtomsSerializer
    filter_fields = (
        'review',
        'Ligand__fragalysis_ligand__ligand_name',
        'Ligand__fragalysis_ligand__fragalysis_target__open',
        'Ligand__fragalysis_ligand__fragalysis_target__target',
        'comment'
    )

class MetaDataView(viewsets.ReadOnlyModelViewSet):
    queryset = MetaData.objects.filter()
    serializer = MetaDataSerializer
    filter_fields = (
        'Ligand_name__ligand_name',
        'Ligand_name__fragalysis_target__open',
        'Ligand_name__fragalysis_target__target',
        'Site_Label',
        'new_smiles',
        'alternate_name',
        'pdb_id',
        'fragalysis_name',
        'original_name'

    )