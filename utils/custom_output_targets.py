import luigi
from django.apps import apps
from django.core.exceptions import FieldError, MultipleObjectsReturned


class DjangoFieldTarget(luigi.target.Target):
    def __init__(self, app, model, field, value):
        self.app = app
        self.model = model
        self.field = field
        self.value = value

    def exists(self):
        model = apps.get_model(app_label=self.app, model=self.model)
        try:
            objs = model.filter(**{self.field:self.value})
            if len(objs) > 0:
                return True
            else:
                return False
        except FieldError:
            return False


class DjangoTaskTarget(luigi.target.Target):
    def __init__(self, class_name, uuid, app_label='xchem_db'):
        self.class_name = class_name
        self.uuid = uuid
        self.app_label = app_label

    def exists(self):
        model = apps.get_model(app_label=self.app_label, model='tasks')
        try:
            model.get(**{'task_name': self.class_name, 'uuid':self.uuid})
        except DoesNotExist:
            return False
        except MultipleObjectsReturned:
            raise Exception('multiple tasks matching this output found!')

        return True