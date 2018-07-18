from django.urls import path
from . import views

urlpatterns = [
    path(r'targets/', views.targets, name='targets'),
    path(r'targets/graph/', views.get_graph, name='get_graph')
]
