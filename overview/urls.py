from django.urls import path

from . import views

urlpatterns = [
    path(r'targets/', views.targets, name='targets'),
    path(r'targets/graph/', views.get_graph, name='get_graph'),
    path(r'targets/mod_date/', views.get_update_times, name='get_update_times'),
    path(r'targets/crystal_summary/', views.get_crystal_info, name='get_crystal_info')
]
