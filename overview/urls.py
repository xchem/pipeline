from django.urls import path
from . import views

urlpatterns = [
    path(r'targets/', views.targets, name='targets'),
    path(r'targets/$', views.get_graph, name='graph')
]
