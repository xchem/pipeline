from django.urls import path
from . import views

urlpatterns = [
    path(r'targets/', views.targets, name='crystals')
]
