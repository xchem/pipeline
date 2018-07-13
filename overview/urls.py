from django.urls import path
from . import views

urlpatterns = [
    path('<str:target>/', views.crystals_from_target, name='crystals')
]
