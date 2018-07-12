from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter
from rest_framework.authtoken import views as drf_views
from db.views import TargetView


router = DefaultRouter()
# Register the basic data
router.register(r'target', TargetView)

urlpatterns = [
    url(r"^", include(router.urls)),
    url(r"^auth$", drf_views.obtain_auth_token, name="auth"),
]