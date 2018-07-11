from rest_framework.routers import DefaultRouter
import db.views as db_views


router = DefaultRouter()
# Register the basic data
router.register(r"target", db_views.TargetView)