from django.urls import path

from marketsdata.views import marketsdata_stats


urlpatterns = [
    path('exchanges', marketsdata_stats, name='exchanges'),
]
