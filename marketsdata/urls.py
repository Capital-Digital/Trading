from django.urls import path

from marketsdata.views import marketsdata_stats


urlpatterns = [
    path('', marketsdata_stats, name='index'),
    path('/markets', marketsdata_stats, name='markets'),
]
