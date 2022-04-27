from django.urls import path

from marketsdata.views import marketsdata_stats


urlpatterns = [
    path('exchanges', marketsdata_stats, name='exchanges'),
    path('markets', marketsdata_stats, name='markets'),
    path('currencies', marketsdata_stats, name='currencies'),
]
