from django.urls import path
from django.conf.urls import url, include

from trading.views import combined_stats, list_accounts, info_account


urlpatterns = [
    path('', combined_stats, name='index'),
    path('list', list_accounts, name='accounts'),
    path('', info_account, name='account'),
]
