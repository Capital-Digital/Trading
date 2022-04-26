from django.urls import path
from django.conf.urls import url, include

from trading.views import combined_stats, list_accounts, info_account, AccountListView


urlpatterns = [
    path('', combined_stats, name='index'),
    path('list/', AccountListView.as_view(), name='accounts-list'),
    path('accounts/<int:pk>', AccountListView.as_view(), name='account-detail'),
]
