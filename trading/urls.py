from django.urls import path
from django.conf.urls import url, include

from trading.views import AccountDetailView, AccountListView


urlpatterns = [
    path('/', AccountListView.as_view(), name='trading_accounts'),
    path('/<int:pk>', AccountDetailView.as_view(), name='trading_account'),
]
