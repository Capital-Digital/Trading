from django.urls import path
from django.conf.urls import url, include

from trading.views import combined_stats, AccountDetailView, AccountListView


urlpatterns = [
    path('', combined_stats, name='home'),
    path('accounts/', AccountListView.as_view(), name='accounts'),
    path('accounts/<int:pk>', AccountDetailView.as_view(), name='account'),
]
