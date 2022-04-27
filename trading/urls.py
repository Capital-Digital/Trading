from django.urls import path
from django.conf.urls import url, include

from trading.views import AccountDetailView, AccountListView


urlpatterns = [
    path('list/', AccountListView.as_view(), name='accounts'),
    path('<int:pk>', AccountDetailView.as_view(), name='account'),
]
