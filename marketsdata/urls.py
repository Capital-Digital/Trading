from django.urls import path
from marketsdata.views import ExchangeListView, MarketListView, CurrencyListView


urlpatterns = [
    path('exchanges/', ExchangeListView.as_view(), name='exchanges'),
    path('markets/', MarketListView.as_view(), name='markets'),
    path('currencies/', CurrencyListView.as_view(), name='currencies'),
]
