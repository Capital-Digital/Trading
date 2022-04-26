from django.urls import path
from django.conf.urls import url, include

from trading.views import trading_stats


urlpatterns = [
    path('', trading_stats, name='index'),
]
