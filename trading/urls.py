from django.urls import path
from django.conf.urls import url, include, re_path

from trading.views import combined_stats


urlpatterns = [
    path('', combined_stats, name='index'),
    path('/summary', combined_stats, name='trading'),
]
