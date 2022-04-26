from django.urls import path
from django.conf.urls import url, include

from trading.views import index

app_name = 'trading'

urlpatterns = [
    path('', index, name='index'),
]
