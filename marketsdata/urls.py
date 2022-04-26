from django.urls import path

from marketsdata.views import index


urlpatterns = [
    path('', index, name='index'),
]
