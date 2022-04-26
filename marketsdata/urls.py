from django.urls import path

from marketsdata.views import index

app_name = 'marketsdata'

urlpatterns = [
    path('', index, name='index'),
]
