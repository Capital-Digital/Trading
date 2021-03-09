from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('marketsdata/', include('marketsdata.urls')),
    path('strategy/', include('strategy.urls')),
    path('admin/', admin.site.urls),
]

admin.site.site_header = 'Admin Dashboard'


