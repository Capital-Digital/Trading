from django.contrib import admin
from django.urls import include, path
from django.conf.urls import include, url
from trading.models import Account
import structlog
from graphene_django.views import GraphQLView
from django.views import generic

log = structlog.get_logger(__name__)

urlpatterns = [

    path('admin/', admin.site.urls),
    path('/', include('marketsdata.urls')),
    path('strategies/', include('strategy.urls')),

    path("account/", include("django.contrib.auth.urls")),
    path('', generic.TemplateView.as_view(template_name='home.html'), name='home'),
    path("graphql", GraphQLView.as_view(graphiql=True)),
]


admin.site.site_header = 'Admin Dashboard'
admin.autodiscover()
admin.site.enable_nav_sidebar = False
