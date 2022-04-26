from django.contrib import admin
from django.urls import include, path
from django.conf.urls import include, url
from trading.models import Account
import structlog
from graphene_django.views import GraphQLView

log = structlog.get_logger(__name__)

urlpatterns = [

    path('admin/', admin.site.urls),
    url(r"^", include("trading.urls")),

    path('markets/', include('marketsdata.urls')),
    path('strategies/', include('strategy.urls')),
    path('accounts/', include('trading.urls')),

    path("graphql", GraphQLView.as_view(graphiql=True)),
]


admin.site.site_header = 'Admin Dashboard'
admin.autodiscover()
admin.site.enable_nav_sidebar = False
