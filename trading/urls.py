from django.urls import path
from django.conf.urls import url, include
from trading.views import *


urlpatterns = [
    path('', views.index, name='index'),
]
#
#
# urlpatterns = [
#     # path('', index, name='index'),
#     path("", dashboard, name="dashboard"),
#
#     url(r"^oauth/", include("social_django.urls")),
#     url(r"^register/", register, name="register"),
#     url(r"^login/", login, name='login'),
#
#     path("see_request/", see_request),
#     path("user_info/", user_info),
#     path("private_place/", private_place),
#     path("accounts/", include("django.contrib.auth.urls")),
#     path("add_messages/", add_messages),
#
# ]