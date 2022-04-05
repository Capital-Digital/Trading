from django.http import HttpResponse

from .models import Market


def index(request):
    return HttpResponse()
