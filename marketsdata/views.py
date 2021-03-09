from django.http import HttpResponse

from .models import Market


def index(request):
    markets = Market.objects.order_by('market')[:5]
    market = ', '.join([q.market for q in markets])
    return HttpResponse(market)
