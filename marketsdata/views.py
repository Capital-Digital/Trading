from django.http import HttpResponse
from django.shortcuts import render
from django.template.response import TemplateResponse
from marketsdata.models import Market, Exchange, Currency


def marketsdata_stats(request):

    # Generate counts of some main objects
    num_exchanges = Exchange.objects.count()
    num_currencies = Currency.objects.count()
    num_spot_markets = Market.objects.filter(type='spot').count()
    num_derivative_markets = Market.objects.filter(type='derivative').count()

    context = {
        'num_exchanges': num_exchanges,
        # 'num_currencies': num_currencies,
        # 'num_spot_markets': num_spot_markets,
        # 'num_derivative_markets': num_derivative_markets,
    }

    # Render the HTML template index.html with the data in the context variable
    return TemplateResponse(request, 'index.html', context=context)