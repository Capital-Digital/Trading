from django.http import HttpResponse
from django.shortcuts import render
from django.template.response import TemplateResponse
from marketsdata.models import Market, Exchange, Currency
from django.views import generic


class ExchangeListView(generic.ListView):
    model = Exchange
    paginate_by = 10
    context_object_name = 'exchanges_list'
    template_name = 'exchanges.html'

    def get_queryset(self):
        return Exchange.objects.all()


class MarketListView(generic.ListView):
    model = Market
    paginate_by = 10
    context_object_name = 'markets_list'
    template_name = 'markets.html'

    def get_queryset(self):
        return Market.objects.all()


class CurrencyListView(generic.ListView):
    model = Currency
    paginate_by = 10
    context_object_name = 'currencies_list'
    template_name = 'currencies.html'

    def get_queryset(self):
        return Currency.objects.all()


# def marketsdata_stats(request):
#
#     # Generate counts of some main objects
#     num_exchanges = Exchange.objects.count()
#     num_currencies = Currency.objects.count()
#     num_spot_markets = Market.objects.filter(type='spot').count()
#     num_derivative_markets = Market.objects.filter(type='derivative').count()
#
#     context = {
#         'num_exchanges': num_exchanges,
#         'num_currencies': num_currencies,
#         'num_spot_markets': num_spot_markets,
#         'num_derivative_markets': num_derivative_markets,
#     }
#
#     # Render the HTML template index.html with the data in the context variable
#     return TemplateResponse(request, 'exchanges.html', context=context)