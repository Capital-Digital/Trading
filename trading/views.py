from django.http import HttpResponse
from django.shortcuts import render
from django.template.response import TemplateResponse
from marketsdata.views import marketsdata_stats
from strategy.views import strategy_stats
from trading.models import Account, Order, Fund, Position


def trading_stats(request):

    # Generate counts of some main objects
    num_accounts = Account.objects.all().count()
    num_open_orders = Order.objects.filter(status='open').count()
    num_closed_orders = Order.objects.filter(status='closed').count()
    num_canceled_orders = Order.objects.filter(status='canceled').count()

    context = {
        'num_accounts': num_accounts,
        'num_open_orders': num_open_orders,
        'num_closed_orders': num_closed_orders,
        'num_canceled_orders': num_canceled_orders,
    }

    # Render the HTML template index.html with the data in the context variable
    return TemplateResponse(request, 'index.html', context=context)


def combined_stats(request):
    response1 = trading_stats(request)
    response2 = marketsdata_stats(request)
    response3 = strategy_stats(request)
    return render(request, 'index.html', {
        **response1.context_data,
        **response2.context_data,
        **response3.context_data
    })


def list_accounts(request):

    # Generate counts of some main objects
    active_accounts = Account.objects.filter(active=True).values_list('pk', flat=True)
    paused_accounts = Account.objects.filter(active=False).values_list('pk', flat=True)

    context = {
        'active_accounts': active_accounts,
        'paused_accounts': paused_accounts,
    }

    # Render the HTML template index.html with the data in the context variable
    return render(request, 'accounts.html', context=context)