from django.http import HttpResponse
from django.shortcuts import render
from django.template.response import TemplateResponse
from marketsdata.views import marketsdata_stats
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
    return render('index.html', {
        **response1.context_data,
        **response2.context_data
    })