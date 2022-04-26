from django.http import HttpResponse
from django.shortcuts import render
from trading.models import Account, Order, Fund, Position


def index_trading(request):
    """View function for home page of site."""

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
    return render(request, 'index.html', context=context)