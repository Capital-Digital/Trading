from django.http import HttpResponse
from django.db.models import Sum
from django.shortcuts import render
from django.template.response import TemplateResponse
from marketsdata.views import marketsdata_stats
from strategy.views import strategy_stats
from trading.models import Account, Order, Fund, Position
from django.views import generic
from django.shortcuts import get_object_or_404


class AccountListView(generic.ListView):
    model = Account
    paginate_by = 10
    context_object_name = 'accounts_list'
    template_name = 'accounts.html'

    def get_queryset(self):
        return Account.objects.all()


class AccountDetailView(generic.DetailView):
    model = Account


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
    active_accounts = Account.objects.filter(active=True)
    paused_accounts = Account.objects.filter(active=False)

    context = {
        'active_accounts': active_accounts,
        'paused_accounts': paused_accounts,
    }

    # Render the HTML template index.html with the data in the context variable
    return render(request, 'accounts.html', context=context)


def info_account(request, account_id):

    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account)
    orders_open = orders.filter(status='open')
    orders_closed = orders.filter(status='closed')
    orders_canceled = orders.filter(status='canceled')
    trade_total = orders_closed.aggregate(Sum('cost'))['cost__sum']

    context = {
        'orders': orders,
        'orders_open': orders_open,
        'orders_closed': orders_closed,
        'orders_canceled': orders_canceled,
        'account_value': account.account_value(),
        'account_strategy_name': account.strategy.name,
        'account_name': account.name,
        'trade_total': trade_total
    }

    # Render the HTML template index.html with the data in the context variable
    return render(request, 'account.html', context=context)
