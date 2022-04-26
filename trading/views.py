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

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        orders = Order.objects.filter(account=context.id)
        orders_closed = orders.filter(status='closed')

        context['orders'] = orders
        context['orders_closed'] = orders_closed
        context['orders_open'] = orders.filter(status='open')
        context['orders_canceled'] = orders.filter(status='canceled')
        context['orders_closed'] = orders_closed.aggregate(Sum('cost'))['cost__sum']
        return context


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


