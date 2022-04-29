from django.http import HttpResponse
from django.db.models import Sum
from django.shortcuts import render
from django.template.response import TemplateResponse
from trading.models import Account, Order, Fund, Position
from django.views import generic
from django.shortcuts import get_object_or_404
from trading.tables import OrderTable
from django_tables2 import SingleTableMixin, LazyPaginator
from django.utils import timezone
from datetime import timedelta, datetime


class HomePage(generic.TemplateView):
    """
    Because our needs are so simple, all we have to do is
    assign one value; template_name. The home.html file will be created
    in the next lesson.
    """
    template_name = 'home.html'


class AccountListView(generic.ListView):
    model = Account
    paginate_by = 10
    context_object_name = 'accounts_list'
    template_name = 'trading/accounts.html'

    def get_queryset(self):
        return Account.objects.filter(active=True)


class AccountDetailView(SingleTableMixin, generic.DetailView):
    model = Account

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        orders = Order.objects.filter(account=self.object)

        table = OrderTable(Order.objects.filter(account=self.object).order_by('-dt_create'))
        table.paginate(page=self.request.GET.get("page", 1), per_page=10)
        table.paginator_class = LazyPaginator
        table.localize=True

        now = timezone.now()
        last_24h = now - timedelta(days=1)

        context['table'] = table
        context['owner'] = self.object.owner
        context['assets_value'] = round(self.object.assets_value(), 2)
        context['has_position'] = self.object.has_opened_short()
        context['positions_pnl'] = round(self.object.positions_pnl(), 2)
        context['orders_open'] = orders.filter(status='open')
        context['orders_canceled'] = orders.filter(status='canceled').filter(dt_update__range=(last_24h, now))
        context['orders_error'] = orders.filter(status='error').filter(dt_update__range=(last_24h, now))
        return context

#
# def trading_stats(request):
#
#     # Generate counts of some main objects
#     num_accounts = Account.objects.all().count()
#     num_open_orders = Order.objects.filter(status='open').count()
#     num_closed_orders = Order.objects.filter(status='closed').count()
#     num_canceled_orders = Order.objects.filter(status='canceled').count()
#
#     context = {
#         'num_accounts': num_accounts,
#         'num_open_orders': num_open_orders,
#         'num_closed_orders': num_closed_orders,
#         'num_canceled_orders': num_canceled_orders,
#     }
#
#     # Render the HTML template index.html with the data in the context variable
#     return TemplateResponse(request, 'home.html', context=context)
#
#
# def combined_stats(request):
#     response1 = trading_stats(request)
#     response2 = marketsdata_stats(request)
#     response3 = strategy_stats(request)
#     return render(request, 'home.html', {
#         **response1.context_data,
#         **response2.context_data,
#         **response3.context_data
#     })


