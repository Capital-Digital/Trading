from django.http import HttpResponse
from django.db.models import Sum
from django.shortcuts import render
from django.template.response import TemplateResponse
from trading.models import Account, Order, Fund, Position, Asset
from django.views import generic
from django.shortcuts import get_object_or_404
from trading.tables import OrderTable, AssetTable, PositionTable
from django_tables2 import SingleTableMixin, LazyPaginator
from django.utils import timezone
from datetime import timedelta, datetime
from plotly.offline import plot
import plotly.graph_objects as go
from marketsdata.models import Tickers
from capital.methods import get_year, get_semester


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

        table_asset = AssetTable(Asset.objects.filter(account=self.object).order_by('wallet'))
        table_position = PositionTable(Position.objects.filter(account=self.object).order_by('market__symbol'))

        table_order = OrderTable(Order.objects.filter(account=self.object).order_by('-dt_created'))
        table_order.paginate(page=self.request.GET.get("page", 1), per_page=10)
        table_order.paginator_class = LazyPaginator
        table_order.localize = True

        now = timezone.now()
        last_24h = now - timedelta(hours=6)

        # Account historical balance
        data_assets_hist = []
        assets = self.object.stats.get().assets_value_history
        data = [assets[k]['balance'] for k in assets.keys()]
        bench = Tickers.objects.get(market__symbol='BTC/USDT',
                                    market__exchange__exid='binance',
                                    market__type='spot', year=get_year(), semester=get_semester())
        x = [k for k in assets.keys()]
        bench = [bench.data[k]['last'] for k in bench.data.keys() if k in x]

        data_assets_hist.append(go.Scatter(x=x, y=data, name='Balance', line=dict(color='#a9a9a9', width=2)))
        data_assets_hist.append(go.Scatter(x=x, y=bench, name='Bitcoin', line=dict(color='#ff8c00', width=2)))

        layout_weights = {
            'yaxis_title': 'Account balance',
            'height': 520,
            'width': 1100,
            'plot_bgcolor': "#f8f9fa"
        }
        plot_div_1 = plot({'data': data_assets_hist, 'layout': layout_weights},
                          output_type='div',
                          )

        context['plot_div_1'] = plot_div_1
        context['table_asset'] = table_asset
        context['table_position'] = table_position
        context['table_order'] = table_order
        context['owner'] = self.object.owner
        context['assets_value'] = round(self.object.assets_value(), 2)
        context['has_position'] = self.object.has_opened_short()
        context['positions_pnl'] = round(self.object.positions_pnl(), 2)
        context['orders_open'] = orders.filter(status='open')
        context['orders_canceled'] = orders.filter(status='canceled').filter(dt_modified__range=(last_24h, now))
        context['orders_error'] = orders.filter(status='error').filter(dt_modified__range=(last_24h, now))
        return context
