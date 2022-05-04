from django.http import HttpResponse
from django.db.models import Sum
from django.shortcuts import render
from django.template.response import TemplateResponse
from capital.methods import *
from trading.models import Account, Order, Position, Asset, Stat
from django.views import generic
from django.shortcuts import get_object_or_404
from trading.tables import OrderTable, AssetTable, PositionTable, ReturnTable
from django_tables2 import SingleTableMixin, LazyPaginator
from django.utils import timezone
from datetime import timedelta, datetime
from plotly.offline import plot
import plotly.graph_objects as go
from marketsdata.models import Tickers
from capital.methods import get_year, get_semester
import numpy as np


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

        # Create tables
        table_asset = AssetTable(Asset.objects.filter(account=self.object, total_value__gte=1).order_by('wallet'))
        table_position = PositionTable(Position.objects.filter(account=self.object).order_by('market__symbol'))
        table_order = OrderTable(Order.objects.filter(account=self.object).order_by('-dt_created'))

        # Configure pagination
        table_order.paginate(page=self.request.GET.get("page", 1), per_page=10)
        table_order.paginator_class = LazyPaginator
        table_order.localize = True

        # Create chart
        stats = Stat.objects.get(account=self.object, strategy=self.object.strategy)
        btcusdt = Tickers.objects.get(market__symbol='BTC/USDT',
                                      market__type='spot',
                                      market__exchange__exid='binance',
                                      year=get_year(),
                                      semester=get_semester()
                                      )
        ethusdt = Tickers.objects.get(market__symbol='ETH/USDT',
                                      market__type='spot',
                                      market__exchange__exid='binance',
                                      year=get_year(),
                                      semester=get_semester()
                                      )
        acc_val = json_to_df(stats.metrics)['acc_val']
        btcusdt = json_to_df(btcusdt.data)['last']
        btcusdt = btcusdt.loc[acc_val.index]
        ethusdt = json_to_df(ethusdt.data)['last']
        ethusdt = ethusdt.loc[acc_val.index]

        ret_acc = acc_val.pct_change(1)
        ret_btc = btcusdt.pct_change(1)
        ret_eth = ethusdt.pct_change(1)

        # Data for line chart
        # acc_line = np.exp(np.log1p(ret_acc).cumsum())
        # btc_line = np.exp(np.log1p(ret_btc).cumsum())
        acc_line = ((1 + ret_acc).cumprod() - 1).fillna(0) * 100
        btc_line = ((1 + ret_btc).cumprod() - 1).fillna(0) * 100
        eth_line = ((1 + ret_eth).cumprod() - 1).fillna(0) * 100

        chart_returns = [go.Line(x=acc_line.index.tolist(),
                                 y=acc_line.squeeze().values.tolist(),
                                 name='Account',
                                 line=dict(width=2)),
                         go.Line(x=btc_line.index.tolist(),
                                 y=btc_line.squeeze().values.tolist(),
                                 name='BTC/USDT',
                                 line=dict(width=2)),
                         go.Line(x=eth_line.index.tolist(),
                                 y=eth_line.squeeze().values.tolist(),
                                 name='ETH/USDT',
                                 line=dict(width=2))
                         ]

        yaxis = dict(title='Balance')
        yaxis2 = dict(title='Bitcoin',
                      overlaying='y',
                      side='right')

        layout = {
            'yaxis_title': 'Return (%)',
            'height': 520,
            'width': 1100,
            'plot_bgcolor': "#f8f9fa",
            # 'title_text': "Double Y Axis Example",
            # 'yaxis': yaxis,
            # 'yaxis2': yaxis2
        }

        plot_div_1 = plot({'data': chart_returns, 'layout': layout}, output_type='div',)

        # Data for the table
        acc_1h = round(ret_acc, 2).dropna()[::-1].squeeze().tolist()
        acc_24h = round(acc_val.pct_change(24) * 100, 2)[::-1].squeeze().tolist()
        acc_7d = round(acc_val.pct_change(24 * 7) * 100, 2)[::-1].squeeze().tolist()
        dt = ret_acc.index.strftime(datetime_directive_s).tolist()
        dic = [{"Datetime": c0, "ret_1h": c1, "ret_24h": c2, "ret_7d": c3} for c0, c1, c2, c3 in zip(dt, acc_1h, acc_24h, acc_7d)]

        # Table of returns
        table_returns = ReturnTable(dic)

        context['plot_div_1'] = plot_div_1
        context['table_asset'] = table_asset
        context['table_position'] = table_position
        context['table_order'] = table_order
        context['table_returns'] = table_returns
        context['last_update'] = stats.dt_modified.strftime(datetime_directive_literal) + ' UTC'
        context['owner'] = self.object.owner
        context['assets_value'] = round(self.object.assets_value(), 2)
        context['has_position'] = self.object.has_opened_short()
        context['positions_pnl'] = round(self.object.positions_pnl(), 2)
        context['orders_open'] = orders.filter(status='open')
        return context
