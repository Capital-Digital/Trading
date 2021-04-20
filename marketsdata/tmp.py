from IPython.core.display import display, HTML

display(HTML("<style>.container { width:120% !important; }</style>"))

df = pd.DataFrame()
account = Account.objects.get(name='Binance_Test')
import nest_asyncio

nest_asyncio.apply()

print(account)
print(account.strategy.name)


# return cumulative orderbook
def cumulative_book(ob):
    asks = ob['asks']
    bids = ob['bids']
    asks_p = [a[0] for a in asks]
    bids_p = [a[0] for a in bids]
    cum_a = list(accumulate([a[1] for a in asks]))
    cum_b = list(accumulate([a[1] for a in bids]))
    return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]


# Set the best ask and bids
def set_markets_bid_ask(code, quote, default_type, symbol, bids, asks):
    df_markets.loc[(code, quote, default_type, symbol), ('price', 'bid')] = bids[0][0]
    df_markets.loc[(code, quote, default_type, symbol), ('price', 'ask')] = asks[0][0]


# Calculate the best price available for the desired quantity and it's distance to best bid/ask
def calculate_distance(depth, amount):
    # Iterate through depth until desired amount is available
    for i, b in enumerate(depth):

        if b[1] > amount:
            if i == 0:
                return depth[0][0], 0
            else:
                depth = depth[:i]  # select the first n elements needed
                break

    # select prices and sum total quantity needed
    prices = [p[0] for p in depth]
    qty = sum([q[1] for q in depth])

    # weight each element and multiply prices by weights and sum
    weights = [q[1] / qty for q in depth]
    best_price = sum([a * b for a, b in zip(prices, weights)])

    # Calculate distance in % to the best bid or to the best ask
    distance = abs(100 * (best_price / depth[0][0] - 1))

    return best_price, distance


# Calculate bid-ask spread
def calculate_spread(bids, asks):
    spread = asks[0][0] - bids[0][0]
    spread_pct = spread / asks[0][0]

    return spread_pct * 100


# Calculate the cost of trading through a route from a source market and a destination market
def calculate_cost(code, quote, default_type, symbol, bids, asks):
    # If the current market match a currency help in the account
    if any(df_account.index.isin([(code, default_type)])):

        # Create a boolean mask to test if market is a source or a destination of an existing route
        indexes_1 = (df_routes.index.get_level_values(1) == symbol) & (
                    df_routes.index.get_level_values(2) == default_type)
        indexes_2 = (df_routes.index.get_level_values(4) == symbol) & (
                    df_routes.index.get_level_values(5) == default_type)

        # Select value of delta, exposure and spot price
        delta = df_account.loc[(code, default_type), ('target', 'delta_usd')]
        expos = df_account.loc[(code, default_type), ('exposure', 'value')]
        delta_qty = df_account.loc[(code, default_type), ('target', 'delta')]
        spot = df_account.loc[(code, default_type), ('price')][0]

        # Should we adresse the bids (sell coins, open a short)
        # or should we addresse the asks (buy coins, open a long) ?
        if delta > 0:
            depth = bids
        else:
            depth = asks

        # Determine absolute value to buy (if long) or sell (if short)
        delta = abs(delta)

        # Select funding rate
        funding = df_markets.loc[(code, quote, default_type, symbol), ('funding', 'rate')][0]

        # Iterate through routes for which market is among source markets
        for indice in [i for i, index in enumerate(indexes_1) if index]:

            # select route
            route = df_routes.index[indice]
            route_type = df_routes.loc[route, ('route', 'type')]

            # Something to trade or market could be a hedge ?
            if not np.isnan(delta) or route_type == 'hedge':

                # Then determine value to trade in destination market
                delta_dest = df_account.loc[(route[3], route[5]), ('target', 'delta_usd')]
                delta_dest = abs(delta_dest)

                print('\nvalue to trade in source', symbol, default_type, delta)
                print('value to trade in destination market', delta_dest)

                # Finally determine maximum value to trade through this route
                delta = min(delta, delta_dest)

                print('value to trade', delta)

                # Convert trade value from US. Dollar to currency amount
                amount = delta / spot

                print('amount to trade', amount, '\n')

                # Get best price and distance % for the desired amount
                best_price, distance = calculate_distance(bids, amount)
                spread = calculate_spread(bids, asks)
                cost = distance + spread

                df_routes.loc[route, ('source', 'amount')] = amount
                df_routes.loc[route, ('source', 'distance')] = distance
                df_routes.loc[route, ('source', 'best price')] = best_price
                df_routes.loc[route, ('source', 'spread')] = spread
                df_routes.loc[route, ('source', 'cost')] = cost
                df_routes.loc[route, ('source', 'quantity %')] = abs(amount / delta_qty)

            else:
                df_routes.drop([route], inplace=True)
                return

        # Iterate through routes for which market is among destination markets
        for indice in [i for i, index in enumerate(indexes_2) if index]:

            # select route and route type
            route = df_routes.index[indice]
            route_type = df_routes.loc[route, ('route', 'type')]

            # Determine the max value to sell (if long) or buy (if short position) in the source market
            delta_sour = df_account.loc[(route[0], route[2]), ('target', 'delta_usd')]
            expos_sour = df_account.loc[(route[0], route[2]), ('exposure', 'value')]

            # Raise an exception currency should be bought in source market
            if expos_sour > 0 and delta_sour < 0:
                raise Exception('Currency should be bought in source market')

            # If a short position should be closed in source market
            elif expos_sour < 0 and delta_sour < 0:
                delta_sour = abs(max(delta_sour, expos_sour))

            # If currency should be sold in source market
            elif expos_sour > 0 and delta_sour > 0:
                delta_sour = min(delta_sour, expos_sour)

            # Check if there is something to sell in the source market or market could be a hedge ?
            if not np.isnan(delta_sour) or route_type == 'hedge':

                # Then determine maximum value to trade through this route
                delta = min(delta, delta_sour)

                # Convert value from US. Dollar to currency amount
                amount = delta / spot

                # Get best price and distance % for the desired amount
                best_price, distance = calculate_distance(asks, amount)
                spread = calculate_spread(bids, asks)
                cost = distance + spread

                # Set NaN if direct or hedge trade
                if route_type in ['direct', 'hedge']:
                    distance, spread, cost, funding_rate = [np.nan for i in range(4)]
                else:
                    funding_rate = funding

                df_routes.loc[route, ('destination', 'amount')] = amount
                df_routes.loc[route, ('destination', 'distance')] = distance
                df_routes.loc[route, ('destination', 'best price')] = best_price
                df_routes.loc[route, ('destination', 'spread')] = spread
                df_routes.loc[route, ('destination', 'cost')] = cost
                df_routes.loc[route, ('destination', 'quantity %')] = abs(amount / delta_qty)
                df_routes.loc[route, ('destination', 'funding')] = funding_rate

            else:
                df_routes.drop([route], inplace=True)
                return

                # Iterate through rows and sum cost
        if 'source' in df_routes.droplevel('second', axis=1).columns:
            for index, route in df_routes.iterrows():
                if not route.empty:
                    if not np.isnan(route['source']['cost']):

                        # Set cost = source cost if direct or hedge trade
                        if route['route']['type'] in ['direct', 'hedge']:
                            df_routes.loc[index, ('route', 'cost')] = route['source']['cost']

                        # Else cost = source cost + destination cost
                        elif 'destination' in df_routes.droplevel('second', axis=1).columns:

                            if not np.isnan(route['destination']['cost']):
                                # or sum cost source and destination
                                df_routes.loc[index, ('route', 'cost')] = route['source']['cost'] + \
                                                                          route['destination']['cost']

                            # print(df_routes.droplevel('code1').droplevel('code2').drop('best price', axis=1, level=1).to_string())
                            # print(df_routes.xs('distance', level='second', axis=1, drop_level=False).to_string())
                            # print(df_routes.xs('spread', level='second', axis=1, drop_level=False).to_string())
                            # print(df_routes.xs('cost', level='second', axis=1, drop_level=False).to_string())
                            # print(df_routes.xs('quantity %', level='second', axis=1, drop_level=False).to_string())

        # print(df_routes.droplevel('code1').droplevel('code2').to_string())


# Update coin quantity and value after a trade
def update_account_df(code, default_type, side, n, close):
    # Select quantity to update
    quantity = df_account.loc[(code, default_type), (default_type, 'quantity')]

    # Remove quantity if sale
    if side == 'sell':
        n = -n

    # Update coin quantity and value
    df_account.loc[(code, default_type), (default_type, 'quantity')] = quantity + n
    df_account.loc[(code, default_type), (default_type, 'value')] = (quantity + n) * close


def create_routes(df_account):
    if 'df_routes' not in locals():

        # Buy
        #####

        # Create a list of currency to long and to short
        codes_short = df_account[df_account[('target', 'quantity')] < 0].index.get_level_values('code').unique()
        codes_long = df_account[df_account[('target', 'quantity')] > 0].index.get_level_values('code').unique()

        # Create a list of currencies to buy : open long (derivative or spot) and close short (derivative)
        codes_open_long = df_account[
            (df_account[('target', 'delta')] < 0) & (df_account[('target', 'quantity')] > 0)].index.get_level_values(
            'code').unique()
        codes_close_short = df_account[
            (df_account[('target', 'delta')] < 0) & (df_account[('target', 'quantity')] < 0)].index.get_level_values(
            'code').unique()

        # Sell
        ######

        # Create a list of currencies to sell : close long (derivative) and open short (derivative)
        codes_close_long = df_account[
            (df_account[('target', 'delta')] > 0) & (df_account[('position', 'quantity')] > 0)].index.get_level_values(
            'code').unique()
        codes_open_short = df_account[
            (df_account[('target', 'delta')] > 0) & (df_account[('position', 'quantity')] < 0)].index.get_level_values(
            'code').unique()

        # Create a list of currencies we should sell in spot markets
        codes_sell_spot = df_account[
            (df_account[('target', 'delta')] > 0) & (df_account[('account', 'quantity')] > 0)].index.get_level_values(
            'code').unique()

        # Market candidates
        ###################

        # Create a list of markets to sell spot
        mk_spot_sell = [df_markets[df_markets.index.isin([sell], level='base') & df_markets.index.isin(['spot'],
                                                                                                       level='type')].index.tolist()
                        for sell in codes_sell_spot]

        # Create a list of markets to open a short
        mk_deri_open_short = [df_markets[
                                  df_markets.index.isin([short], level='base') & df_markets.index.isin(['derivative'],
                                                                                                       level='type')].index.tolist()
                              for short in codes_open_short]

        # Create a list of markets to buy spot or and open a long
        mk_spot_buy = [df_markets[df_markets.index.isin([buy], level='base') & df_markets.index.isin(['spot'],
                                                                                                     level='type')].index.tolist()
                       for buy in codes_open_long]
        mk_deri_open_long = [df_markets[
                                 df_markets.index.isin([buy], level='base') & df_markets.index.isin(['derivative'],
                                                                                                    level='type')].index.tolist()
                             for buy in codes_open_long]

        # Create a list of candidates for a hedge
        mk_deri_hedge = [df_markets[df_markets.index.isin([sell], level='base') & df_markets.index.isin(['derivative'],
                                                                                                        level='type')].index.tolist()
                         for sell in codes_sell_spot]

        # Market with a position
        ########################

        # Create a list of markets with an open position
        mk_opened_long = [i for i, p in df_positions.iterrows() if p['side'] == 'buy']
        mk_opened_short = [i for i, p in df_positions.iterrows() if p['side'] == 'sell']

        # Unnest lists and remove duplicate candidates
        mk_spot_buy = list(set(sum(mk_spot_buy, [])))
        mk_spot_sell = list(set(sum(mk_spot_sell, [])))
        mk_deri_open_short = list(set(sum(mk_deri_open_short, [])))
        mk_deri_open_long = list(set(sum(mk_deri_open_long, [])))
        mk_deri_hedge = list(set(sum(mk_deri_hedge, [])))

        print('currency to buy (open long):', list(codes_open_long))
        print('currency to buy (close short):', list(codes_close_short))
        print('currency to sell (close long):', list(codes_close_long))
        print('currency to sell (open short):', list(codes_open_short))
        print('currency to sell (spot):', list(codes_sell_spot))

        for i in mk_spot_buy:
            print('market buy spot:', i[3])

        for i in mk_spot_sell:
            print('market sell spot:', i[3])

        for i in mk_deri_open_short:
            print('market open short:', i[3])

        for i in mk_deri_open_long:
            print('market open long:', i[3])

        for i in mk_opened_long:
            print('market with position long:', i[3])

        for i in mk_opened_short:
            print('market with position short:', i[3])

        if mk_deri_hedge:
            print(mk_deri_hedge)
            for i in mk_deri_hedge:
                print('market for a hedge:', i[3])

        routes = []

        # [0] : base
        # [1] : quote
        # [2] : default_type
        # [3] : symbo
        # [4] : type
        # [5] : derivative
        # [6] : margined

        # Loop through markets where a long position is opened
        ######################################################

        for long in mk_opened_long:

            # Position should be reduced
            if long[0] in codes_close_long:

                # Margined currency is a desired currency
                if long[6] in codes_open_long + codes_close_short:
                    # [0] : market source
                    # [1] : market destination
                    # [2] : route type
                    # [3] : order type source
                    # [4] : order type destination

                    routes.append([long, None, 'direct', 'close_long', None])

                    # Loop through spot markets where a currency could be bought on spot
                for spot in mk_spot_buy:

                    # Margin currency is an intermediary
                    if long[6] == spot[1]:
                        routes.append([long, spot, 'inter', 'close_long', 'buy'])

                # Loop through derivative markets for a buy on derivative
                for deri in mk_deri_open_long:

                    # Margin currency is an intermediary
                    if long[6] == deri[6]:

                        # Markets share the same symbol and default_type
                        if long[2] == deri[2] and long[3] == deri[3]:
                            routes.append([long, None, 'direct', 'close_long', 'open_long'])

                        else:
                            routes.append([long, deri, 'inter', 'close_long', 'open_long'])

                # Loop through markets where a short could be opened
                for deri in mk_deri_open_short:

                    # If quote is an intermediary currency
                    if short[1] == deri[6]:
                        routes.append([long, deri, 'inter', 'close_long', 'open_short'])

                # Finally append close_long
                routes.append([long, None, 'direct', 'close_long', None])

        # Loop through markets where a short position is opened
        #######################################################

        for short in mk_opened_short:

            # Position should be reduced
            if short[0] in codes_close_short:

                # Margined currency is a desired currency
                if short[6] in codes_open_long + codes_close_short:
                    routes.append([short, None, 'direct', 'close_short', None])

                    # Loop through spot markets where a currency could be bought on spot
                for spot in mk_spot_buy:

                    # Margin (and quote) currency is an intermediary
                    if short[6] == spot[1]:
                        routes.append([short, spot, 'inter', 'close_short', 'buy'])

                # Loop through derivative markets for a buy on derivative
                for deri in mk_deri_open_long:

                    # Margin currency is an intermediary
                    if short[6] == deri[6]:

                        # Markets share the same symbol and default_type
                        if short[2] == deri[2] and short[3] == deri[3]:
                            routes.append([short, None, 'direct', 'close_short', 'open_long'])

                        else:
                            routes.append([short, deri, 'inter', 'close_short', 'open_long'])

                # Loop through markets where a short could be opened
                for deri in mk_deri_open_short:

                    # If quote is an intermediary currency
                    if short[1] == deri[6]:
                        routes.append([short, deri, 'inter', 'close_short', 'open_short'])

                # Finally append close_short
                routes.append([short, None, 'direct', 'close_short', None])

        # Loop through markets where the base currency should be sold
        #############################################################

        for spot_sell in mk_spot_sell:

            # Quote currency is a desired currency
            if spot_sell[1] in codes_open_long + codes_close_short:
                routes.append([spot_sell, None, 'direct', 'sell', None])

            # Loop through markets where a currency could be bought on spot
            for spot_buy in mk_spot_buy:

                # If quote is an intermediary currency
                if spot_sell[1] == spot_buy[1]:
                    routes.append([spot_sell, spot_buy, 'inter', 'sell', 'buy'])

            # Loop through markets where a long could be opened
            for deri_buy in mk_deri_open_long:

                # Quote is an intermediary currency
                if spot_sell[1] == deri_buy[6]:
                    routes.append([spot_sell, deri_buy, 'inter', 'sell', 'open_long'])

            # Loop through markets where a short could be opened
            for short in mk_deri_open_short:

                # If quote is an intermediary currency
                if spot_sell[1] == short[6]:
                    routes.append([spot_sell, short, 'inter', 'sell', 'open_short'])

            # Finally append a sell
            routes.append([spot_sell, None, 'direct', 'sell', None])

        # Search markets where a currency we should sell on spot could be hedged
        ########################################################################

        if mk_deri_hedge:

            # Loop through currency to sell
            for sell in codes_sell_spot:

                # Loop candidates for a hedge
                for deri in mk_deri_hedge:

                    # Currency can be hedged
                    if sell == deri[0]:
                        if sell == deri[6]:
                            routes.append([deri, None, 'hedge', 'open_short', None])

                            # Create dataframe
        df_routes = pd.DataFrame()
        names = ['code1', 'source', 'type', 'code2', 'destination', 'type']

        # Insert routes into dataframe
        for r in routes:
            print(r)
            # Create an index with market source and market destination
            if r[1] == None:
                # code, symbol, default_type => code, symbol, default_type,
                index = [r[0][0], r[0][3], r[0][2], r[0][0], r[0][3], r[0][2]]
            else:
                # Duplicate market source if it's a direct trade
                index = [r[0][0], r[0][3], r[0][2], r[1][0], r[1][3], r[1][2]]

            indexes = pd.MultiIndex.from_tuples([index], names=names)
            columns = pd.MultiIndex.from_product([['route'], ['type']], names=['first', 'second'])

            # Create a dataframe with route type
            df = pd.DataFrame([[r[2]]], index=indexes, columns=columns)

            # Add actions
            df.loc[indexes, ('action', 'source')] = r[3]
            df.loc[indexes, ('action', 'destination')] = r[4]

            df_routes = pd.concat([df, df_routes], axis=0)

        df_routes.sort_index(axis=0, inplace=True)
        df_routes.sort_index(axis=1, inplace=True)

        # print('\nroutes dataframe\n', df_routes.to_string())

        return df_routes


def trade():
    # Iterate through routes
    for index, route in df_routes.iterrows():

        route_type = df_routes.loc[index, ('route', 'type')]

        # Iterate through source and destination
        for i in ['source', 'destination']:

            print(index, i)

            # Select symbol and default_type
            if i == 'source':
                symbol = index[1]
                default_type = index[2]

            elif i == 'destination':
                symbol = index[4]
                default_type = index[5]

            # Select market in df_markets
            market = df_markets.iloc[(df_markets.index.get_level_values('symbol') == symbol) & (
                        df_markets.index.get_level_values('default_type') == default_type)]

            # Select order index level
            order = market['order']

            # If no open order in the market
            if np.isnan(order_src['status'][0]):
                # Select amount and action
                amount = df_routes.loc[index, (i, 'amount')]
                action = df_routes.loc[index, ('action', i)]

                market = Market.objects.get(exchange=account.exchange, symbol=symbol, default_type=default_type)
                account.create_order(market, action, amount)


async def watch_book(i, j, client, market):
    default_type = market.default_type
    symbol = market.symbol
    base = market.base
    quote = market.quote

    loop = 0

    # access df variable in a nested function
    # nonlocal df

    while True:

        try:

            # print('iteration', i, j, default_type, '\t\t', symbol)
            ob = await client.watch_order_book(symbol)  # , limit=account.exchange.orderbook_limit)

            if ob:

                loop += 1
                if loop == 5:
                    break

                # Capture current depth
                bids, asks = cumulative_book(ob)

                # Update recent market price in df_markets
                set_markets_bid_ask(base.code, quote.code, default_type, symbol, bids, asks)

                # Calculate cost for all routes to which this market belong
                calculate_cost(base.code, quote.code, default_type, symbol, bids, asks)

                # Calculate bid-ask spread in %
                # update_bid_ask_spread(base.code, quote.code, default_type, symbol, bids, asks)

                # Determine available routes and execute trades
                # on first iteration to prevent duplicate orders
                if i == j == 0:
                    print(df_routes.droplevel('code1').droplevel('code2').drop('best price',
                                                                               axis=1,
                                                                               level=1).sort_values(
                        [('route', 'cost')]).to_string())
                    trade()

                    # print(df_markets.droplevel([0, 1, 4, 5, 6]))
                    # if not all(df_markets.loc[(base.code), ('price', 'best')] != 0):

            else:
                print('wait')

            await client.sleep(5000)

        except Exception as e:
            # print('exception', str(e))
            traceback.print_exc()
            raise e  # uncomment to break all loops in case of an error in any one of them
            # break  # you can break just this one loop if it fails


async def create_client(j, loop, default_type):
    client = getattr(ccxtpro, account.exchange.exid)({'enableRateLimit': True,
                                                      'asyncio_loop': loop, })
    client.apiKey = account.api_key
    client.secret = account.api_secret

    # configure client for market
    if account.exchange.default_types:
        client.options['defaultType'] = default_type

    # Filter markets to monitor
    markets = Market.objects.filter(exchange=account.exchange,
                                    default_type=default_type,
                                    base__code__in=codes,
                                    excluded=False,
                                    active=True
                                    )

    loops = [watch_book(i, j, client, market) for i, market in enumerate(markets) if market.is_updated()]
    log.info('Watch markets for {0}'.format(account.name), default_type=default_type)

    await asyncio.gather(*loops)
    await client.close()


async def main(loop):
    # create clients
    loops = [create_client(j, loop, default_type) for j, default_type in
             enumerate(account.exchange.get_default_types())]

    await asyncio.gather(*loops)
    # await asyncio.wait([*loops, watch_direct_trades()])


# Create dataframes
df_account = account.create_dataframe_funds()
df_positions = account.create_dataframe_positions()
df_markets = pd.DataFrame()

# Create a list with codes we have exposure in
codes = [code for code, row in df_account.groupby(level=0)]
markets = Market.objects.filter(exchange=account.exchange, excluded=False, active=True)

# Create a list with codes of the new portfolio allocation
# codes_target = [code for code, row in df_account[df_account[('target', 'percent')].notna()].groupby(level=0)]

for code in codes:

    # Select markets to include in the dataframe
    for market in markets.filter(base__code=code):
        if market.is_updated():

            margined = market.margined.code if market.margined else None

            # Create multilevel columns
            indexes = pd.MultiIndex.from_tuples([(code,
                                                  market.quote.code,
                                                  market.default_type,
                                                  market.symbol,
                                                  market.type,
                                                  market.derivative,
                                                  margined
                                                  )],
                                                names=['base',
                                                       'quote',
                                                       'default_type',
                                                       'symbol',
                                                       'type',
                                                       'derivative',
                                                       'margined'
                                                       ])
            cols = pd.MultiIndex.from_product([['depth'], ['spread']])

            # Select funding rate for perp
            if market.derivative == 'perpetual':
                funding = market.funding_rate['lastFundingRate']
            else:
                funding = np.nan

            # Construct dataframe and normalize rows
            df = pd.DataFrame(np.nan, index=indexes, columns=cols)
            df['funding', 'rate'] = funding
            df['price', 'close'] = market.get_candle_price_last()
            df['order', 'status'] = np.nan
            df['order', 'id'] = np.nan
            df['order', 'side'] = np.nan
            df['order', 'amount'] = np.nan
            df['order', 'filled'] = np.nan
            df_markets = pd.concat([df, df_markets], axis=0)  # .groupby(level=[0, 1, 2, 3, 4, 5, 6]).mean()

# Sort indexes and columns
df_markets.sort_index(axis=0, inplace=True)
df_markets.sort_index(axis=1, inplace=True)

# Create dataframe with route
df_routes = create_routes(df_account)

# print(df_account)
print('\nMarkets\n')
print(df_markets)

# Create and execute loop
loop = asyncio.get_event_loop()
gp = asyncio.wait([main(loop)])  # , watch_direct_trades()])
loop.run_until_complete(gp)

# loop.run_until_complete(main(loop))
# gp = asyncio.gather(main(loop)) #, watch_direct_trades())
# loop.create_task(main(loop))
# loop.create_task(watch_direct_trades())
# loop.run_forever()
# loop.close()