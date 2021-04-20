
# Select primary key of open order (if any) and check for recent trades.
# Filled quantity of open order is periodically updated with update_pending_orders()
status = df_markets.loc[(base.code, default_type, symbol), ('order', 'status')]

if status == 'open':

    # Select object
    pk = float(df_markets.loc[(base.code, default_type, symbol), ('order', 'id')])
    order = Order.objects.get(pk=pk)

    # Select filled quantity in dataframe and compare it to quantity in object
    filled = df_markets.loc[(base.code, default_type, symbol), ('order', 'filled')]
    filled_total = float(order.filled)

    if filled_total > filled:
        df_markets.loc[(base.code, default_type, symbol), ('order', 'filled')] = filled_total

        # Calculate delta and Update account quantity and calculate new value (with price at close)
        delta = filled_total - filled
        update_account_df(base.code, default_type, order.side, delta, close)

        # Update status
    if order.status != 'open':
        df_markets.loc[(base.code, default_type, symbol), ('order', 'status')] = order.status

else:

    # Get rebalance instructions and quantity hold in this default_type
    # If a sell is needed for this currency default_type_quantity will determine
    # whether or not an order can be placed on the current market
    buy, sell, delta, default_type_quantity = is_rebalance(base.code, default_type)

    if sell:

        # If holdings in this specific default_type
        if default_type_quantity:

            # If market quote is a desired currency
            if quote.code in codes_target:

                # Direct trade
                if quote.code == account.exchange.dollar_currency:

                    # If bids depth from all markets has been captured
                    if all(df_markets.loc[(base.code), ('depth', 'bid')] != 0):

                        # Create an order object and return it's private key
                        amount = min(default_type_quantity, delta)
                        pk = account.create_order(market, 'sell', amount)

                        if pk:

                            print('object created with primary key', pk, 'amount', amount, symbol)

                            # Place order
                            response = place_order(account.name, pk)

                            filled = response['filled']
                            status = response['status']
                            side = response['side']

                            # Update amount
                            df_markets.loc[(base.code, default_type, symbol), ('order', 'id')] = str(pk)
                            df_markets.loc[(base.code, default_type, symbol), ('order', 'status')] = status
                            df_markets.loc[(base.code, default_type, symbol), ('order', 'side')] = side
                            df_markets.loc[(base.code, default_type, symbol), ('order', 'amount')] = amount
                            df_markets.loc[(base.code, default_type, symbol), ('order', 'filled')] = filled

                            print('\nMARKETS', df_markets)

                            if filled :

                                print('\nAmount filled :', filled)

                                # Update account dataframe with new quantity
                                print('\nACCOUNT BEFORE', df_account)
                                update_account_df(base.code, default_type, side, filled, close)
                                print('\nACCOUNT AFTER', df_account)

                    else:
                        print('wait bids depth')

    elif buy:

        # Asks from all markets has been captured
        if all(df_markets.loc[(base.code), ('depth', 'ask')] != 0):

            # Check if there is enough quote
            pass

            if market.type == 'spot':

                # Create an order object and return it's private key
                pk = account.create_order(market, 'buy', abs(delta))

                if pk:

                    # Place valid order
                    response = place_order(account.name, pk)

                    filled = response['filled']
                    status = response['status']
                    side = response['side']

                    # Update amount
                    df_markets.loc[(base.code, default_type, symbol), ('order', 'id')] = str(pk)
                    df_markets.loc[(base.code, default_type, symbol), ('order', 'status')] = status
                    df_markets.loc[(base.code, default_type, symbol), ('order', 'side')] = side
                    df_markets.loc[(base.code, default_type, symbol), ('order', 'amount')] = abs(delta)
                    df_markets.loc[(base.code, default_type, symbol), ('order', 'filled')] = filled

                    print('\nMARKETS', df_markets)

                    if filled :

                        print('\nAmount filled :', filled)

                        # Update account dataframe with new quantity
                        print('\nACCOUNT BEFORE', df_account)
                        update_account_df(base.code, default_type, side, filled, close)
                        print('\nACCOUNT AFTER', df_account)


        else:
            print('wait asks depth')


        # print(df_markets.loc[(base.code)])