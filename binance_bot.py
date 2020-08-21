import argparse
import boto3
import configparser
import datetime
import math
import json
import time

from decimal import Decimal

from binance.exceptions import BinanceAPIException
from binance.client import Client


def get_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


parser = argparse.ArgumentParser(
    description="""
        This is a basic Binance DCA buying/selling bot.

        ex:
            ETH-BTC SELL 0.00125 BTC    (sell 0.00125 BTC worth of ETH)
            ETH-BTC SELL 0.1 ETH        (sell 0.1 ETH)
    """,
    formatter_class=argparse.RawTextHelpFormatter
)

# Required positional arguments
parser.add_argument('market_name', help="(e.g. BTC-USD, ETH-BTC, etc)")

parser.add_argument('order_side',
                    type=str,
                    choices=["BUY", "SELL"])

parser.add_argument('amount',
                    type=Decimal,
                    help="The quantity to buy or sell in the amount_currency")

parser.add_argument('amount_currency',
                    help="The currency the amount is denominated in")

# Optional switches
parser.add_argument('-c', '--settings_config',
                    default="settings.conf",
                    dest="settings_config_file",
                    help="Override default settings config file location")

parser.add_argument('-d', '--dynamic_dca',
                    action='store_true',
                    default=False,
                    dest="dynamic_dca",
                    help="""Scale the trade amount up or down depending on 24hr price change""")

parser.add_argument('-l', '--live',
                    action='store_true',
                    default=False,
                    dest="live_mode",
                    help="""Submit live orders. When omitted, just tests API connection
                        and amount without submitting actual orders""")

parser.add_argument('-j', '--job',
                    action='store_true',
                    default=False,
                    dest="job_mode",
                    help="""Suppress the confirmation step before submitting
actual orders""")


if __name__ == "__main__":
    args = parser.parse_args()

    market_name = args.market_name
    order_side = args.order_side.lower()
    amount = args.amount
    amount_currency = args.amount_currency
    is_dynamic_dca = args.dynamic_dca

    live_mode = args.live_mode
    job_mode = args.job_mode

    print("%s: STARTED: %s" % (get_timestamp(), args))

    if not live_mode:
        print("\n")
        print("\t================= NOT in Live mode =================")
        print("\t*                                                  *")
        print("\t*        No actual trades being submitted!         *")
        print("\t*                                                  *")
        print("\t====================================================")
        print("\n")

    # Read settings
    config = configparser.ConfigParser()
    config.read(args.settings_config_file)

    api_key = config.get('API', 'API_KEY')
    api_secret = config.get('API', 'SECRET_KEY')

    try:
        sns_topic = config.get('AWS', 'SNS_TOPIC')
        aws_access_key_id = config.get('AWS', 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    except configparser.NoSectionError:
        sns_topic = None

    if sns_topic:
        # Prep boto SNS client for email notifications
        sns = boto3.client(
            "sns",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="us-east-1"     # N. Virginia
        )

    # Instantiate API client
    client = Client(api_key, api_secret)

    # Get exchange info (pairs available, min order sizes, etc.)
    exchange_info = client.get_exchange_info()

    """
        {
            "symbol": "ETHBTC",
            "status": "TRADING",
            "baseAsset": "ETH",
            "baseAssetPrecision": 8,
            "quoteAsset": "BTC",
            "quotePrecision": 8,
            "quoteAssetPrecision": 8,
            "baseCommissionPrecision": 8,
            "quoteCommissionPrecision": 8,
            "orderTypes": [
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT_LIMIT"
            ],
            "icebergAllowed": true,
            "ocoAllowed": true,
            "quoteOrderQtyMarketAllowed": true,
            "isSpotTradingAllowed": true,
            "isMarginTradingAllowed": true,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00000100",
                    "maxPrice": "100000.00000000",
                    "tickSize": "0.00000100"
                },
                {
                    "filterType": "PERCENT_PRICE",
                    "multiplierUp": "5",
                    "multiplierDown": "0.2",
                    "avgPriceMins": 5
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00100000",
                    "maxQty": "100000.00000000",
                    "stepSize": "0.00100000"
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "0.00010000",
                    "applyToMarket": true,
                    "avgPriceMins": 5
                },
                {
                    "filterType": "ICEBERG_PARTS",
                    "limit": 10
                },
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "8949.33836294",
                    "stepSize": "0.00000000"
                },
                {
                    "filterType": "MAX_NUM_ORDERS",
                    "maxNumOrders": 200
                },
                {
                    "filterType": "MAX_NUM_ALGO_ORDERS",
                    "maxNumAlgoOrders": 5
                }
            ],
            "permissions": [
                "SPOT",
                "MARGIN"
            ]
        }
    """
    for market in exchange_info.get("symbols"):
        if market.get("symbol") == market_name:
            base_currency = market.get("baseAsset")
            quote_currency = market.get("quoteAsset")
            quote_asset_precision = market.get("quoteAssetPrecision")

            # What's this asset's minimum purchase?
            for filter in market.get("filters"):
                if filter.get('filterType') == 'MIN_NOTIONAL':
                    base_min_size = Decimal(filter.get("minNotional")).normalize()
                elif filter.get('filterType') == 'LOT_SIZE':
                    base_increment = Decimal(filter.get("stepSize")).normalize()
                elif filter.get('filterType') == 'PRICE_FILTER':
                    quote_increment = Decimal(filter.get("tickSize")).normalize()

            if not base_min_size:
                raise Exception("MIN_NOTIONAL.minNotional not found in %s info" % market_name)
            if not base_increment:
                raise Exception("LOT_SIZE.stepSize not found in %s info" % market_name)
            if not quote_increment:
                raise Exception("PRICE_FILTER.tickSize not found in %s info" % market_name)

            if amount_currency == quote_currency:
                amount_currency_is_quote_currency = True
            elif amount_currency == base_currency:
                amount_currency_is_quote_currency = False
            else:
                raise Exception("amount_currency %s not in market %s" % (amount_currency,
                                                                         market_name))
            print(market)

    print("base_min_size: %s" % base_min_size)
    print("base_increment: %s" % base_increment)
    print("quote_increment: %s" % quote_increment)

    if live_mode and not job_mode:
        print("\n================================================\n")
        response = input("\tLive purchase! Confirm Y/[n]: ")
        if response != 'Y':
            print("Exiting without submitting orders.")
            exit()

    purchase_summary = ""

    if is_dynamic_dca:
        step_size = Decimal("5.0")
        amount_multiplier = Decimal("1.0")
        amount_divider = Decimal("0.25")
        orig_amount = amount

        # Get the current 24hr price diff
        ticker = client.get_ticker(symbol=market_name)
        percent_change = Decimal(ticker.get("priceChangePercent"))
        steps = int(math.floor(abs(percent_change / step_size)))

        print(f"\tDynamic DCA\n" +
                f"\tpercent_change: {percent_change}%\n" +
                f"\tsteps: {steps}")

        if steps > 0:
            if (order_side == 'buy' and percent_change < 0.0) or \
                    (order_side == 'sell' and percent_change > 0.0):
                # We want to multiply up our trade amount
                amount += amount * amount_multiplier * Decimal(steps)
                print(f"Dynamic DCA scaling amount up {steps}x to {amount}")
            else:
                # Divide down the trade amount
                amount -= amount * amount_divider * Decimal(steps)
                if amount <= 0.0:
                    print(f"Dynamic DCA canceling trade at {percent_change}%")
                    amount = Decimal("0.0")
                else:
                    print(f"Dynamic DCA scaling amount down {steps}x to {amount}")
        else:
            # No changes to apply
            is_dynamic_dca = False


    # What's the current best offer?
    #   Binance maker/taker fees are the same so just do a market order for fast order
    #   fills.
    depth = client.get_order_book(symbol=market_name, limit=5)
    if order_side == 'buy':
        market_price = Decimal(depth.get("bids")[0][0])
    else:
        market_price = Decimal(depth.get("asks")[0][0])

    print("market_price: %s %s" % (market_price, quote_currency))

    # Denominate our target amount as necessary, then quantize to base_increment
    if amount_currency_is_quote_currency:
        base_currency_amount = (amount / market_price).quantize(base_increment)
    else:
        base_currency_amount = Decimal(amount).quantize(base_increment)

    print("base_currency_amount: %s %s" % (base_currency_amount, base_currency))

    order_value = (base_currency_amount * market_price).quantize(
        Decimal('1e-%d' % quote_asset_precision)
    )
    if order_value < base_min_size:
        message = f"Cannot purchase {float(base_currency_amount)} {base_currency} @ {market_price} {quote_currency}. " +
            f"Resulting order of {order_value:.8f} {quote_currency} " +
            f"is below the minNotional value of {base_min_size} {quote_currency}"
        print(message)

        if is_dynamic_dca:
            purchase_summary = "Dynamic DCA: %0.2f%% (%dx): %s %s order of %s (%s) %s CANCELED" % (
                percent_change,
                steps,
                market_name,
                order_side,
                amount,
                orig_amount,
                amount_currency
            )
            print(purchase_summary)
            if sns_topic and live_mode:
                sns.publish(
                    TopicArn=sns_topic,
                    Subject=purchase_summary,
                    Message=message
                )

        exit()
    else:
        print("order_value: %s %s" % (order_value, quote_currency))

    if not live_mode:
        if order_side == 'buy':
            side = Client.SIDE_BUY
        else:
            side = Client.SIDE_SELL

        order = client.create_test_order(
            symbol=market_name,
            side=side,
            type=Client.ORDER_TYPE_MARKET,
            quantity=float(base_currency_amount)
        )

        if order:
            print(order)
    else:
        try:
            if order_side == 'buy':
                order = client.order_market_buy(
                    symbol=market_name,
                    quantity=float(base_currency_amount))
            else:
                order = client.order_market_sell(
                    symbol=market_name,
                    quantity=float(base_currency_amount))
        except BinanceAPIException as e:
            print(f'Unable to place {market} order: {e}')
            if sns_topic and live_mode:
                sns.publish(
                    TopicArn=sns_topic,
                    Subject=f'Unable to place {market} order',
                    Message=str(e)
                )
            exit()

        print(json.dumps(order, indent=4))
        """
            {
                "symbol": "ADABTC",
                "orderId": 194439891,
                "orderListId": -1,
                "clientOrderId": "jfsd09eijfsdkl",
                "transactTime": 1596984553336,
                "price": "0.00000000",
                "origQty": "10.00000000",
                "executedQty": "10.00000000",
                "cummulativeQuoteQty": "0.00012380",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "side": "SELL",
                "fills": [
                    {
                        "price": "0.00001238",
                        "qty": "10.00000000",
                        "commission": "0.00004701",
                        "commissionAsset": "BNB",
                        "tradeId": 40016638
                    }
                ]
            }
        """
        market_price = order.get("fills")[0].get("price")

    if is_dynamic_dca:
        purchase_summary = "Dynamic DCA: %0.2f%% (%dx): %s %s order of %s (%s) %s %s @ %s %s" % (
            percent_change,
            steps,
            market_name,
            order_side,
            amount,
            orig_amount,
            amount_currency,
            order.get("status"),
            market_price,
            quote_currency
        )
    else:
        purchase_summary = "%s %s order of %s %s %s @ %s %s" % (
            market_name,
            order_side,
            amount,
            amount_currency,
            order.get("status"),
            market_price,
            quote_currency
        )

    if sns_topic and live_mode:
        sns.publish(
            TopicArn=sns_topic,
            Subject=purchase_summary,
            Message=json.dumps(order, indent=4)
        )

    print("\n================================================")
    print(purchase_summary)
    if not live_mode:
        print("(NOT in live mode - no actual orders placed!)")

