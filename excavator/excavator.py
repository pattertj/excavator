import datetime as dt
import gzip
import logging
import logging.config
import os
import time
from typing import Any

import attr
import pandas as pd
from broker.tda import GetMarketHoursResponseMessage, TdaBroker
from pytz import timezone

logger = logging.getLogger("excavator")


@attr.s(auto_attribs=True)
class Excavator:
    symbol: str = attr.ib(
        validator=attr.validators.instance_of(str), init=False, default="$SPX.X"
    )
    max_dte: int = attr.ib(
        validator=attr.validators.instance_of(int), init=False, default=60
    )
    min_dte: int = attr.ib(
        validator=attr.validators.instance_of(int), init=False, default=0
    )
    data_interval: int = attr.ib(
        validator=attr.validators.instance_of(int), init=False, default=1
    )
    store_greeks: bool = attr.ib(
        validator=attr.validators.instance_of(bool), init=False, default=True
    )
    contract_type: str = attr.ib(
        validator=attr.validators.in_(["PUT", "CALL", "ALL"]), init=False, default="ALL"
    )
    broker: TdaBroker = attr.ib(
        validator=attr.validators.instance_of(TdaBroker),
        init=False,
        default=TdaBroker(),
    )

    ##################
    ### CORE LOGIC ###
    ##################
    def dig(self):
        # Check the market hours to begin us.
        market_hours = self.get_next_market_hours()

        # Loop
        while True:
            # Market Hours are returned in US EST, our "now" time should be as well.
            now = dt.datetime.now(tz=timezone("US/Eastern"))

            # Process appropriate market status
            if now < market_hours.start or market_hours.end < now:
                self.process_closed_market(market_hours, now)
            elif market_hours.start < now < market_hours.end:
                self.process_open_market()

    #######################
    ### PROCESS MARKETS ###
    #######################
    def process_open_market(self):
        # Get Option Chain
        option_chain_request = self.build_option_chain_request()
        option_chain = self.broker.get_option_chain(option_chain_request)

        # Validate Option Chain
        if any(
            [
                option_chain is None,
                option_chain["callExpDateMap"] is None,
                option_chain["callExpDateMap"] is None,
            ]
        ):
            logger.critical("Failed to retrieve option chain.")
            return

        # Get VIX
        vix = self.broker.get_quote("$VIX.X")

        # Iterate Put Option Chain Expirations
        for expiration in dict(option_chain["putExpDateMap"]).items():
            self.process_expiration(
                expiration, option_chain["underlyingPrice"], vix["$VIX.X"]["lastPrice"]
            )

        # Iterate Call Option Chain Expirations
        for expiration in dict(option_chain["callExpDateMap"]).items():
            self.process_expiration(
                expiration, option_chain["underlyingPrice"], vix["$VIX.X"]["lastPrice"]
            )

        # Sleep according to the looping interval
        self.iteration_sleep()

    def process_closed_market(
        self, market_hours: GetMarketHoursResponseMessage, now: dt.datetime
    ):
        # If market closed earlier today, find the next market opening
        if market_hours.start < now:
            self.process_after_hours()
            market_hours = self.get_next_market_hours()

        # Sleep until the market opens + 15 seconds due to weird bid/ask spreads
        time_to_sleep = (market_hours.start - now).total_seconds() + 15

        # Log it
        logger.info(f"Markets are closed. Sleeping until {market_hours.start}.")

        # Sleep
        time.sleep(time_to_sleep)

    def process_after_hours(self):
        output_path = self.get_output_path()

        for file in os.listdir(output_path):
            if file.endswith(".csv"):
                with open(f"{output_path}/{file}", "rb") as csv:
                    # Write the CSV
                    output = gzip.open(f"{output_path}/{file}.gz", "wb")
                    output.writelines(csv)
                    output.close()

    ####################
    ### OPTION CHAIN ###
    ####################
    def process_expiration(
        self, expiration: tuple[str, dict], underlying_price: float, volatility: float
    ):
        # Build Strike List
        strike_list = []

        # Iterate strikes in the expiration
        for strike in expiration[1].values():
            strike = self.process_strike(strike, underlying_price, volatility)
            strike_list.append(strike)

        # Convert list to a Data Frame
        df = pd.DataFrame(strike_list)

        # Save as CSV
        self.save_to_csv(df, expiration[0])

    def save_to_csv(self, df: pd.DataFrame, expiration: str):
        # Get variables
        expiration_dateparts = expiration.split(":")[0].split("-")
        ticker = self.symbol.replace("$", "").split(".")

        # Build strings
        exp_date = f"{expiration_dateparts[0]}{expiration_dateparts[1]}{expiration_dateparts[2]}"
        folder_path = self.get_output_path()
        file_name = f"{ticker[0]}.{exp_date}.csv"

        # Make sure our folder exists, make it if not
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Build the output path
        output_path = f"{folder_path}/{file_name}"

        # Save to CSV, adding in headers the first time.
        df.to_csv(
            output_path, mode="a", header=not os.path.exists(output_path), index=False
        )

    def process_strike(
        self, strike: list, underlying_price: float, volatility: float
    ) -> dict[str, Any]:
        now = dt.datetime.now(tz=timezone("US/Eastern"))
        now_rounded = now.replace(second=0, microsecond=0)
        put_call = "P" if strike[0]["putCall"] == "PUT" else "C"

        return {
            "Time": now_rounded.isoformat(),
            "Symbol": self.symbol,
            "Underlying price": underlying_price,
            "Volatility": volatility,
            "Strike": strike[0]["strikePrice"],
            "PutCall": put_call,
            "Bid": strike[0]["bid"],
            "Ask": strike[0]["ask"],
            "Delta": strike[0]["delta"],
            "Gamma": strike[0]["gamma"],
            "Theta": strike[0]["theta"],
            "Vega": strike[0]["vega"],
            "Rho": strike[0]["rho"],
        }

    def build_option_chain_request(self):
        return {
            "symbol": self.symbol,
            "contractType": self.contract_type,
            "includeQuotes": "FALSE",
            "range": "ALL",
            "fromDate": dt.date.today() + dt.timedelta(days=self.min_dte),
            "toDate": dt.date.today() + dt.timedelta(days=self.max_dte),
        }

    ####################
    ### MARKET HOURS ###
    ####################
    def get_next_market_hours(
        self,
        date: dt.datetime = dt.datetime.now().astimezone(dt.timezone.utc),
    ):
        # Build Market Request
        market_hours_request = {"market": "OPTION", "date": str(date), "product": "IND"}

        # Get the Market Hours
        hours = self.broker.get_market_hours(market_hours_request)

        # If the market is not open or closed already, check tomorrow
        if hours is None or hours.end < dt.datetime.now(tz=timezone("US/Eastern")):
            return self.get_next_market_hours(date + dt.timedelta(days=1))

        # Return the result
        return hours

    ##############
    ### SHARED ###
    ##############
    def iteration_sleep(self):
        # Get now and now rounded to the minute
        now = dt.datetime.now(tz=timezone("US/Eastern"))
        now_rounded = now.replace(second=0, microsecond=0)

        # Find the next time to pull data
        next_interval = now_rounded + dt.timedelta(minutes=self.data_interval)

        # Calculate the time to sleep
        time_to_sleep = (next_interval - now).total_seconds()

        # Log it
        logger.info(
            f"{now_rounded.strftime('%H:%M')} iteration complete. Sleeping {time_to_sleep} seconds, until {next_interval.strftime('%H:%M')}."
        )

        # Sleep
        time.sleep(time_to_sleep)

    def get_output_path(self) -> str:
        year = dt.datetime.now().strftime("%Y")
        month = dt.datetime.now().strftime("%m")
        day = dt.datetime.now().strftime("%d")
        ticker = self.symbol.replace("$", "").split(".")

        return f"{os.curdir}/results/{ticker[0]}/{year}/{year}{month}{day}"
