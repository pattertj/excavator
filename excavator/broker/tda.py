import logging
import logging.config
from datetime import date, datetime
from os import getenv
from typing import Any, Union

import attr
from dotenv import load_dotenv
from td.client import TDClient
from td.option_chain import OptionChain

logger = logging.getLogger("excavator")
load_dotenv()


@attr.s(auto_attribs=True)
class GetMarketHoursRequestMessage:
    """Generic request object for getting Market Hours."""

    market: str = attr.ib(
        validator=attr.validators.in_(["OPTION", "EQUITY", "FUTURE", "FOREX", "BOND"])
    )
    product: str = attr.ib(validator=attr.validators.in_(["EQO", "IND"]))
    datetime: datetime = attr.ib(
        default=datetime.now(), validator=attr.validators.instance_of(datetime)
    )


@attr.s(auto_attribs=True, init=False)
class GetMarketHoursResponseMessage:
    """Generic reponse object for getting Market Hours."""

    start: datetime = attr.ib(validator=attr.validators.instance_of(datetime))
    end: datetime = attr.ib(validator=attr.validators.instance_of(datetime))
    isopen: bool = attr.ib(validator=attr.validators.instance_of(bool))


@attr.s(auto_attribs=True)
class GetOptionChainRequestMessage:
    """Generic request object for retrieving the Option Chain."""

    symbol: str = attr.ib(validator=attr.validators.instance_of(str))
    contract_type: str = attr.ib(validator=attr.validators.in_(["CALL", "PUT", "ALL"]))
    include_quotes: bool = attr.ib(validator=attr.validators.instance_of(bool))
    option_range: str = attr.ib(
        validator=attr.validators.in_(["ITM", "NTM", "OTM", "SAK", "SBK", "SNK", "ALL"])
    )
    from_date: date = attr.ib(validator=attr.validators.instance_of(date))
    to_date: date = attr.ib(validator=attr.validators.instance_of(date))


@attr.s(auto_attribs=True)
class TdaBroker:
    def get_option_chain(self, params: dict) -> Union[None, dict]:
        """Reads the option chain for a given symbol, date range, and contract type."""

        if params is None:
            logger.error("OptionChainRequest is None.")
            return None

        option_chain = OptionChain()
        option_chain.query_parameters = params

        if not option_chain.validate_chain():
            logger.exception("Chain Validation Failed.")
            return None

        for attempt in range(3):
            try:
                options_chain = self.getsession().get_options_chain(params)

                if options_chain["status"] == "FAILED":
                    raise BaseException("Option Chain Status Response = FAILED")

                return options_chain

            except BaseException:
                logger.exception(f"Failed to get Options Chain. Attempt #{attempt}")
                if attempt == 3 - 1:
                    return None

        return None

    def get_market_hours(
        self, request: dict
    ) -> Union[None, GetMarketHoursResponseMessage]:
        """Gets the opening and closing market hours for a given day."""

        markets = [request["market"]]

        # Get Market Hours
        for attempt in range(3):
            try:
                hours = self.getsession().get_market_hours(
                    markets=markets, date=str(request["date"])
                )
                break
            except Exception:
                logger.exception(
                    f"Failed to get market hours for {markets} on {request['date']}. Attempt #{attempt}"
                )

                if attempt == 3 - 1:
                    return None

        if hours is None:
            return None

        markettype: dict
        for markettype in hours.values():

            details: dict
            for type, details in markettype.items():
                if type == request["product"]:
                    sessionhours = details.get("sessionHours", dict)

                    return self.process_session_hours(sessionhours, details)

        return None

    def get_quote(self, ticker: str) -> Union[None, dict]:
        for attempt in range(3):
            try:
                quote = self.getsession().get_quotes(instruments=[ticker])
                break
            except Exception:
                logger.exception(f"Failed to get quotes. Attempt #{attempt}")
                if attempt == 3 - 1:
                    return None

        return quote

    def process_session_hours(
        self, sessionhours: dict, details: dict
    ) -> GetMarketHoursResponseMessage:
        """Iterates session hours to build a market hours response"""
        for session, markethours in sessionhours.items():
            if session == "regularMarket":
                response = self.build_market_hours_response(markethours, details)
        return response

    @staticmethod
    def build_market_hours_response(
        markethours: list, details: dict
    ) -> GetMarketHoursResponseMessage:
        """Builds a Market Hours reponse Message for given details"""
        response = GetMarketHoursResponseMessage()

        response.start = datetime.strptime(
            str(dict(markethours[0]).get("start")), "%Y-%m-%dT%H:%M:%S%z"
        )

        response.end = datetime.strptime(
            str(dict(markethours[0]).get("end")), "%Y-%m-%dT%H:%M:%S%z"
        )

        response.isopen = details.get("isOpen", bool)

        return response

    def getsession(self) -> TDClient:
        """Generates a TD Client session"""

        return TDClient(
            client_id=getenv("client_id"),
            redirect_uri=getenv("redirect_uri"),
            account_number=getenv("account_number"),
            credentials_path=getenv("credentials_path"),
        )

    def getaccesstoken(self):
        """Retrieves a new access token."""

        try:
            self.getsession().grab_access_token()
        except Exception:
            logger.exception("Failed to get access token.")

    def build_option_chain_request(
        self, request: GetOptionChainRequestMessage
    ) -> dict[str, Any]:
        """Builds the Option Chain Request Message"""
        return {
            "symbol": request.symbol,
            "contractType": request.contract_type,
            "includeQuotes": "TRUE" if request.include_quotes is True else "FALSE",
            "range": request.option_range,
            "fromDate": request.from_date,
            "toDate": request.to_date,
        }
