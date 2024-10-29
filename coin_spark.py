"""
spark 
"""

from typing import Any
from src.setting.streaming_connection import (
    TickerQueryOrganization,
    OrderbookQueryOrganization,
)
from concurrent.futures import ThreadPoolExecutor
from src.config.properties import Orderbook_Topic_Name, Ticker_Topic_Name
from src.schema.data_constructure import socket_market_schema, orderbook_data_schema


def ticker_spark_streaming(topics: str, schema: Any) -> None:
    TickerQueryOrganization(topics, schema).run_spark_streaming()


def orderbook_spark_streaming(topics: str, schema: Any) -> None:
    OrderbookQueryOrganization(topics, schema).run_spark_streaming()


if __name__ == "__main__":
    # spark_in_start()
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(ticker_spark_streaming, Ticker_Topic_Name, socket_market_schema)
        executor.submit(
            orderbook_spark_streaming, Orderbook_Topic_Name, orderbook_data_schema
        )
