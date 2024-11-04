"""
spark 
"""

from typing import Any
from concurrent.futures import ThreadPoolExecutor

from src.config.properties import Orderbook_Topic_Name, Ticker_Topic_Name
from src.schema.data_constructure import socket_market_schema, orderbook_data_schema
from src.setting.streaming_connection import (
    SparkStreamingTicker,
    SparkStreamingOrderbook,
)


def ticker_spark_streaming(topics: str, schema: Any) -> None:
    SparkStreamingTicker(topics, schema).ticker_spark_streaming()


def orderbook_spark_streaming(topics: str, schema: Any) -> None:
    SparkStreamingOrderbook(topics, schema).orderbook_spark_streaming()


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(
            ticker_spark_streaming, Ticker_Topic_Name, socket_market_schema
        )
        future2 = executor.submit(
            orderbook_spark_streaming, Orderbook_Topic_Name, orderbook_data_schema
        )

        future1.result()
        future2.result()
