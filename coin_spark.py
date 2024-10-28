"""
spark 
"""

from typing import Any
from src.setting.streaming_connection import SparkStreamingCoinAverage
from src.config.properties import (
    BTC_TOPIC_NAME,
    BTC_AVERAGE_TOPIC_NAME,
    ETH_TOPIC_NAME,
    ETH_AVERAGE_TOPIC_NAME,
)
from src.schema.data_constructure import socket_market_schema


def run_spark_streaming(coin_name: str, topics: str, schema: Any) -> None:
    SparkStreamingCoinAverage(coin_name, topics, schema).run_spark_streaming()


if __name__ == "__main__":
    # spark_in_start()
    run_spark_streaming("BTC", BTC_TOPIC_NAME, socket_market_schema)
