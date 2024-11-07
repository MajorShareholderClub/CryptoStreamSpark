from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.preprocess.ticker_functions import (
    ticker_flatten_exchange_data,
    calculate_time_based_metrics,
    calculate_arbitrage,
    calculate_market_arbitrage,
    detect_price_volume_signals,
)
from src.preprocess.orderbook_functions import (
    orderbook_flatten_exchange_data,
    calculate_region_stats,
    calculate_all_regions_stats,
)


# 메인 클래스
class SparkCoinAverageQueryOrganization:
    def __init__(self, kafka_data: DataFrame) -> None:
        self.kafka_cast_string = kafka_data.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)"
        )


class TickerQueryOrganization(SparkCoinAverageQueryOrganization):
    """Ticker 데이터 처리를 위한 클래스"""

    def __init__(self, kafka_data: DataFrame, schema: Any) -> None:
        super().__init__(kafka_data)
        self.schema = schema
        self.data = self._coin_main_columns()
        self.ticker_data = ticker_flatten_exchange_data(self.data)

    def _coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        return (
            self.kafka_cast_string.select(
                F.from_json("value", schema=self.schema).alias("parsed_data")
            )
            .select(F.explode("parsed_data").alias("crypto"))
            .select("crypto")
        )

    def cal_time_based_metrics(self) -> DataFrame:
        return calculate_time_based_metrics(self.ticker_data)

    def cal_arbitrage(self) -> DataFrame:
        return calculate_arbitrage(self.ticker_data)

    def cal_market_arbitrage(self) -> DataFrame:
        return calculate_market_arbitrage(self.ticker_data)

    def cal_market_arbitrage_signal(self) -> DataFrame:
        return detect_price_volume_signals(self.ticker_data)


class OrderbookQueryOrganization(SparkCoinAverageQueryOrganization):
    """Orderbook 데이터 처리를 위한 클래스"""

    def __init__(self, kafka_data: DataFrame, schema: Any) -> None:
        super().__init__(kafka_data)
        self.schema = schema
        self.data = self._coin_main_columns()
        self.orderbook_data = orderbook_flatten_exchange_data(self.data)

    def _coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        return (
            self.kafka_cast_string.select(
                F.from_json("value", schema=self.schema).alias("parsed_data")
            )
            .select(F.explode("parsed_data").alias("crypto"))
            .select("crypto")
        )

    def cal_region_stats(self) -> DataFrame:
        return calculate_region_stats(self.orderbook_data)

    def cal_all_regions_stats(self) -> DataFrame:
        return calculate_all_regions_stats(self.orderbook_data)
