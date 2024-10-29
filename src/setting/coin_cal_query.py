from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.preprocess.ticker_functions import (
    ticker_flatten_exchange_data,
    calculate_time_based_metrics,
    calculate_arbitrage,
)
from src.preprocess.orderbook_functions import (
    orderbook_flatten_exchange_data,
    calculate_region_stats,
    calculate_all_regions_stats,
)


# 메인 클래스
class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(
        self,
        kafka_data: DataFrame,
        schema: Any,
    ) -> None:
        self.schema = schema

        self.kafka_cast_string = kafka_data.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)"
        )
        self.data = self.coin_main_columns()

    def coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        return self.kafka_cast_string.select(
            F.from_json("value", schema=self.schema).alias("crypto")
        )


class TickerQueryOrganization(SparkCoinAverageQueryOrganization):
    """Ticker 데이터 처리를 위한 클래스"""

    def __init__(self, kafka_data: DataFrame, schema: Any) -> None:
        super().__init__(kafka_data, schema)
        self.ticker_data = ticker_flatten_exchange_data(self.data)

    def cal_time_based_metrics(self) -> DataFrame:
        return calculate_time_based_metrics(self.ticker_data)

    def cal_arbitrage(self) -> DataFrame:
        return calculate_arbitrage(self.ticker_data)


class OrderbookQueryOrganization(SparkCoinAverageQueryOrganization):
    """Orderbook 데이터 처리를 위한 클래스"""

    def __init__(self, kafka_data: DataFrame, schema: Any) -> None:
        super().__init__(kafka_data, schema)
        self.orderbook_data = orderbook_flatten_exchange_data(self.data)

    def cal_region_stats(self) -> DataFrame:
        return calculate_region_stats(self.orderbook_data)

    def cal_all_regions_stats(self) -> DataFrame:
        return calculate_all_regions_stats(self.orderbook_data)
