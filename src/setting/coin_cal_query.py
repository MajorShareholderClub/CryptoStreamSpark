from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.analysis_functions import (
    flatten_exchange_data,
    calculate_time_based_metrics,
    calculate_arbitrage,
)


# 데이터 평탄화 및 기본 처리
def flatten_and_process_data(df: DataFrame) -> DataFrame:
    """데이터를 평탄화하고 기본 컬럼을 선택"""
    flattened_df = flatten_exchange_data(df)
    return flattened_df


# 메인 클래스
class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(
        self,
        kafka_data: DataFrame,
        config: dict,
        schema: Any,
    ) -> None:
        self.schema = schema
        self.kafka_cast_string = kafka_data.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)"
        )
        self.config = config

    def coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        data = self.kafka_cast_string.select(
            F.from_json("value", schema=self.schema).alias("crypto")
        )
        flat_data = flatten_and_process_data(data)
        return flat_data

    def cal_time_based_metrics(self):
        # 데이터 변환 및 평탄화
        return calculate_time_based_metrics(self.coin_main_columns())

    def cal_arbitrage(self):
        return calculate_arbitrage(self.coin_main_columns())
