from typing import Any
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import logging

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# 데이터 평탄화 함수 (실시간 스트리밍 지원)
def flatten_exchange_data(df: DataFrame) -> DataFrame:
    """각 거래소 데이터를 평탄화하여 변환"""
    # 데이터 변환 및 평탄화
    flattened_df = df.select(
        "crypto.market",
        "crypto.coin_symbol",
        F.from_unixtime(col("crypto.timestamp") / 1000).alias("timestamp"),
        F.explode("crypto.data").alias("data"),
    ).select(
        "market",
        "coin_symbol",
        "timestamp",
        col("data.opening_price").cast("double").alias("opening_price"),
        col("data.trade_price").cast("double").alias("trade_price"),
        col("data.max_price").cast("double").alias("max_price"),
        col("data.min_price").cast("double").alias("min_price"),
        col("data.prev_closing_price").cast("double").alias("prev_closing_price"),
        col("data.acc_trade_volume_24h").cast("double").alias("acc_trade_volume_24h"),
    )
    return flattened_df


# 통계 계산을 위한 함수
def calculate_statistics(flattened_df: DataFrame) -> DataFrame:
    """기본 통계 분석을 계산"""
    stats_df = flattened_df.select(
        "timestamp",
        "trade_price",
        "max_price",
        "min_price",
        "acc_trade_volume_24h",
    ).agg(
        F.round(F.avg("trade_price"), 2).alias("avg_price"),
        F.round(F.stddev("trade_price"), 2).alias("price_volatility"),
        F.round(F.max("max_price"), 2).alias("highest_price"),
        F.round(F.min("min_price"), 2).alias("lowest_price"),
        F.round(F.sum("acc_trade_volume_24h"), 2).alias("total_volume"),
    )
    return stats_df


# 변동성 지표 계산 함수
def calculate_volatility(flattened_df: DataFrame) -> DataFrame:
    """가격 변동성 지표 계산"""
    volatility_df = flattened_df.select(
        "timestamp",
        F.round(
            (col("max_price") - col("min_price")) / col("opening_price") * 100, 2
        ).alias("price_range_percent"),
        F.round(
            (col("trade_price") - col("opening_price")) / col("opening_price") * 100, 2
        ).alias("price_change_percent"),
    )
    return volatility_df


def calculate_moving_average(flattened_df: DataFrame) -> DataFrame:
    """시간 기반 이동 평균 계산 (6개 데이터 기준)"""
    moving_averages_df = flattened_df.groupBy(F.window("timestamp", "10 minutes")).agg(
        F.round(F.avg("trade_price"), 2).alias("MA6_price"),
        F.round(F.avg("acc_trade_volume_24h"), 2).alias("MA6_volume"),
    )
    return moving_averages_df


# VWAP 계산 함수
def calculate_vwap(flattened_df: DataFrame) -> DataFrame:
    """VWAP 계산 (시간 기반)"""
    # 시간 기반의 슬라이딩 창을 생성 (예: 10분 간격)
    vwap_df = flattened_df.groupBy(F.window("timestamp", "10 minutes")).agg(
        F.round(
            F.sum(col("trade_price") * col("acc_trade_volume_24h"))
            / F.sum("acc_trade_volume_24h"),
            2,
        ).alias("VWAP")
    )
    return vwap_df


# 가격 변동 패턴 분석 함수
def analyze_price_patterns(flattened_df: DataFrame) -> DataFrame:
    """가격 변동 패턴 분석"""
    price_patterns_df = flattened_df.select(
        "timestamp",
        F.when(col("trade_price") > col("opening_price"), "상승")
        .when(col("trade_price") < col("opening_price"), "하락")
        .otherwise("보합")
        .alias("price_pattern"),
        F.round(col("trade_price") - col("opening_price"), 2).alias("price_change"),
    )
    return price_patterns_df


class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(
        self,
        kafka_data: DataFrame,
        partition: int,
        config: dict,
        schema: Any,
    ) -> None:
        self.schema = schema
        self.kafka_cast_string = kafka_data.filter(
            col("partition") == partition
        ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        self.config = config
        self.fields = config["fields"]

    def coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        data = self.kafka_cast_string.select(
            F.from_json("value", schema=self.schema).alias("crypto")
        )
        data.printSchema()
        flat_data = flatten_exchange_data(data)
        return flat_data

    def cal_col(self):
        # 데이터 변환 및 평탄화
        data = self.coin_main_columns()

        stats_df = calculate_statistics(data)
        volatility_df = calculate_volatility(data)
        moving_averages_df = calculate_moving_average(data)
        vwap_df = calculate_vwap(data)
        price_patterns_df = analyze_price_patterns(data)

        return vwap_df
