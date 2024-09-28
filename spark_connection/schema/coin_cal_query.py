from typing import Any
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from functools import reduce


def get_avg_field(markets: list[str], field: str) -> Column:
    """각 거래소 별 평균

    Args:
        markets (list[str]): 마켓
        field (str): 각 필드 이름

    Returns:
        Column: 각 Col Fields
    """
    columns: list[Column] = [
        F.col(f"crypto.{market}.data.{field}").cast("double") for market in markets
    ]
    avg_column = sum(columns) / len(markets)
    return F.round(avg_column, 3).alias(field)


def time_instructure(c) -> Column:
    return F.to_timestamp(F.from_unixtime(c + 9 * 3600, "yyyy-MM-dd HH:mm:ss")).alias(
        "timestamp"
    )


def agg_generator(markets: list[str]) -> list[Column]:
    """집계 함수 comprehension"""
    return [F.format_number(F.avg(i), 2).alias(f"avg_{i}") for i in markets]


def process_exchange_data(df: DataFrame, exchange_name: str) -> DataFrame:
    return df.withColumn("market", F.lit(exchange_name)).select(
        F.col(f"{exchange_name}.market").alias("market"),
        time_instructure(F.col(f"{exchange_name}.timestamp").alias("timestamp")),
        F.col(f"{exchange_name}.coin_symbol").alias("coin_symbol"),
        F.col(f"{exchange_name}.data.opening_price").alias("opening_price"),
        F.col(f"{exchange_name}.data.trade_price").alias("trade_price"),
        F.col(f"{exchange_name}.data.max_price").alias("max_price"),
        F.col(f"{exchange_name}.data.min_price").alias("min_price"),
        F.col(f"{exchange_name}.data.prev_closing_price").alias("prev_closing_price"),
        F.col(f"{exchange_name}.data.acc_trade_volume_24h").alias(
            "acc_trade_volume_24h"
        ),
    )


class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(
        self,
        kafka_data: DataFrame,
        market: list[str],
        partition: int,
        schema: Any,  # schema
    ) -> None:
        self.schema = schema
        self.kafka_cast_string = kafka_data.filter(
            col("partition") == partition
        ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        self.markets = market
        self.fields = [
            "opening_price",
            "trade_price",
            "max_price",
            "min_price",
            "prev_closing_price",
        ]

    def coin_main_columns(self) -> DataFrame:
        """Process market data from kafka input."""
        data = self.kafka_cast_string.select(
            F.from_json("value", schema=self.schema).alias("crypto")
        )

        # 데이터프레임을 생성
        dataframes = [
            process_exchange_data(data, f"crypto.{market}") for market in self.markets
        ]
        final_stream = reduce(DataFrame.union, dataframes)

        # Watermark 추가
        watermarked_stream = final_stream.withWatermark("timestamp", "10 minutes")
        return watermarked_stream

    def cal_col(self):
        price_grouped = (
            self.coin_main_columns()
            .groupBy(
                "market",
                "coin_symbol",
                F.window("timestamp", "1 minute"),  # 1분 단위로 윈도우 설정
            )
            .agg(
                *agg_generator(self.fields),
                F.avg("acc_trade_volume_24h").alias("total_trade_volume"),
            )
        )
        return price_grouped
