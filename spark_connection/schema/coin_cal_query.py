from typing import Any
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from functools import reduce
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_avg_field(markets: list[str], field: str) -> Column:
    """각 거래소 별 평균"""
    columns: list[Column] = [
        F.col(f"crypto.{market}.data.{field}").cast("double") for market in markets
    ]
    avg_column = F.coalesce(F.avg(F.array(*columns)), F.lit(0))
    return F.round(avg_column, 3).alias(field)


def time_instructure(c) -> Column:
    return F.to_timestamp(F.from_unixtime(c + 9 * 3600, "yyyy-MM-dd HH:mm:ss")).alias(
        "timestamp"
    )


def agg_generator(markets: list[str]) -> list[Column]:
    """집계 함수 comprehension"""
    return [F.format_number(F.avg(i), 2).alias(f"avg_{i}") for i in markets]


def process_exchange_data(df: DataFrame, exchange_name: str) -> DataFrame:
    """각 거래소 데이터 처리"""
    try:
        return df.withColumn("market", F.lit(exchange_name)).select(
            F.col(f"{exchange_name}.market").alias("market"),
            time_instructure(F.col(f"{exchange_name}.timestamp")),
            F.col(f"{exchange_name}.coin_symbol").alias("coin_symbol"),
            F.col(f"{exchange_name}.data.opening_price").alias("opening_price"),
            F.col(f"{exchange_name}.data.trade_price").alias("trade_price"),
            F.col(f"{exchange_name}.data.max_price").alias("max_price"),
            F.col(f"{exchange_name}.data.min_price").alias("min_price"),
            F.col(f"{exchange_name}.data.prev_closing_price").alias(
                "prev_closing_price"
            ),
            F.col(f"{exchange_name}.data.acc_trade_volume_24h").alias(
                "acc_trade_volume_24h"
            ),
        )
    except Exception as e:
        logger.error(f"Error processing data for exchange {exchange_name}: {str(e)}")
        return df.limit(0)  # 빈 DataFrame 반환


class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(
        self,
        kafka_data: DataFrame,
        market: list[str],
        partition: int,
        config: dict,
        schema: Any,  # schema
    ) -> None:
        self.schema = schema
        self.kafka_cast_string = kafka_data.filter(
            col("partition") == partition
        ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        self.config = config
        self.markets = market
        self.fields = config["fields"]

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
        watermarked_stream = final_stream.withWatermark("timestamp", "30 seconds")
        return watermarked_stream

    def cal_col(self):
        price_grouped = (
            self.coin_main_columns()
            .groupBy(
                "coin_symbol",
                F.window("timestamp", "3 minute"),  # 1분 단위로 윈도우 설정
            )
            .agg(
                *agg_generator(self.fields),
                F.avg("acc_trade_volume_24h").alias("total_trade_volume"),
            )
        )
        final_df = price_grouped.select(
            col("coin_symbol").alias("name"),
            F.struct(
                col("avg_opening_price"),
                col("avg_max_price"),
                col("avg_min_price"),
                col("avg_prev_closing_price"),
                col("total_trade_volume"),
            ).alias("data"),
        )

        return final_df
