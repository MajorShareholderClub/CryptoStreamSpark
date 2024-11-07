from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def get_transformed_columns(target: str) -> list[Column]:
    """데이터 컬럼 변환 정의를 반환하는 함수"""
    return [
        F.col(f"{target}.highest_bid").cast("double").alias("highest_bid"),
        F.col(f"{target}.lowest_ask").cast("double").alias("lowest_ask"),
        F.col(f"{target}.total_bid_volume").cast("double").alias("total_bid_volume"),
        F.col(f"{target}.total_ask_volume").cast("double").alias("total_ask_volume"),
    ]


def bid_ask_avg(col_name: Column) -> Column:
    return F.round(F.avg(F.when(F.col(col_name) > 0, F.col(col_name))), 2)


def orderbook_flatten_exchange_data(df: DataFrame) -> DataFrame:
    """각 거래소 데이터를 평탄화하여 변환"""
    flattened_df = (
        df.select(
            "crypto.region",
            "crypto.market",
            "crypto.symbol",
            *get_transformed_columns("crypto"),
            "crypto.timestamp",
        )
        .withWatermark("timestamp", "1 minute")
        .select("*")
    )
    return flattened_df


def calculate_region_stats(df: DataFrame) -> DataFrame:
    """각 region별 통계 계산 (1분 윈도우)"""
    return (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy("region", F.window("timestamp", "1 minute"), "symbol")
        .agg(
            bid_ask_avg("highest_bid").alias("avg_highest_bid"),
            bid_ask_avg("lowest_ask").alias("avg_lowest_ask"),
            bid_ask_avg("total_bid_volume").alias("total_bid_volume"),
            bid_ask_avg("total_ask_volume").alias("total_ask_volume"),
            F.count("*").alias("record_count"),
        )
        .filter(
            (F.col("window.start") > F.lit("2024-01-01").cast("timestamp"))
            & (
                F.col("avg_highest_bid").isNotNull()
                | F.col("avg_lowest_ask").isNotNull()
                | F.col("total_bid_volume").isNotNull()
                | F.col("total_ask_volume").isNotNull()
            )
        )
        .select(
            "region",
            "symbol",
            "window.start",
            "window.end",
            "avg_highest_bid",
            "avg_lowest_ask",
            "total_bid_volume",
            "total_ask_volume",
            "record_count",
        )
    )


# fmt: off
def calculate_all_regions_stats(df: DataFrame) -> DataFrame:
    """모든 region의 통합 통계 계산 (1분 윈도우)"""
    return (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(F.window("timestamp", "1 minute"), "symbol")
        .agg(
            bid_ask_avg("highest_bid").alias("avg_highest_bid"),
            bid_ask_avg("lowest_ask").alias("avg_lowest_ask"), 
            F.sum(F.when(F.col("total_bid_volume") > 0, F.col("total_bid_volume"))).alias("total_bid_volume"),
            F.sum(F.when(F.col("total_ask_volume") > 0, F.col("total_ask_volume"))).alias("total_ask_volume"),
            F.count("*").alias("record_count"),
        )
        .filter(
            (F.col("window.start") > F.lit("2024-01-01").cast("timestamp")) &
            (F.col("total_bid_volume").isNotNull() | F.col("total_ask_volume").isNotNull())
        )
        .select(
            F.lit("Total").alias("region"),
            "symbol",
            "window.start",
            "window.end",
            "avg_highest_bid",
            "avg_lowest_ask",
            "total_bid_volume",
            "total_ask_volume",
            "record_count",
        )
    )
