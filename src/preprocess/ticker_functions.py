from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

# fmt: off
def unix_timestamp_to_timestamp(col_name: Column) -> Column:
    """unix timestamp를 timestamp로 변환"""
    return F.to_timestamp(F.from_unixtime(col_name / 1000)).alias("timestamp")


def get_transformed_columns(target: str) -> list[Column]:
    """데이터 컬럼 변환 정의를 반환하는 함수"""
    return [
        F.coalesce(F.col(f"{target}.opening_price").cast("double"), F.lit(0.0)).alias("opening_price"),
        F.coalesce(F.col(f"{target}.trade_price").cast("double"), F.lit(0.0)).alias("trade_price"),
        F.coalesce(F.col(f"{target}.max_price").cast("double"), F.lit(0.0)).alias("max_price"),
        F.coalesce(F.col(f"{target}.min_price").cast("double"), F.lit(0.0)).alias("min_price"),
        F.coalesce(F.col(f"{target}.prev_closing_price").cast("double"), F.lit(0.0)).alias("prev_closing_price"),
        F.coalesce(F.col(f"{target}.acc_trade_volume_24h").cast("double"), F.lit(0.0)).alias("acc_trade_volume_24h"),
        F.coalesce(F.col(f"{target}.signed_change_price").cast("double"), F.lit(0.0)).alias("signed_change_price"),
        F.coalesce(F.col(f"{target}.signed_change_rate").cast("double"), F.lit(0.0)).alias("signed_change_rate")
    ]


def ticker_flatten_exchange_data(df: DataFrame) -> DataFrame:
    """각 거래소 데이터를 평탄화하여 변환"""
    flattened_df = df.select(
        "crypto.region",
        "crypto.market",
        "crypto.coin_symbol",
        unix_timestamp_to_timestamp(F.col("crypto.timestamp")),
        F.explode("crypto.data").alias("data"),
    ).select(
        "region",
        "market",
        "coin_symbol",
        "timestamp",
        *get_transformed_columns("data"),
    )
    return flattened_df


def calculate_time_based_metrics(
    flattened_df: DataFrame,
    window_duration: str = "1 minute",
    sliding_duration: str = "30 seconds",
) -> DataFrame:
    """시간 기반 메트릭 계산 (통계 및 이동 평균)"""
    return (
        flattened_df
        .withWatermark("timestamp", "2 minutes")  # watermark 적용
        .groupBy(
            F.window("timestamp", window_duration, sliding_duration),
            F.col("coin_symbol"),
            F.col("region"),
        )
        .agg(
            F.round(F.avg("trade_price"), 2).alias("avg_price"),
            F.round(F.max("max_price"), 2).alias("highest_price"),
            F.round(F.min("min_price"), 2).alias("lowest_price"),
            F.round(F.sum("acc_trade_volume_24h"), 2).alias("total_volume"),
            F.round(F.stddev("trade_price"), 2).alias("price_volatility"),
            F.round(F.avg("trade_price"), 2).alias("MA_price"),
            F.round(F.avg("acc_trade_volume_24h"), 2).alias("MA_volume"),
        )
        .select(
            "region",
            "coin_symbol",
            "window.start",
            "window.end",
            "avg_price",
            "highest_price",
            "lowest_price",
            "total_volume",
            "price_volatility",
            "MA_price",
            "MA_volume",
        )   
    )


def calculate_arbitrage(df: DataFrame) -> DataFrame:
    """지역 간 거래 차익 계산"""
    
    def avg_region_price(region: str) -> Column:
        """거래소 매핑 함수"""
        return F.avg(F.when(F.col("region") == region, F.col("trade_price")).otherwise(F.lit(0.0)))

    def region_spread_price(first_region: str, second_region: str) -> Column:
        """지역 가격 차익 계산 함수"""
        first_region_price = F.col(f"{first_region}_price")
        second_region_price = F.col(f"{second_region}_price")
        return F.round(
            (first_region_price - second_region_price) / second_region_price * 100,
            2,
        )
        
    def region_spread(first_region: str, second_region: str) -> Column:
        """지역 간 거래 차익 계산 함수"""
        first_region_price = F.col(f"{first_region}_price")
        second_region_price = F.col(f"{second_region}_price")
        return F.round(
            (first_region_price - second_region_price) / second_region_price * 100,
            2,
        )

    return (
        df.withWatermark("timestamp", "3 minutes")  # watermark 적용
        .groupBy(F.window("timestamp", "5 minutes", "1 minute"), "coin_symbol")
        .agg(
            avg_region_price("korea").alias("kr_price"),
            avg_region_price("asia").alias("asia_price"),
            avg_region_price("ne").alias("global_price"),
        )
        .select(
            "coin_symbol",
            "window.start",
            "window.end",
            "kr_price",
            "global_price",
            "asia_price",
            region_spread_price("kr", "global").alias("kr_global_spread"),
            region_spread_price("kr", "asia").alias("kr_asia_spread"),
            region_spread_price("global", "asia").alias("global_asia_spread"),
            region_spread("kr", "global").alias("kr_global_spread_percent"),
            region_spread("kr", "asia").alias("kr_asia_spread_percent"),
            region_spread("global", "asia").alias("global_asia_spread_percent"),
        )
    )

# def calculate_vwap(flattened_df: DataFrame) -> DataFrame:
#     """VWAP 계산 (시간 기반)"""
#     vwap_df = flattened_df.groupBy(F.window("timestamp", "10 minutes")).agg(
#         F.round(
#             F.sum(F.col("trade_price") * F.col("acc_trade_volume_24h"))
#             / F.sum("acc_trade_volume_24h"),
#             2,
#         ).alias("VWAP")
#     )
#     return vwap_df


# def detect_outliers(stats_df: DataFrame) -> DataFrame:
#     """이상치 탐지"""
#     # 평균과 표준편차 계산
#     avg_volatility = stats_df.agg(F.avg("price_volatility")).first()
#     stddev_volatility = stats_df.agg(F.stddev("price_volatility")).first()

#     # 이상치 기준 설정 (예: 평균 + 2 * 표준편차)
#     threshold = avg_volatility + 2 * stddev_volatility

#     # 이상치 필터링
#     outliers_df = stats_df.filter(F.col("price_volatility") > threshold)

#     return outliers_df
