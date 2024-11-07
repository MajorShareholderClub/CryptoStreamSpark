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


def calculate_time_based_metrics(df: DataFrame) -> DataFrame:
    """시간 기반 메트릭 계산 (통계 및 이동 평균)"""
    return (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(
            F.window("timestamp", "1 minute", "30 seconds"),
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
        .filter(
            (F.col("window.start") > F.lit("2024-01-01").cast("timestamp")) &
            (F.col("avg_price") > 0)
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
        return F.avg(F.when(F.col("region") == region, F.col("trade_price")))
    
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
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(F.window("timestamp", "5 minutes", "1 minute"), "coin_symbol")
        .agg(
            avg_region_price("korea").alias("kr_price"),
            avg_region_price("asia").alias("asia_price"),
            avg_region_price("ne").alias("global_price"),
        )
        .filter(
            (F.col("window.start") > F.lit("2024-01-01").cast("timestamp")) &
            (F.col("kr_price").isNotNull() | F.col("asia_price").isNotNull() | F.col("global_price").isNotNull())
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



def calculate_market_arbitrage(df: DataFrame) -> DataFrame:
    """거래소별 차익 계산"""
    
    def avg_market_price(market: str) -> Column:
        """거래소 평균 가격 계산 함수"""
        return F.avg(F.when(F.col("market") == market, F.col("trade_price")))
    
    def market_spread(first_market: str, second_market: str) -> Column:
        """거래소간 가격 차이 계산 함수"""
        first_price = F.col(f"{first_market}_price")
        second_price = F.col(f"{second_market}_price")
        return F.round(
            (first_price - second_price) / second_price * 100,
            2,
        )

    return (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(F.window("timestamp", "5 minutes", "1 minute"), "coin_symbol")
        .agg(
            # 한국 거래소
            avg_market_price("UPBIT").alias("UPBIT_price"),
            avg_market_price("BITHUMB").alias("BITHUMB_price"),
            avg_market_price("KORBIT").alias("KORBIT_price"),
            avg_market_price("COINONE").alias("COINONE_price"),
            # 아시아 거래소
            avg_market_price("BYBIT").alias("BYBIT_price"),
            avg_market_price("GATEIO").alias("GATEIO_price"),
            avg_market_price("OKX").alias("OKX_price"),
            # 글로벌 거래소
            avg_market_price("BINANCE").alias("BINANCE_price"),
            avg_market_price("KRAKEN").alias("KRAKEN_price"),
        )
        .filter(F.col("window.start") > F.lit("2024-01-01").cast("timestamp"))
        .select(
            "coin_symbol",
            "window.start",
            "window.end",
            # 기본 가격 정보
            "UPBIT_price", "BITHUMB_price", "KORBIT_price", "COINONE_price",
            "BYBIT_price", "GATEIO_price", "OKX_price",
            "BINANCE_price", "KRAKEN_price",
            # 한국 거래소간 차이
            market_spread("UPBIT", "BITHUMB").alias("UPBIT_BITHUMB_spread"),
            market_spread("UPBIT", "KORBIT").alias("UPBIT_KORBIT_spread"),
            market_spread("UPBIT", "COINONE").alias("UPBIT_COINONE_spread"),
            # 아시아 거래소간 차이
            market_spread("BYBIT", "GATEIO").alias("BYBIT_GATEIO_spread"),
            market_spread("BYBIT", "OKX").alias("BYBIT_OKX_spread"),
            # 글로벌 거래소간 차이
            market_spread("BINANCE", "KRAKEN").alias("BINANCE_KRAKEN_spread"),
            # 대표 거래소간 차이 (UPBIT vs BINANCE vs BYBIT)
            market_spread("UPBIT", "BINANCE").alias("UPBIT_BINANCE_spread"),
            market_spread("UPBIT", "BYBIT").alias("UPBIT_BYBIT_spread"),
            market_spread("BINANCE", "BYBIT").alias("BINANCE_BYBIT_spread"),
        )
    )


def detect_price_volume_signals(df: DataFrame) -> DataFrame:
    """실시간 가격 변동성과 거래량 급증 감지"""
    def calc_change_percent(col: Column) -> Column:
        """변화율 계산 함수"""
        return F.round(
            ((F.last(col) - F.first(col)) / F.first(col) * 100),
            2
        )
    
    return (
        df.withWatermark("timestamp", "5 minutes")
        .groupBy(
            F.window("timestamp", "5 minutes", "1 minute"),
            "coin_symbol",
            "market",
            "region",
        )
        .agg(
            # 기본 통계
            F.first("trade_price").alias("current_price"),
            F.first("acc_trade_volume_24h").alias("current_volume"),
            
            # 가격 변화율 계산
            calc_change_percent(F.col("trade_price")).alias("price_change_percent"),
            
            # 거래량 변화율 계산 - 수정된 부분
            calc_change_percent(F.col("acc_trade_volume_24h")).alias("volume_change_percent"),

            # 추가 지표
            F.count("trade_price").alias("trade_count"),
            F.round(F.stddev("trade_price"), 2).alias("price_volatility"),
        )
        .filter(
            (F.col("window.start") > F.lit("2024-01-01").cast("timestamp")) &
            (F.col("current_price") > 0)
        )
        .select(
            "coin_symbol",
            "market",
            "region",
            "window.start",
            "window.end",
            "current_price",
            "current_volume",
            "price_change_percent",
            "volume_change_percent",
            "trade_count",
            "price_volatility",
            # 가격 신호
            F.when(
                F.abs(F.col("price_change_percent")) >= F.lit(2.0),
                F.concat(
                    F.lit("PRICE_ALERT: "),
                    F.when(F.col("price_change_percent") > 0, F.lit("▲")).otherwise(F.lit("▼")),
                    F.col("price_change_percent").cast("string"),
                    F.lit("%")
                )
            ).otherwise(F.lit(0)).alias("price_signal"),
            
            # 거래량 신호 
            F.when(
                F.col("volume_change_percent") >= F.lit(2.8),
                F.concat(
                    F.lit("VOLUME_SURGE: "),
                    F.lit("▲"),
                    F.col("volume_change_percent").cast("string"),
                    F.lit("%")
                )
            ).otherwise(0).alias("volume_signal")
        )
    )
