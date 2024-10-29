from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    FloatType,
    TimestampType,
    IntegerType,
)


time_metrics_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("coin_symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_price", DoubleType(), True),
        StructField("highest_price", DoubleType(), True),
        StructField("lowest_price", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("price_volatility", FloatType(), True),
        StructField("MA_price", DoubleType(), True),
        StructField("MA_volume", DoubleType(), True),
    ]
)


arbitrage_schema = StructType(
    [
        StructField("coin_symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("kr_price", DoubleType(), True),
        StructField("global_price", DoubleType(), True),
        StructField("asia_price", DoubleType(), True),
        StructField("kr_global_spread", DoubleType(), True),
        StructField("kr_asia_spread", DoubleType(), True),
        StructField("global_asia_spread", DoubleType(), True),
        StructField("kr_global_spread_percent", DoubleType(), True),
        StructField("kr_asia_spread_percent", DoubleType(), True),
        StructField("global_asia_spread_percent", DoubleType(), True),
    ]
)


orderbook_cal_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_highest_bid", DoubleType(), True),
        StructField("avg_lowest_ask", DoubleType(), True),
        StructField("total_bid_volume", DoubleType(), True),
        StructField("total_ask_volume", DoubleType(), True),
        StructField("record_count", IntegerType(), True),
    ]
)

orderbook_cal_all_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_highest_bid", DoubleType(), True),
        StructField("avg_lowest_ask", DoubleType(), True),
        StructField("total_bid_volume", DoubleType(), True),
        StructField("total_ask_volume", DoubleType(), True),
        StructField("record_count", IntegerType(), True),
    ]
)
