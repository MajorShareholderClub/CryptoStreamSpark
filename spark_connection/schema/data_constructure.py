"""
pyspark data schema
{
    "upbit": {
        "name": "upbit-ETH",
        "timestamp": 1689633864.89345,
        "data": {
            "opening_price": 2455000.0,
            "max_price": 2462000.0,
            "min_price": 2431000.0,
            "prev_closing_price": 2455000.0,
            "acc_trade_volume_24h": 11447.92825886,
        }
    },
    .....
}

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


data_schema = StructType(
    [
        StructField("opening_price", StringType(), True),
        StructField("trade_price", StringType(), True),
        StructField("max_price", StringType(), True),
        StructField("min_price", StringType(), True),
        StructField("prev_closing_price", StringType(), True),
        StructField("acc_trade_volume_24h", StringType(), True),
    ]
)

market_schema = StructType(
    [
        StructField("market", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("coin_symbol", StringType(), True),
        StructField("data", data_schema),
    ]
)

korea_schema = StructType(
    [
        StructField("upbit", market_schema, True),
        StructField("bithumb", market_schema, True),
        StructField("coinone", market_schema, True),
        StructField("korbit", market_schema, True),
    ]
)

foreign_schema = StructType(
    [
        StructField("binance", market_schema, True),
        StructField("kraken", market_schema, True),
        StructField("okx", market_schema, True),
        StructField("bybit", market_schema, True),
        StructField("gateio", market_schema, True),
    ]
)

"""
# 평균값
{
    "name": "ETH",
    "timestamp": 1689633864.89345,
    "data": {
        "opening_price": 2455000.0,
        "max_price": 2462000.0,
        "min_price": 2431000.0,
        "prev_closing_price": 2455000.0,
        "acc_trade_volume_24h": 11447.92825886,
    }
}
"""
average_schema = StructType(
    StructType(
        [
            StructField("name", StringType()),
            StructField("time", LongType()),
            StructField("data", data_schema),
        ]
    ),
)


def average_price_schema(type_: str) -> StructType:

    return StructType(
        [
            StructField(
                type_,
                StructType(average_schema),
            )
        ]
    )
