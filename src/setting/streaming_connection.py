"""
Spark streaming coin average price 
"""

from __future__ import annotations
from typing import Any
from pyspark.sql import SparkSession, DataFrame

import yaml
from src.schema.abstruct_class import AbstructSparkSettingOrganization
from src.setting.coin_cal_query import (
    TickerQueryOrganization,
    OrderbookQueryOrganization,
)
from src.config.properties import (
    KAFKA_BOOTSTRAP_SERVERS,
    SPARK_PACKAGE,
    COIN_MYSQL_URL,
    COIN_MYSQL_USER,
    COIN_MYSQL_PASSWORD,
)


class _SparkSettingOrganization(AbstructSparkSettingOrganization):
    """SparkSession Setting 모음"""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self._spark: SparkSession = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Spark Session Args:
            - spark.jars.packages : 패키지
                - 2024년 9월 28일 기준 : Kafka-connect, mysql-connector
            - spark.streaming.stopGracefullyOnShutdown : 우아하게 종료 처리
            - spark.streaming.backpressure.enabled : 유압 밸브
            - spark.streaming.kafka.consumer.config.auto.offset.reset : kafka 스트리밍 경우 오프셋이 없을때 최신 메시지 부터 처리
            - spark.sql.adaptive.enabled : SQL 실행 계획 최적화
            - spark.executor.memory : Excutor 할당되는 메모리 크기를 설정
            - spark.executor.cores : Excutor 할당되는 코어 수 설정
            - spark.cores.max : Spark 에서 사용할 수 있는 최대 코어 수
        """
        spark = (
            SparkSession.builder.appName("coin")
            .master("local[*]")
            .config("spark.jars.packages", f"{SPARK_PACKAGE}")
            # .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            # .config("spark.kafka.consumer.cache.capacity", "")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config(
                "spark.streaming.kafka.consumer.config.auto.offset.reset", "earliest"
            )
            .config(
                "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"
            )
            .config("spark.sql.session.timeZone", "Asia/Seoul")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "16")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def _stream_kafka_session(self) -> DataFrame:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
        """
        return (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("subscribe", f"{self.topic}")
            .option("startingOffsets", "earliest")
            .load()
        )

    def _topic_to_kafka_sending(self, data_format: DataFrame, retrieve_topic: str):
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
            - checkpointLocation: 체크포인트
            - value.serializer: 직렬화 종류
        """
        checkpoint_dir: str = f".checkpoint_{retrieve_topic}"

        return (
            data_format.writeStream.outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", retrieve_topic)
            .option("kafka.acks", "all")
            .option("kafka.retries", "3")
            .option("checkpointLocation", f"checkpoint/{checkpoint_dir}")
            .option("startingOffsets", "earliest")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .start()
        )


class SparkStreamingCoinAverage(_SparkSettingOrganization):
    """
    데이터 처리 클래스
    """

    def __init__(self, topics: str, schema: Any) -> None:
        """
        Args:
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
        """
        super().__init__(topic=topics)
        self._streaming_kafka_session: DataFrame = self._stream_kafka_session()
        self.schema = schema

    def ticker_spark_streaming(self) -> None:
        """
        Spark Streaming 실행 함수 - 여러 쿼리를 동시에 실행하고 관리하고 카프카로 전송
        """
        # 공통 설정

        # # debug 용
        # time_metrics_console = (
        #     spark_struct.cal_time_based_metrics()
        #     .writeStream.outputMode("update")
        #     .format("console")
        #     .option("truncate", "false")
        #     .start()
        # )
        # arbitrage_console = (
        #     arbitrage_df.writeStream.outputMode("update")
        #     .format("console")
        #     .option("truncate", "false")
        #     .start()
        # )
        ticker = TickerQueryOrganization(self._streaming_kafka_session, self.schema)
        # 시계열 지표 쿼리 및 카프카 전송
        time_metrics_kafka = self._topic_to_kafka_sending(
            data_format=ticker.cal_time_based_metrics().selectExpr(
                "to_json(struct(*)) AS value"
            ),
            retrieve_topic="TimeMetricsProcessedCoin",
        )

        arbitrage_kafka = self._topic_to_kafka_sending(
            data_format=ticker.cal_arbitrage().selectExpr(
                "to_json(struct(*)) AS value"
            ),
            retrieve_topic="ArbitrageProcessedCoin",
        )

        time_metrics_kafka.awaitTermination()
        arbitrage_kafka.awaitTermination()

    def orderbook_spark_streaming(self) -> None:
        """
        오더북 Spark Streaming 실행 함수 - 오더북 쿼리를 실행하고 카프카로 전송
        """
        orderbook = OrderbookQueryOrganization(
            self._streaming_kafka_session, self.schema
        )
        # 모든 지역 region orderbook 카프카 전송
        orderbook_cal_all_regions_kafka = self._topic_to_kafka_sending(
            data_format=orderbook.cal_all_regions_stats().selectExpr(
                "to_json(struct(*)) AS value"
            ),
            retrieve_topic="OrderbookProcessedAllRegionCoin",
        )

        # 각 지역 region orderbook 카프카 전송
        orderbook_cal_region_kafka = self._topic_to_kafka_sending(
            data_format=orderbook.cal_region_stats().selectExpr(
                "to_json(struct(*)) AS value"
            ),
            retrieve_topic="OrderbookProcessedRegionCoin",
        )

        orderbook_cal_all_regions_kafka.awaitTermination()
        orderbook_cal_region_kafka.awaitTermination()
