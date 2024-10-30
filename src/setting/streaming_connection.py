"""
Spark streaming coin average price 
"""

from __future__ import annotations
from typing import Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from src.schema.preprocess_schema import (
    time_metrics_schema,
    arbitrage_schema,
    orderbook_cal_schema,
    orderbook_cal_all_schema,
)
from src.config.properties import KAFKA_BOOTSTRAP_SERVERS, SPARK_PACKAGE
from src.schema.abstruct_class import AbstructSparkSettingOrganization
from src.storage.mysql_sink import write_to_mysql
from src.setting.coin_cal_query import (
    TickerQueryOrganization,
    OrderbookQueryOrganization,
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
            .config("spark.kafka.consumer.cache.capacity", "150")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config(
                "spark.streaming.kafka.consumer.config.auto.offset.reset", "earliest"
            )
            # .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
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
        self._streaming_kafka_session = self._stream_kafka_session()
        self.schema = schema
        self.to_json_struct = "to_json(struct(*)) AS value"

    def _process_select_expr(self, process: DataFrame) -> DataFrame:
        return process.selectExpr(self.to_json_struct)

    def _write_to_mysql(self, data: DataFrame, table_name: str, schema: StructType):
        def saving_to_mysql_query(data: DataFrame, schema: StructType) -> DataFrame:
            return data.select(
                F.from_json("value", schema=schema).alias("data")
            ).select("data.*")

        p_data: DataFrame = saving_to_mysql_query(data, schema)
        return write_to_mysql(p_data, table_name)

    def _process_and_send(
        self,
        data: DataFrame,
        topic: str,
        table_name: str,
        schema: StructType,
    ) -> tuple:
        """데이터 처리 및 전송을 위한 헬퍼 메서드"""
        processed_data = self._process_select_expr(data)
        mysql_sink = self._write_to_mysql(processed_data, table_name, schema)
        kafka_sink = self._topic_to_kafka_sending(processed_data, topic)
        return mysql_sink, kafka_sink

    def _await_all(self, *sinks) -> None:
        """모든 싱크의 종료를 기다림"""
        for sink in sinks:
            sink.awaitTermination()

    def ticker_spark_streaming(self) -> None:
        """시계열 지표와 차익 계산 스트리밍"""
        ticker = TickerQueryOrganization(self._streaming_kafka_session, self.schema)

        # 시계열 지표 처리
        time_metrics_mysql, time_metrics_kafka = self._process_and_send(
            ticker.cal_time_based_metrics(),
            "TimeMetricsProcessedCoin",
            "time_metrics",
            time_metrics_schema,
        )

        # 차익 계산 처리
        arbitrage_mysql, arbitrage_kafka = self._process_and_send(
            ticker.cal_arbitrage(),
            "ArbitrageProcessedCoin",
            "arbitrage",
            arbitrage_schema,
        )

        self._await_all(
            time_metrics_mysql, time_metrics_kafka, arbitrage_mysql, arbitrage_kafka
        )

    def orderbook_spark_streaming(self) -> None:
        """오더북 데이터 스트리밍"""
        orderbook = OrderbookQueryOrganization(
            self._streaming_kafka_session, self.schema
        )

        # 전체 지역 통계 처리
        all_regions_mysql, all_regions_kafka = self._process_and_send(
            orderbook.cal_all_regions_stats(),
            "OrderbookProcessedAllRegionCoin",
            "order_cal_all",
            orderbook_cal_all_schema,
        )

        # 개별 지역 통계 처리
        region_mysql, region_kafka = self._process_and_send(
            orderbook.cal_region_stats(),
            "OrderbookProcessedRegionCoin",
            "order_cal_region",
            orderbook_cal_schema,
        )

        self._await_all(
            all_regions_mysql,
            all_regions_kafka,
            region_mysql,
            region_kafka,
        )
