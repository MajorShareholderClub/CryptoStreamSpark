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
    market_arbitrage_schema,
    market_arbitrage_signal_schema,
    orderbook_cal_schema,
    orderbook_cal_all_schema,
)
from src.schema.abstruct_class import AbstructSparkSettingOrganization
from src.storage.mysql_sink import write_to_mysql
from src.setting.coin_cal_query import (
    TickerQueryOrganization,
    OrderbookQueryOrganization,
)
from src.config.properties import (
    KAFKA_BOOTSTRAP_SERVERS,
    SPARK_PACKAGE,
    MINIO_ACCESS_KEY_ID,
    MINIO_SECRET_ACCESS_KEY,
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
            - spark.sql.streaming.checkpointFileManagerClass : MinIO의 체크포인트 매니저를 사용하도록 설정
        """
        # fmt: off

        spark = (
            SparkSession.builder
            .appName("coin")
            .master("local[*]")
            .config("spark.jars.packages", f"{SPARK_PACKAGE}")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            # .config("spark.hadoop.fs.s3a.access.key", f"{MINIO_ACCESS_KEY_ID}")
            # .config("spark.hadoop.fs.s3a.secret.key", f"{MINIO_SECRET_ACCESS_KEY}")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.kafka.consumer.cache.capacity", "150")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "earliest")
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

    def _topic_to_kafka_sending(
        self, data_format: DataFrame, retrieve_topic: str, stream_name: str
    ):
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
            - checkpointLocation: 체크포인트
            - value.serializer: 직렬화 종류
        """
        checkpoint_dir: str = (
            f"checkpoint/{stream_name}/.kafka_checkpoint/.checkpoint_{retrieve_topic}"
        )
        minio_path: str = f"s3a://streaming-checkpoint/{checkpoint_dir}"

        return (
            data_format.writeStream.outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", retrieve_topic)
            .option("kafka.acks", "all")
            .option("kafka.retries", "3")
            .option("checkpointLocation", f"{checkpoint_dir}")
            .option("startingOffsets", "earliest")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .option("failOnDataLoss", "false")
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
        self.schema = schema
        self.to_json_struct = "to_json(struct(*)) AS value"

    def _process_select_expr(self, process: DataFrame) -> DataFrame:
        return process.selectExpr(self.to_json_struct)

    def _write_to_mysql(
        self, data: DataFrame, table_name: str, stream_name: str, schema: StructType
    ):
        def saving_to_mysql_query(data: DataFrame, schema: StructType) -> DataFrame:
            return data.select(
                F.from_json("value", schema=schema).alias("data")
            ).select("data.*")

        p_data: DataFrame = saving_to_mysql_query(data, schema)
        return write_to_mysql(p_data, table_name, stream_name)

    def _debug_print(self, data: DataFrame) -> None:
        """디버깅을 위한 콘솔 출력"""
        return (
            data.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .start()
            .awaitTermination()
        )

    def _process_and_send(
        self,
        data: DataFrame,
        topic: str,
        stream_name: str,
        table_name: str,
        schema: StructType,
    ) -> tuple:
        """데이터 처리 및 전송을 위한 헬퍼 메서드"""
        processed_data = self._process_select_expr(data)
        mysql_sink = self._write_to_mysql(
            data=processed_data,
            table_name=table_name,
            stream_name=stream_name,
            schema=schema,
        )
        kafka_sink = self._topic_to_kafka_sending(
            data_format=processed_data,
            retrieve_topic=topic,
            stream_name=stream_name,
        )
        return mysql_sink, kafka_sink

    def _await_all(self, *sinks) -> None:
        """모든 싱크의 종료를 기다림"""
        for sink in sinks:
            sink.awaitTermination()


class SparkStreamingTicker(SparkStreamingCoinAverage):
    def __init__(self, topics: str, schema: Any) -> None:
        super().__init__(topics, schema)
        self._streaming_kafka_session = self._stream_kafka_session()

    def ticker_spark_streaming(self) -> None:
        """시계열 지표와 차익 계산 스트리밍"""
        ticker = TickerQueryOrganization(self._streaming_kafka_session, self.schema)

        # 시계열 지표 처리
        time_metrics_mysql, time_metrics_kafka = self._process_and_send(
            data=ticker.cal_time_based_metrics(),
            topic="TimeMetricsProcessedCoin",
            stream_name="Ticker",
            table_name="time_metrics",
            schema=time_metrics_schema,
        )

        # 차익 계산 처리
        arbitrage_mysql, arbitrage_kafka = self._process_and_send(
            data=ticker.cal_arbitrage(),
            topic="ArbitrageProcessedCoin",
            stream_name="Ticker",
            table_name="arbitrage",
            schema=arbitrage_schema,
        )

        # 시장 차익 계산 처리
        market_arbitrage_mysql, market_arbitrage_kafka = self._process_and_send(
            data=ticker.cal_market_arbitrage(),
            topic="MarketArbitrageProcessedCoin",
            stream_name="Ticker",
            table_name="market_arbitrage",
            schema=market_arbitrage_schema,
        )

        # 시장 차익 신호 처리
        market_arbitrage_signal_mysql, market_arbitrage_signal_kafka = (
            self._process_and_send(
                data=ticker.cal_market_arbitrage_signal(),
                topic="MarketArbitrageSignalProcessedCoin",
                stream_name="Ticker",
                table_name="market_arbitrage_signal",
                schema=market_arbitrage_signal_schema,
            )
        )

        self._await_all(
            time_metrics_mysql,
            time_metrics_kafka,
            arbitrage_mysql,
            arbitrage_kafka,
            market_arbitrage_mysql,
            market_arbitrage_kafka,
            market_arbitrage_signal_mysql,
            market_arbitrage_signal_kafka,
        )

    def debug_ticker_streaming(self) -> None:
        """디버깅을 위한 시계열 지표와 차익 계산 스트리밍"""
        ticker = TickerQueryOrganization(self._streaming_kafka_session, self.schema)

        # 시계열 지표 디버깅
        time_metrics_data = ticker.cal_market_arbitrage_signal()
        self._debug_print(time_metrics_data)

        # # 차익 계산 디버깅
        # arbitrage_data = ticker.cal_arbitrage()
        # self._debug_print(arbitrage_data)


class SparkStreamingOrderbook(SparkStreamingCoinAverage):
    def __init__(self, topics: str, schema: Any) -> None:
        super().__init__(topics, schema)
        self._streaming_kafka_session = self._stream_kafka_session()

    def orderbook_spark_streaming(self) -> None:
        """오더북 데이터 스트리밍"""
        orderbook = OrderbookQueryOrganization(
            self._streaming_kafka_session, self.schema
        )

        # 전체 지역 통계 처리
        all_regions_mysql, all_regions_kafka = self._process_and_send(
            data=orderbook.cal_all_regions_stats(),
            topic="OrderbookProcessedAllRegionCoin",
            stream_name="Orderbook",
            table_name="order_cal_all",
            schema=orderbook_cal_all_schema,
        )

        # 개별 지역 통계 처리
        region_mysql, region_kafka = self._process_and_send(
            orderbook.cal_region_stats(),
            topic="OrderbookProcessedRegionCoin",
            stream_name="Orderbook",
            table_name="order_cal_region",
            schema=orderbook_cal_schema,
        )

        self._await_all(
            all_regions_mysql,
            all_regions_kafka,
            region_mysql,
            region_kafka,
        )

    def debug_orderbook_streaming(self) -> None:
        """디버깅을 위한 오더북 데이터 스트리밍"""
        orderbook = OrderbookQueryOrganization(
            self._streaming_kafka_session, self.schema
        )

        # 전체 지역 통계 디버깅
        all_regions_data = orderbook.cal_region_stats()
        self._debug_print(all_regions_data)

        # # 개별 지역 통계 디버깅
        region_data = orderbook.cal_region_stats()
        self._debug_print(region_data)
