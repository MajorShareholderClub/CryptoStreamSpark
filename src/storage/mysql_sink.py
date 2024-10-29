from pyspark.sql import DataFrame
from src.config.properties import COIN_MYSQL_URL, COIN_MYSQL_USER, COIN_MYSQL_PASSWORD


def write_to_mysql(data: DataFrame, table_name: str):
    """
    Function Args:
        - data_format (DataFrame): 저장할 데이터 포맷
        - table_name (str): 체크포인트 저장할 테이블 이름
            - ex) .checkpoint_{table_name}

    MySQL Setting Args (_write_batch_to_mysql):
        - url : JDBC MYSQL connention URL
        - driver : com.mysql.cj.jdbc.Driver
        - dbtable : table
        - user : user
        - password: password
        - mode : append
            - 추가로 들어오면 바로 넣기

    - trigger(processingTime="1 minute")
    """
    checkpoint_dir: str = f".checkpoint_{table_name}"

    def _write_batch_to_mysql(batch_df: DataFrame, batch_id) -> None:
        (
            batch_df.write.format("jdbc")
            .option("url", COIN_MYSQL_URL)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", COIN_MYSQL_USER)
            .option("password", COIN_MYSQL_PASSWORD)
            .mode("append")
            .save()
        )

    return (
        data.writeStream.outputMode("update")
        .foreachBatch(_write_batch_to_mysql)
        .option("checkpointLocation", f"checkpoint/mysql_checkpoint/{checkpoint_dir}")
        .trigger(processingTime="1 minute")
        .start()
    )
