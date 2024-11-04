"""
API에 필요한것들
"""

import configparser
from pathlib import Path


path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/config/setting.conf")

KAFKA_BOOTSTRAP_SERVERS = parser.get("KAFKASPARK", "KAFKA_BOOTSTRAP_SERVERS")
SPARK_PACKAGE = parser.get("KAFKASPARK", "SPARK_PACKAGE")


# ------------------------------------------------------------------------------
# Coin setting
# ------------------------------------------------------------------------------
Ticker_Topic_Name = parser.get("REALTIMETOPICNAME", "Ticker_Topic_Name")
Orderbook_Topic_Name = parser.get("REALTIMETOPICNAME", "Orderbook_Topic_Name")


COIN_MYSQL_URL = parser.get("MYSQL", "COIN_MYSQL_URL")
COIN_MYSQL_USER = parser.get("MYSQL", "COIN_MYSQL_USER")
COIN_MYSQL_PASSWORD = parser.get("MYSQL", "COIN_MYSQL_PASSWORD")

MINIO_ACCESS_KEY_ID = parser.get("MINIO", "MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = parser.get("MINIO", "MINIO_SECRET_ACCESS_KEY")
