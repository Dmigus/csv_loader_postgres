import os
import logging
import sys

import psycopg

from datasource.remote_csv_file import RemoteCSVFile
from uploader.postgres_uploader import PostgresUploader

logger = logging.getLogger(__name__)


def get_logger_level() -> int:
    match os.environ.get("LOG_LEVEL", "INFO"):
        case "TRACE":
            return logging.NOTSET
        case "DEBUG":
            return logging.DEBUG
        case "INFO":
            return logging.INFO
        case "FATAL":
            return logging.FATAL


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=get_logger_level())
    file_url = os.environ['FILE_URL']
    data_source = RemoteCSVFile(file_url)
    dsn = os.environ['DATABASE_URL']
    dest_table = os.environ['DESTINATION_TABLE']
    batch_size = int(os.environ['BATCH_SIZE'])
    try:
        with psycopg.connect(dsn) as conn:
            loader = PostgresUploader(conn, dest_table, batch_size)
            loader.load(data_source)
    except Exception as e:
        logger.fatal(e)
    else:
        logger.info("Processing completed successful")
