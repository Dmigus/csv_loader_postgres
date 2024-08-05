import csv
import datetime
import io
import ssl
import logging
import urllib.request

from model.row import RowToInsert

logger = logging.getLogger(__name__)


class RemoteCSVFile:

    def __init__(self, url: str):
        self.__generator = RemoteCSVFile.__data_rows_generator(url)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.__generator)

    @staticmethod
    def __data_rows_generator(url: str):
        ctx = RemoteCSVFile.__get_disabled_ssl_context()
        with urllib.request.urlopen(url, context=ctx) as binary_file:
            text_file = io.TextIOWrapper(binary_file, encoding='utf-8')
            header = text_file.readline().strip()
            fieldnames = [name.strip('<>').lower() for name in header.split(';')]
            rows_reader = csv.DictReader(text_file, fieldnames=fieldnames, delimiter=';')
            for row in rows_reader:
                try:
                    model_row = RemoteCSVFile.__dict_to_row(row)
                    if logger.level < logging.DEBUG:
                        logger.log(logging.NOTSET, "csv row parsed to model: " + str(model_row))
                    yield model_row
                except KeyError as ke:
                    logger.error("One of fields are missing" + str(ke))
                except Exception as e:
                    logger.error("Could not parse csv row to model: " + str(e))

    @staticmethod
    def __dict_to_row(row: dict) -> RowToInsert:
        return RowToInsert(
            dt=datetime.datetime.strptime(row['date'], '%y%m%d').date(),
            open=float(row['open']),
            high=float(row['high']),
            low=float(row['low']),
            close=float(row['close']),
            vol=int(row['vol'])
        )

    @staticmethod
    def __get_disabled_ssl_context():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
