import csv
import datetime
import io
import ssl
import urllib.request

from src.model.row import RowToInsert


class RemoteCSVFile:

    def __init__(self, url: str):
        self.__generator = self.__data_rows_generator(url)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.__generator)

    def __data_rows_generator(self, url: str):
        ctx = RemoteCSVFile.__get_disabled_ssl_context()
        with urllib.request.urlopen(url, context=ctx) as binary_file:
            text_file = io.TextIOWrapper(binary_file, encoding='utf-8')
            header = text_file.readline().strip()
            fieldnames = [name.strip('<>').lower() for name in header.split(';')]
            rowsreader = csv.DictReader(text_file, fieldnames=fieldnames, delimiter=';')
            for row in rowsreader:
                try:
                    model_row = RemoteCSVFile.__dict_to_row(row)
                    yield model_row
                except KeyError:
                    pass
                except Exception:
                    pass


            # row['date'], row['open'], row['high'], row['low'], row['close'], row['vol']

    @staticmethod
    def __dict_to_row(row: dict) -> RowToInsert:
        return RowToInsert(
            dt=datetime.datetime.strptime(row['date'], '%Y%m%d').date(),
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
