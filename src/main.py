import os

import psycopg

from datasource.remote_csv_file import RemoteCSVFile
from uploader.postgres_uploader import PostgresUploader


if __name__ == "__main__":
    file_url = os.environ['FILE_URL']
    data_source = RemoteCSVFile(file_url)
    dsn = os.environ['DATABASE_URL']
    dest_table = os.environ['DESTINATION_TABLE']
    batch_size = int(os.environ['BATCH_SIZE'])
    with psycopg.connect(dsn) as conn:
        loader = PostgresUploader(conn, dest_table, batch_size)
        loader.load(data_source)
