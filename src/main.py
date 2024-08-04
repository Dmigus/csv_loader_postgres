import psycopg

from src.datasource.remote_csv_file import RemoteCSVFile
from src.uploader.postgres_uploader import PostgresUploader


if __name__ == "__main__":
    file_url = "https://drive.google.com/uc?export=download&id=1JGz6XWXtOWRHtN2p2A37m25lGkhag0Mx"
    data_source = RemoteCSVFile(file_url)
    dsn = "host=localhost dbname=postgres user=postgres password=mysecretpassword port=5432"
    dest_table = 'dataset'
    batch_size = 10
    with psycopg.connect(dsn) as conn:
        loader = PostgresUploader(conn, dest_table, batch_size)
        loader.load(data_source)
