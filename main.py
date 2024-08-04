import itertools
import urllib.request
import ssl
import csv
import io
import psycopg
import pypika.functions
from pypika import Query, Table, PostgreSQLQuery

# file_url = "https://cloclo63.cloud.mail.ru/public/249m8NwuQBuscKASLpJq/g/no/L1xB/nvgHGYJz5"
file_url = "https://drive.google.com/uc?export=download&id=1JGz6XWXtOWRHtN2p2A37m25lGkhag0Mx"


def data_rows():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(file_url, context=ctx) as binary_file:
        # while True:
        #     readed = binary_file.read(amt=1000)
        #     if len(readed) == 0:
        #         break
        #     print(readed)
        text_file = io.TextIOWrapper(binary_file, encoding='utf-8')
        header = text_file.readline().strip()
        fieldnames = [name.strip('<>').lower() for name in header.split(';')]
        datareader = csv.DictReader(text_file, fieldnames=fieldnames, delimiter=';')
        yield from datareader


batch_size = 10


def on_conflict_statement(q: pypika.dialects.PostgreSQLQueryBuilder,
                            tbl: pypika.queries.Table) -> pypika.dialects.PostgreSQLQueryBuilder:
    return q.on_conflict(tbl.dt) \
        .do_update(tbl.open) \
        .do_update(tbl.high) \
        .do_update(tbl.low) \
        .do_update(tbl.close) \
        .do_update(tbl.volume)


if __name__ == "__main__":
    rows = data_rows()
    batched = itertools.batched(rows, batch_size)
    tbl = Table('dataset')
    with psycopg.connect(
            "host=localhost dbname=postgres user=postgres password=mysecretpassword port=5432",
            autocommit=True) as conn:
        with conn.cursor() as cur:
            for batch in batched:
                if len(batch) == 0:
                    continue
                q = PostgreSQLQuery.into(tbl)
                for row in batch:
                    q = q.insert(row['date'], row['open'], row['high'], row['low'], row['close'], row['vol'])
                q = on_conflict_statement(q, tbl)
                with conn.transaction():
                    cur.execute(q.get_sql())

