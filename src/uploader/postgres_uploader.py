import itertools
import logging
from typing import Iterable, Tuple

import psycopg
import pypika
from psycopg import sql
from pypika import Table, PostgreSQLQuery

from model.row import RowToInsert

logger = logging.getLogger(__name__)


class PostgresUploader:
    def __init__(self, conn: psycopg.connection.Connection, table_name: str, batch_size: int):
        self.__conn = conn
        self.__batch_size = batch_size
        self.__tbl = Table(table_name)

    def load(self, rows: Iterable[RowToInsert]):
        batched: Iterable[Tuple[RowToInsert]] = itertools.batched(rows, self.__batch_size)
        with self.__conn.cursor() as cur:
            for batch in batched:
                if len(batch) == 0:
                    continue
                if logger.level <= logging.DEBUG:
                    logger.debug(f"processing {len(batch)} lines")
                query = self.__create_upsert_query(batch)
                try:
                    with self.__conn.transaction():
                        cur.execute(query)
                except Exception as e:
                    logger.error(e)
                else:
                    logger.debug("lines inserted successfully")

    def __create_upsert_query(self, batch: tuple[RowToInsert]) -> sql.SQL:
        q = PostgreSQLQuery.into(self.__tbl)
        for row in batch:
            q = q.insert(row.dt.strftime('%y%m%d'), row.open, row.high, row.low, row.close, row.vol)
        q = self.__add_on_conflict_statement(q)
        return sql.SQL(q.get_sql())

    def __add_on_conflict_statement(self,
                                    q: pypika.dialects.PostgreSQLQueryBuilder) -> pypika.dialects.PostgreSQLQueryBuilder:
        return q.on_conflict(self.__tbl.dt) \
            .do_update(self.__tbl.open) \
            .do_update(self.__tbl.high) \
            .do_update(self.__tbl.low) \
            .do_update(self.__tbl.close) \
            .do_update(self.__tbl.volume)
