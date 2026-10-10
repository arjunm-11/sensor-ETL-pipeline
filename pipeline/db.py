"""Postgres connection and bulk-load helpers."""
import io
import os
from contextlib import contextmanager

import pandas as pd
import psycopg2


def connect():
    # Defaults match docker-compose.yml for running from the host machine.
    # Inside Docker (Airflow), PGHOST is set to the service name "postgres".
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "wsn"),
        password=os.environ.get("PGPASSWORD", "wsn"),
        dbname=os.environ.get("PGDATABASE", "wsn"),
    )


@contextmanager
def transaction():
    """Yield a cursor inside one transaction: commit on success, roll back on any error."""
    conn = connect()
    try:
        with conn:  # psycopg2: commits on clean exit, rolls back on exception
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def _escape_copy_text(s: str) -> str:
    # COPY ... (FORMAT text) treats backslash, tab, newline and CR specially.
    # Escaping them keeps an arbitrary raw line byte-for-byte intact.
    return (s.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\n", "\\n").replace("\r", "\\r"))


def copy_text_rows(cur, table: str, columns: list[str], rows) -> None:
    """COPY an iterable of tuples whose values are ints or (unescaped) strings."""
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(_escape_copy_text(v) if isinstance(v, str) else str(v) for v in row))
        buf.write("\n")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN (FORMAT text)", buf)


def copy_dataframe(cur, table: str, df: pd.DataFrame) -> None:
    """
    COPY a typed DataFrame (numbers, timestamps, and our own controlled
    strings such as batch ids and reject reasons, which never contain
    tabs/newlines/backslashes). NULLs are written as \\N.
    """
    buf = io.StringIO()
    df.to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N",
              date_format="%Y-%m-%d %H:%M:%S.%f")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(df.columns)}) FROM STDIN (FORMAT text)", buf)
