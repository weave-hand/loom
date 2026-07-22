"""Arrow IPC stream encoding for the ingest write surface.

loom's ingest endpoints (`POST /datasets/{schema}/{table}`, `POST
/models/{type}`) accept the request body as an Arrow IPC **stream** (not
the file/random-access format) with content type
`application/vnd.apache.arrow.stream`. This module is the only place that
builds those bytes.
"""

from __future__ import annotations

import io

import pyarrow as pa


def to_ipc(data: pa.Table | pa.RecordBatch | list[dict], schema: pa.Schema | None = None) -> bytes:
    """Encode `data` as Arrow IPC stream bytes.

    `list[dict]` rows are converted via `pa.Table.from_pylist(data,
    schema=schema)` (pass `schema` to pin column types loom won't infer
    correctly on its own, e.g. date/timestamp columns on a bootstrap land).
    A `pa.RecordBatch` is wrapped into a single-batch `pa.Table` first.
    """
    if isinstance(data, pa.Table):
        table = data
    elif isinstance(data, pa.RecordBatch):
        table = pa.Table.from_batches([data])
    else:
        table = pa.Table.from_pylist(data, schema=schema)

    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def empty_ipc(schema: pa.Schema) -> bytes:
    """Encode a zero-row Arrow IPC stream carrying only `schema`.

    Used for the zero-row dataset bootstrap: landing an empty stream still
    commits a snapshot and records the column schema (paired with the
    `X-Loom-Model` header when the schema includes date/timestamp columns,
    which inference alone cannot type).
    """
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(schema.empty_table())
    return sink.getvalue()
