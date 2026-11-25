"""
This library hold add-ons for developing Spark Structured Streaming pipelines on Databricks
"""
import shutil
import uuid
import pandas as pd
import queue
import threading
import re
from datetime import timedelta, datetime, timezone
from typing import NamedTuple, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.streaming import StatefulProcessor, StreamingQueryListener
from pyspark.sql.types import *

__all__ = ["TempCheckpointDisplay", "register_tckp_display", "TableStreamingQueryListener", "live_session_window"]

# --------------------------------------------------------
# Private Helper Functions & Variables
# --------------------------------------------------------
_UNITS_MS = {
    "millisecond": 1,
    "second": 1_000,
    "minute": 60_000,
    "hour": 3_600_000,
    "day": 86_400_000,
    "week": 604_800_000}


def _gap_to_ms(gap) -> int:
    """'10 minutes' / timedelta / int (ms) -> ms."""
    if isinstance(gap, timedelta):
        return int(gap.total_seconds() * 1000)
    if isinstance(gap, int):
        return gap
    m = re.fullmatch(r"\s*(\d+)\s*([a-z]+?)s?\s*", str(gap).lower())
    if not m or m.group(2) not in _UNITS_MS:
        raise ValueError(
            f"Unsupported gapDuration {gap!r}; use e.g. '10 minutes', a timedelta, or ms as int"
        )
    return int(m.group(1)) * _UNITS_MS[m.group(2)]


def _unwrap(c) -> str:
    """Column -> the expression text inside Column<'...'> (works in classic and Connect)."""
    if isinstance(c, str):
        return c
    m = re.fullmatch(r"Column<'(.*)'>", str(c), re.S)
    if not m:
        raise ValueError(f"Cannot interpret column expression: {c!r}")
    return m.group(1)


def _col_name(c) -> str:
    """Plain column reference -> its name."""
    name = _unwrap(c).strip("`")
    if not re.fullmatch(r"[\w.\- ]+", name):
        raise ValueError(f"{c!r} must be a plain column reference (alias expressions first)")
    return name


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _as_tuple(x):
    return (x,) if isinstance(x, (str, Column)) else tuple(x)

def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None
    

class AggSpec(NamedTuple):
    kind: str            # sum | count | min | max
    col: Optional[str]   # None => count every row (count("*") / count(lit(1)))
    out_name: str        # name as Spark would give it (alias or e.g. 'sum(amount)')
    ddl_type: str        # result type as Spark resolves it


_AGG_RE = re.compile(r"(sum|count|min|max)\((.*)\)", re.I | re.S)


def _parse_agg(c: Column, out_name: str, ddl_type: str) -> AggSpec:
    expr = re.sub(r"\s+AS\s+`?[^`]+`?\s*$", "", _unwrap(c), flags=re.I).strip()
    m = _AGG_RE.fullmatch(expr)
    if not m:
        raise ValueError(f"Unsupported aggregation {c!r}; supported: sum, count, min, max")
    kind, arg = m.group(1).lower(), m.group(2).strip()
    if arg.upper().startswith("DISTINCT"):
        raise ValueError(f"DISTINCT aggregations are not supported: {c!r}")
    if kind == "count" and (arg in ("*", "1")):
        return AggSpec(kind, None, out_name, ddl_type)
    arg = arg.strip("`")
    if not re.fullmatch(r"[\w.\- ]+", arg):
        raise ValueError(f"Aggregation input must be a plain column (got {arg!r} in {c!r})")
    return AggSpec(kind, arg, out_name, ddl_type)


# --------------------------------------------------------
# Live Session Window
# --------------------------------------------------------
class Session(NamedTuple):
    start_ms: int
    last_ms: int
    accs: Tuple  # one accumulator per AggSpec, same order


def session_output_type(key_schema: str, aggs: Sequence[AggSpec]) -> str:
    agg_ddl = ", ".join(f"{_q(a.out_name)} {a.ddl_type}" for a in aggs)
    return (f"{key_schema}, session_start_ms long, last_event_ms long, session_end_ms long, "
            f"{agg_ddl}, status string")


class OpenClosedSessions(StatefulProcessor):
    """
    Custom session-window aggregation that publishes progress while a session is
    still ongoing (status 'open') and the final result once the watermark passes
    the end of the session (status 'closed'), like the native session window.
    Supports sum / count / min / max over plain columns.
    """

    def __init__(self, gap_ms, event_time_col, key_names, aggs):
        super().__init__()
        self.gap_ms = gap_ms
        self.event_time_col = event_time_col
        self.key_names = tuple(key_names)
        self.aggs = tuple(aggs)
        self.state_schema = "start_ms long, last_ms long, " + ", ".join(
            f"a{i} {a.ddl_type}" for i, a in enumerate(self.aggs)
        )

    def init(self, handle):
        self.handle = handle
        self.session = handle.getValueState("session", self.state_schema)

    # -- helpers -----------------------------------------------------------
    def _end_ms(self, s):
        return s.last_ms + self.gap_ms

    def _load(self):
        if not self.session.exists():
            return None
        t = tuple(self.session.get())
        return Session(t[0], t[1], tuple(t[2:]))

    def _save(self, s):
        self.session.update((s.start_ms, s.last_ms, *s.accs))

    @staticmethod
    def _step(a, acc, v):
        if a.kind == "count":
            return acc + (1 if a.col is None or not pd.isna(v) else 0)
        if pd.isna(v):
            return acc
        if acc is None:
            return v
        if a.kind == "sum":
            return acc + v
        return min(acc, v) if a.kind == "min" else max(acc, v)

    def _new(self, ts, vals):
        accs = tuple(
            self._step(a, 0 if a.kind == "count" else None, v)
            for a, v in zip(self.aggs, vals)
        )
        return Session(ts, ts, accs)

    def _add(self, s, ts, vals):
        accs = tuple(self._step(a, acc, v) for a, acc, v in zip(self.aggs, s.accs, vals))
        return Session(min(s.start_ms, ts), max(s.last_ms, ts), accs)

    def _row(self, key, s, status):
        row = dict(zip(self.key_names, key))
        row.update(session_start_ms=s.start_ms, last_event_ms=s.last_ms,
                   session_end_ms=self._end_ms(s))
        row.update({a.out_name: v for a, v in zip(self.aggs, s.accs)})
        row["status"] = status
        return row

    # -- stream callbacks --------------------------------------------------
    def handleInputRows(self, key, rows, timerValues):
        watermark = timerValues.getCurrentWatermarkInMs()
        pdf = pd.concat(list(rows)).sort_values(self.event_time_col)

        value_lists = [None if a.col is None else pdf[a.col].tolist() for a in self.aggs]

        cur = self._load()
        old_timer = self._end_ms(cur) if cur else None
        out, changed = [], False

        for i, t in enumerate(pdf[self.event_time_col]):
            ts = t.value // 1_000_000
            if ts < watermark:
                continue
            changed = True
            vals = [None if vl is None else vl[i] for vl in value_lists]
            if cur is None:
                cur = self._new(ts, vals)
            elif ts >= self._end_ms(cur):
                out.append(self._row(key, cur, "closed"))
                cur = self._new(ts, vals)
            else:
                cur = self._add(cur, ts, vals)

        if changed:
            new_timer = self._end_ms(cur)
            if old_timer != new_timer:
                if old_timer is not None:
                    self.handle.deleteTimer(old_timer)
                self.handle.registerTimer(new_timer)
            self._save(cur)
            out.append(self._row(key, cur, "open"))

        if out:
            yield pd.DataFrame(out)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        expiry = expiredTimerInfo.getExpiryTimeInMs()
        s = self._load()
        if s is not None and self._end_ms(s) == expiry:
            self.session.clear()
            yield pd.DataFrame([self._row(key, s, "closed")])

    def close(self):
        pass


def live_session_window(df: DataFrame, groupBy, agg, timeColumn, gapDuration) -> DataFrame:
    """
    Spark's native session_window function only emits each session when it passes
    its watermark threshold which may be too late for some use cases. This implementation
    emits a session as soon as possible with an open status and continously updates 
    until it passes it watermark threshold which at that point will emit a record with
    a closed status. Unlike Spark's native session_window function, the aggregations
    need to be defined alongside the window as they are kept as stateful variables.

    Arguments:
        df (DataFrame): A streaming DataFrame with a watermark on `timeColumn`.=
        groupBy (str | pyspark.sql.Column): The column(s) which to group by for aggregations.
            Similarity to Spark, columns can be represented as strings or by `f.col(...)`
        agg (*exprs) : Same as `pyspark.sql.DataFrame.agg`, Columns or expressions to aggregate by.
            Howover only sum(), count(), min(), max() are acceptable.
        timeColumn (str | pyspark.sql.Column): The column name or column to use as the timestamp for windowing by time.
        gapDuration (str): Literal string specifying the timeout of the session. eg. "10 minutes", "1 second"

    ```python
    (spark
        .readStream()
        .table('catalog.schema.table')
        .withWatermark("event_time", "10 seconds"))
        .live_session_window(
            groupBy="user_id",
            agg=(
                f.sum("amount").alias("sum"),
                f.count("*").alias("count")),
            timeColumn=f.col("event_time"),
            gapDuration="10 seconds")
    ).display()

    >>> +-------+------+-------------------+-------------------+---+-----+------+
        |user_id|window|      session_start|        session_end|sum|count|status|
        +-------+------+-------------------+-------------------+---+-----+------+
        |      0| {...}|2016-03-11 09:00:07|2016-03-11 09:00:08|  2|    1|open  |
        |      0| {...}|2016-03-11 09:00:07|2016-03-11 09:00:10| 13|    2|open  |
        |      0| {...}|2016-03-11 09:00:07|2016-03-11 09:00:18| 26|    3|closed|
        |      0| {...}|2016-03-11 09:00:22|2016-03-11 09:00:23|  3|    1|open  |
        +-------+------+-------------------+-------------------+---+-----+------+
    ```
    """

    # Since the functions signature mimics a Spark, parse 
    group_cols = _as_tuple(groupBy)
    agg_cols = _as_tuple(agg)
    time_name = _col_name(timeColumn)
    gap_ms = _gap_to_ms(gapDuration)

    # Let Spark resolve key / aggregate names and result types for us.
    key_fields = df.select(*group_cols).schema.fields
    agg_fields = df.groupBy().agg(*agg_cols).schema.fields

    key_names = [fld.name for fld in key_fields]
    key_schema = ", ".join(f"{_q(fld.name)} {fld.dataType.simpleString()}" for fld in key_fields)
    aggs = [
        _parse_agg(c, fld.name, fld.dataType.simpleString())
        for c, fld in zip(agg_cols, agg_fields)
    ]

    processor = OpenClosedSessions(gap_ms, time_name, key_names, aggs)
    return (
        df.groupBy(*group_cols)
        .transformWithStateInPandas(
            statefulProcessor=processor,
            outputStructType=session_output_type(key_schema, aggs),
            outputMode="Update",
            timeMode="EventTime",
        )
        .withColumn("session_start", f.date_trunc("second", f.timestamp_millis("session_start_ms")))
        .withColumn("session_end", f.date_trunc("second", f.timestamp_millis("session_end_ms")))
        .withColumn("window", f.struct(f.col("session_start").alias("start"),
                                       f.col("session_end").alias("end")))
        .select(*[f.col(_q(n)) for n in key_names],
                "window", "session_start", "session_end",
                *[f.col(_q(a.out_name)) for a in aggs],
                "status")
    )


DataFrame.live_session_window = live_session_window


# --------------------------------------------------------
# Temporary Checkpoint Display
# --------------------------------------------------------
class TempCheckpointDisplay:
    """
    Using Spark's native display() function for DataStreamReader objects require
    a `checkpointLocation` parameter to be provided, however the display() query
    does not support recovering them checkpoints thus a new one must be invoke every call.
    This implementation manages these checkpoints, creating temporary checkpoints automatically.

    Arguments:
        display_fn (display): Spark's native display() function
        base_path (str): The base directory for all temporary checkpoints

    ```python
    tckp_display = TempCheckpointDisplay(display, '/Volumes/...')
    ```
    """

    def __init__(self, display_fn, base_path):
        self._display = display_fn
        self._base_path = base_path
        self._checkpoints = []
    
    def __call__(self, df=None, *args, name=None, base=None, **kwargs):
        """
        @public
        Display DataStreamReader objects using a checkpoint generation on runtime.

        Arguments:
            name (str): Optional predefined naming for the temporary checkpoint
            base (str): Optional alternative base directory instead of using the class base path

        ```python
        tckp_display(df)
        ```
        """
        # In case a non-streaming table is passed into the function,
        # just call display() normally
        if not getattr(df, "isStreaming", False) or "checkpointLocation" in kwargs:
            return self._display(df, *args, **kwargs)

        path = self._new_path(name, base)
        kwargs["checkpointLocation"] = path
        print(f"[tckp_display] created checkpoint: {path}")

        if path not in self._checkpoints:
            self._checkpoints.append(path)
            return self._display(df, *args, **kwargs)

    def cleanup(self):
        """
        Delete all temporary checkpoints created during the session
        """
        for path in list(self._checkpoints):
            try:
                shutil.rmtree(path, ignore_errors=True)
                self._checkpoints.remove(path)
                print(f"[tckp_display] removed {path}")
            except Exception as e:
                print(f"[tckp_display] could not remove {path}: {e}")

    def _new_path(self, name, base):
        """
        Create the checkpoint path with either the predefined
        directory name and base path or generate a random directory 
        """
        root = (base or self._base_path).rstrip("/")
        return f"{root}/{name or uuid.uuid4().hex[:12]}"


def register_tckp_display(display_fn, base_path):
    """
    Spark transformations are typically invoked using method chaining.
    This function registers a TempCheckpointDisplay class as a callable method
    which can be used within a chain.

    Arguments:
        display_fn (display): Spark's native display() function
        base_path (str): The base directory for all temporary checkpoints
    
    ```python
    tckp_display = register_tckp_display(display, '/Volumes/...')

    df.tckp_display()
    ```
    """
    tckp = TempCheckpointDisplay(display_fn, base_path)

    def _method(self, **kwargs):
        return tckp(self, **kwargs)

    DataFrame.tckp_display = _method
    return tckp


# --------------------------------------------------------
# Table Logging Streaming Query Listener
# --------------------------------------------------------

LIFECYCLE_SCHEMA = (StructType()
    .add("event_type", StringType())
    .add("query_id", StringType())
    .add("run_id", StringType())
    .add("query_name", StringType())
    .add("event_time", TimestampType())
    .add("error", StringType()))


PROGRESS_SCHEMA = (StructType()
    .add("query_id", StringType())
    .add("query_name", StringType())
    .add("batch_id", LongType())
    .add("batch_timestamp", TimestampType())
    .add("batch_duration", LongType())
    .add("duration_breakdown", MapType(StringType(), LongType()))
    .add("input_rows", LongType())
    .add("sources", ArrayType(StringType()))
    .add("files_outstanding", ArrayType(LongType()))
    .add("bytes_outstanding", ArrayType(LongType())))


class TableStreamingQueryListener(StreamingQueryListener):
    """
    A StreamingQueryListener whose events related to StreamingQuery are logged onto managed tables.
    Records are written on background threads as onQueryStarted cannot be blocked.
    There are also seperate threads and an external function which handles onQueryProgress events as
    there seems to be an unknown issue preventing both tables to be written using the same thread.

    The lifecycle and progress tables will have the following schemas:

    ```
    +----------+------------+------------+------------+-------------------+-----+
    |event_type|    query_id|      run_id|  query_name|         event_time|error|
    +----------+------------+------------+------------+-------------------+-----+
    |TERMINATED|74bdb97c-...|08d43c21-...|stream_query|2026-11-25 19:57:19| NULL|
    |   STARTED|74bdb97c-...|08d43c21-...|stream_query|2026-11-25 19:56:32| NULL|
    +----------+------------+------------+------------+-------------------+-----+

    +------------+------------+--------+-------------------+--------------+------------------+----------+---------------------+-----------------+-----------------+
    |    query_id|  query_name|batch_id|    batch_timestamp|batch_duration|duration_breakdown|input_rows|              sources|files_outstanding|bytes_outstanding|
    +------------+------------+--------+-------------------+--------------+------------------+----------+---------------------+-----------------+-----------------+
    |74bdb97c-...|stream_query|      0 |2026-11-25 19:56:45|          3467|             {...}|        35|[catalog.schema.df_0]|              [5]|         [534745]|
    |74bdb97c-...|stream_query|      1 |2026-11-25 19:56:48|          2909|             {...}|        37|[catalog.schema.df_0]|              [4]|         [423123]|
    +------------+------------+--------+-------------------+--------------+------------------+----------+---------------------+-----------------+-----------------+
    ```

    Arguments:
        spark (SparkSession): The SparkSession whose StreamingQuery are to listened to
        lifecycle_table (str): Path to the table which will store onQueryStarted and onQueryTerminated events
        progress_table (str): Path to the table which will store onQueryProgress events
        polling_rate (int) : In seconds, how long to wait before checking for another micro-batch being progressed (Default: 2)
    """

    def __init__(self, spark, lifecycle_table, progress_table, polling_rate=2):
        super().__init__()

        self._spark = spark
        self._lifecycle_table = lifecycle_table
        self._progress_table = progress_table
        self._polling_rate = polling_rate

        self._table_names = {}
        self._query_names = {str(q.id): q.name for q in self._spark.streams.active}

        # Last micro-batch which was already processed and logged per query,
        # ensuring we don't log duplicate records
        self._last_batch = {}
        for q in self._spark.streams.active:
            p = q.lastProgress
            self._last_batch[str(q.id)] = p.batchId if p else -1

        # Create log tables if they do not exist yet
        for table, schema in [(lifecycle_table, LIFECYCLE_SCHEMA), (progress_table, PROGRESS_SCHEMA)]:
            if table is not None:
                self._spark.createDataFrame([], schema).write.mode("ignore").saveAsTable(table)

        # Handles logging onQueryStarted and onQueryTerminated events
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._drain, daemon=True, name="stream-listener-writer")
        self._thread.start()

        # Handles logging onQueryProgress which does not deliver reliably, replacing it with a custom function
        self._stop_event = threading.Event()
        self._poller = threading.Thread(target=self._poll, daemon=True, name="stream-listener-poller")
        self._poller.start()

    @classmethod
    def register(cls, spark, *args, **kwargs):
        """
        Creates an instance of TableStreamingQueryListener while removing any
        old listeners to avoid duplicate entries

        ```python
        my_listener = TableStreamingQueryListener.register(
            spark,
            lifecycle_table="cataqlog.schema.stream_lifecycle",
            progress_table="cataqlog.schema.stream_progress",
            polling_rate=3)
        ```
        """
        for old in cls._registered(spark):
            spark.streams.removeListener(old)
            try:
                old.close(timeout=10)
            except Exception:
                pass
        listener = cls(spark, *args, **kwargs)
        spark.streams.addListener(listener)
        return listener

    @classmethod
    def _registered(cls, spark):
        # Matches by class name so instances of an older cell-defined version are found too.
        bus = getattr(getattr(spark.streams, "_sqlb", None), "_listener_bus", None)
        return [l for l in list(bus or []) if type(l).__name__ == cls.__name__]

    def _log(self, table, schema, row):
        self._queue.put((table, schema, row))

    def _delta_name(self, uuid):
        if uuid not in self._table_names:
            row = self._spark.sql(f"""
                SELECT concat_ws('.', table_catalog, table_schema, table_name) AS name
                FROM system.information_schema.tables
                WHERE storage_path LIKE '%/tables/{uuid}'""").first()
            self._table_names[uuid] = row["name"] if row else None
        return self._table_names[uuid]

    def _source_name(self, source):
        desc = source.get("description", "")
        kind = re.match(r"[^\[]*", desc)[0]
        match = re.search(r"/tables/([0-9a-f-]{36})", desc)

        if kind != "DeltaSource" or not match:
            return kind
        try:
            return self._delta_name(match[1]) or kind
        except Exception:
            return kind

    def _drain(self):
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                table, schema, row = item
                row = row() if callable(row) else row
                self._spark.createDataFrame([row], schema).writeTo(table).append()
            finally:
                self._queue.task_done()

    def _poll(self):
        """Poll lastProgress of every active query and log any batch we haven't seen yet."""
        while not self._stop_event.is_set():
            for q in self._spark.streams.active:
                qid = str(q.id)
                event = q.lastProgress
                if event is None:
                    continue
                if event.batchId > self._last_batch.get(qid, -1):
                    self.ext_onQueryProgress(event)
                    self._last_batch[qid] = event.batchId

            # wait() instead of sleep() so close() can interrupt immediately
            self._stop_event.wait(self._polling_rate)

    def flush(self):
        """
        Blocks until every log queued has been written.

        ```python
        my_listener.flush()
        spark.table('catalog.schema.progress_table').display()
        ```
        """
        self._queue.join()

    def close(self, timeout=None):
        """
        Stop the writing threads, ensuring all logs queued is written prior.

        Arguemnts:
            timeout (float): Optional time in seconds to wait for each thread to stop.
                If it isn't provided, it will wait indefinitely.
        
        ```python
        my_listener.close()
        ```
        """
        self._stop_event.set()
        self._poller.join(timeout)
        self._queue.put(None)
        self._thread.join(timeout)

    def onQueryStarted(self, event):
        """@private Called by Spark"""
        query_id = str(event.id)
        self._query_names[query_id] = event.name

        self._log(self._lifecycle_table, LIFECYCLE_SCHEMA,
            dict(
                event_type  = "STARTED",
                query_id    = query_id,
                run_id      = str(event.runId),
                query_name  = event.name,
                event_time  = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00")),
                error       = None
            ))

    def onQueryTerminated(self, event):
        """@private Called by Spark"""
        query_id = str(event.id)

        self._log(self._lifecycle_table, LIFECYCLE_SCHEMA,
            dict(
                event_type  = "TERMINATED",
                query_id    = query_id,
                run_id      = str(event.runId),
                query_name  = self._query_names.pop(query_id, None),
                event_time  = datetime.now(timezone.utc),
                error       = event.exception
            ))
        self._last_batch.pop(query_id, None)

    def onQueryProgress(self, event):
        """@private"""
        #Handled by the _poll thread (see ext_onQueryProgress)
        pass

    def ext_onQueryProgress(self, event):
        """@private Called by the poller; logs a progress row for one batch."""
        if event.get("id") and event.get("name"):
            self._query_names.setdefault(event["id"], event["name"])

        # Skip on empty micro-batch or no progress table
        if ((self._progress_table is None) or (not event.get("numInputRows"))):
            return

        sources = event.get("sources") or []
        metric = lambda k: [_int((s.get("metrics") or {}).get(k)) for s in sources]

        # Built lazily on the writer thread, because _source_name may run a SQL query.
        self._log(self._progress_table, PROGRESS_SCHEMA, lambda: dict(
            query_id            = event.get("id"),
            query_name          = event.get("name"),
            batch_id            = event.get("batchId"),
            batch_timestamp     = datetime.fromisoformat(event.get("timestamp").replace("Z", "+00:00")),
            batch_duration      = event.get("batchDuration"),
            duration_breakdown  = event.get("durationMs"),
            input_rows          = event.get("numInputRows"),
            sources             = [self._source_name(s) for s in sources],
            files_outstanding   = metric("numFilesOutstanding"),
            bytes_outstanding   = metric("numBytesOutstanding")
        ))
    
    def onQueryIdle(self, event):
        """@private"""
        pass

