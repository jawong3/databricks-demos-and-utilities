"""
This library holds benchmarking decorators catered towards Spark DataFrames & Transformations.
These decorators are designed to be stackable with eachother, however the order in which they are invoked matters.
Below are all the decorators available in order of which they should be stacked

```python
@track_partitions
@time_runs
@clear_cache
@track_stages
def my_func(): ...
```
"""
import time
from functools import wraps
from typing import Any

def _isSparkDF(x) -> bool:
    """
    Check if the parameter object is a Spark DataFrame
    """
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDF
        if isinstance(x, ConnectDF): return True
    except ImportError:
        pass

    from pyspark.sql import DataFrame
    return isinstance(x, DataFrame)


def _force(result) -> None:
    """
    Execute a lazy Spark DataFrame with a noop write, at most once per DataFrame
    object, so stacked decorators don't each re-run the whole plan.
    """
    if _isSparkDF(result) and not result.__dict__.get("_forced"):
        result.write.format("noop").mode("overwrite").save()
        result._forced = True


def time_runs(fn=None, *, runs: int = 5, label: str = ""):
    """
    Execute the call `runs` times, tracking each of their runtime. If the call
    returns a Spark DataFrame, it will forcefully call an action. Results are
    appended into `<function>.times` for future reference as well as printed out,
    displaying the best runtime and all runtimes

    Arguments:
        runs (int): Number of times to repeat the call (default: 5)
        label (str): Optional label to help distinguish seperate runs (default: "")

    ```python
    @time_runs
    def my_func(): ...

    my_func()
    >>> "my_func             best=3.22s  all=[3.67, 3.69, 3.22, 3.73, 5.05]

    @time_runs()
    def my_func(): ...

    my_func()
    >>> "my_func             best=3.22s  all=[3.67, 3.69, 3.22, 3.73, 5.05]

    @time_runs(runs=3, label="baseline")
    def my_func(): ...

    my_func()
    >>> "my_func   baseline  best=3.67s  all=[3.67, 3.69, 3.73]

    my_func.times
    >>> [3.66611, 3.68593, 3.22110, 3.72631, 5.04678]
    ```
    """

    if runs < 1: raise ValueError("Argument `runs` must be some positive integer")
 
    def decorator(fn):
        prefix = f"{fn.__name__:<20} " + (f"{label:<12} " if label else "")
        recorded: list[float] = []
 
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Start each call fresh: clear any per-run records (e.g. track_stages)
            # (clear in place: rebinding a local name would leave `fn.stages` untouched)
            stages = getattr(fn, "stages", None)
            if stages is not None:
                stages.clear()

            times: list[float] = []
            for _ in range(runs):
                start = time.perf_counter()
                result = fn(*args, **kwargs)
                _force(result)
                times.append(time.perf_counter() - start)
 
            recorded[:] = times
            rounded = [round(t, 2) for t in times]
            print(f"{prefix}best={min(times):.2f}s  all={rounded}")
            return result
 
        wrapper.times = recorded
        return wrapper
    
    # Allow argumental calls @time_runs(..) & non-argumental calls @time_runs
    if fn is None: return decorator
    return decorator(fn)


def track_stages(fn):
    """
    For a call returning a Spark DataFrame or invoking an action, count the
    stages as well as the tasks within each stage. Results are appened into
    `<function>.stages` for future reference, where each record has the shape
    {"stages": <list>, "task_chain": <str>, "total_tasks": <int>}

    ```python
    @track_stages
    def my_func(): ...
 
    my_func()

    my_func.stages[0]['stages']
    >>> [
            {
                'job': 1,
                'stage': 10,
                'tasks': 20,
                'name': 'write at WriteIntoDeltaCommand.scala:104'
            },

            {
                'job': 2,
                'stage': 11,
                'tasks': 4,
                'name': 'executeCollect at StateCache.scala:471'
            }
        ]

    my_func.stages[0]['task_chain']
    >>> "20 -> 4"

    my_func.stages[0]['total_tasks']
    >>> 24
    ```
    """

    stages_per_call: list[list[dict[str, Any]]] = []
 
    @wraps(fn)
    def wrapper(*args, **kwargs):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        tr = sc.statusTracker()

        # Define a unique JobGroup per call, tracking their stages seperately
        group = f"{fn.__name__}-{time.time_ns()}"
        sc.setJobGroup(group, group)
 
        # Force a Spark action if required
        result = fn(*args, **kwargs)
        _force(result)
 
        # Parse through the statusTracker, grabbing relevant information 
        stages: list[dict[str, Any]] = []
        for j in tr.getJobIdsForGroup(group):
            job = tr.getJobInfo(j)
            if job is None:
                continue
            for s in job.stageIds:
                info = tr.getStageInfo(s)
                if info is not None:
                    stages.append({"job": j, "stage": s, "tasks": info.numTasks, "name": info.name})
 
        # Chain of task counts in execution order
        # Order by stage ID and keep the raw numbers; format only for display
        stages.sort(key=lambda s: s["stage"])
        counts = [s["tasks"] for s in stages]

        stages_per_call.append({
            "stages": stages,
            "task_chain": " -> ".join(map(str, counts)),
            "total_tasks": sum(counts),
        })
        return result
 
    wrapper.stages = stages_per_call
    return wrapper


def track_partitions(fn):
    """
    For a call returning a Spark DataFrame, print the number of partitions it occupies.
    This includes the number of partitions allocated for the DataFrame as well as the
    number of partitions which contain records.
    
    Results are overwritten into `<function>.partitions` for future reference.

    ```python
    @track_partitions
    def my_func(): ...

    my_func()
    >>> "my_func             partitions: [total=200, used=190, empty=10]")

    my_func.partitions
    >>> {"total": <int>, "used": <int>, "empty": <int>}
    ```
    """
    counts: dict[str, int] = {}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        from pyspark.sql import functions as f
 
        result = fn(*args, **kwargs)
        if not _isSparkDF(result):
            raise TypeError(f"{fn.__name__} must return a Spark DataFrame, got {type(result).__name__}")

        # Spark's `getNumPartitions` only grabs the partitions allocated to the DataFrame
        total = result.rdd.getNumPartitions()

        # Partitions with zero counts aren't being utilizied
        used = result.select(f.spark_partition_id()).distinct().count()

        counts.update(total=total, used=used, empty=total - used)
        print(f"{fn.__name__:<20} partitions: [total={total}, used={used}, empty={total - used}]")
        return result
 
    wrapper.partitions = counts
    return wrapper


def clear_cache(fn=None, *, before=True, after=False):
    """
    Invokes Spark's `clearCache()` method around a call.
    By default, it is done prior to the call, however this
    can be modified by providing arguments.

    Arguments:
        before (bool): Clear cache prior to call (default: True)
        after (bool): Clear cache after call (default: False)

    ```python
    @clear_cache
    def my_func(): ...

    @clear_cache()
    def my_func(): ...

    @clear_cache(after=True)
    def my_func(): ...
    ```
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            if before: spark.catalog.clearCache()
            try:
                return fn(*args, **kwargs)
            finally:
                if after: spark.catalog.clearCache()
    
    # Allow argumental calls @clear_cache(..) & non-argumental calls @clear_cache
    if fn is None: return decorator
    return decorator(fn)

