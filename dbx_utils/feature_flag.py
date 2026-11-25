"""
This library holds a feature flag decorator catered towards Spark DataFrames & Transformations.
It is designed as a logic gate for certain transformation which cannot be done on the orchestration
layer
"""
from functools import wraps
from pyspark.sql import DataFrame

def feature_flag(flag, fallback = None):
    """
    For a call returning a Spark DataFrame, it will only execute the call
    if the flag provided is `True`. Otherwise, it will invoke the callable
    fallback is provided or do nothing - skipping the call.

    Arguments:
        flag (bool): Whether to use the function
        fallback (Callable[[DataFrame], DataFrame]): Optional function which takes a Spark DataFrame,
            transforms it and return another DataFrame. This function will be used if flag is `False`
    
    ```python
    feature_toggle = None

    def old_imple(df):
        print("Using Old Implementation")

    @feature_flag(feature_toggle, old_imple)
    def new_imple(df):
        print("Using New Implementation")

    feature_toggle = True
    df.transform(new_imple)
    >>> "Using New Implementation"

    feature_toggle = False
    df.transform(new_imple)
    >>> "Using Old Implementation"
    ```
    """

    def decorator(func):
        @wraps(func)
        def wrapper(df: DataFrame, *args, **kwargs) -> DataFrame:
            if flag:
                return func(df, *args, **kwargs)
            if fallback is None:
                return df
            return fallback(df, *args, **kwargs)

        return wrapper

    return decorator

