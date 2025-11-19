import polars as pl
import numpy as np

from . import uuid

def get_uuid7_bytes() -> bytes:
    # TODO: reproducability
    return uuid.uuid7().bytes_le
#/def get_uuid_int

def add_col_uuid7(
    df: pl.DataFrame,
    col: str,
    overwrite: bool = False
    ) -> pl.DataFrame:
    """
        :param pl.DataFrame df: Dataframe for new column
        :param str col: Column name for the new column
        :param bool overwrite: Whether to overwrite `col` if already present in `df.columns`
    """
    if (not overwrite) and\
        col in df.columns:
            raise ValueError(
                "Already have column in df col={}".format( col )
            )
        #/if { column conflict }
    #/
    
    return df.with_columns(
        pl.Series(
            get_uuid7_bytes() for _ in range( df.shape[0] )
        ).alias( col ).cast( pl.Binary )
    )
#/def add_col_uuid7

def add_random_row_indices(
    df: pl.DataFrame,
    col: str,
    rng: np.random.Generator
    ) -> pl.DataFrame:
    return df.with_columns(
        pl.Series(
            rng.permutation(
                df.shape[0]
            )
        ).alias( col )
    )
#/def add_random_row_indices
