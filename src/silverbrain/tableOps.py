#
#//  tableOp.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/17/26.
#//
"""
    Each TableOp has no memory, only making transformations to tables, or returning a bool. To make changes to tables, have a web.Web holding tableOps and tables use `.process`
"""

from . import types

import polars as pl
import numpy as np

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import chain
from typing import Self, Sequence, Type

# -- TableOp functions

def add_pl_columns(
    expr: tuple[ pl.Expr ],
    dtype: pl.DataType,
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> pl.Expr:
    if dtype == pl.List:
        return pl.concat_list( *expr )
    #
    
    if dtype.is_numeric():
        return pl.sum_horizontal( *expr )
    #
    raise TypeError("Unrecognized sum dtype={}".format(dtype))
#/def add_pl_columns

def add_values_from_join(
    df: pl.DataFrame,
    update: pl.DataFrame,
    keys: tuple[ str,... ],
    ignore: tuple[ str ] = (),
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        Uses 'addition' from `add_pl_columns(...)` for every column in `update` matching a column in df, except keys which form the join. If the key is not present in `df` then take the raw `update` value.
    """
    ignore_columns: tuple[ str ] = keys + ignore
    parameter_columns: tuple[ str, ... ] = tuple(
        col for col in update.columns if col not in ignore_columns
    )
    
    return df.join(
        update,
        on = keys,
        how = 'left',
        **kwargs
    ).with_columns(
        (
            pl.when(
                pl.col( col +'_right' ).is_null()\
                    | pl.col( col ).is_null()
                #/
            ).then(
                pl.col( col )
            ).otherwise(
                add_pl_columns(
                    (
                        pl.col( col ),
                        pl.col( col +'_right')
                    ),
                    dtype = df.schema[ col ]
                )
            ).alias( col ) if col in df.columns
                # raw update value if not in `df`
                else pl.col( col )\
                for col in parameter_columns
            #/
        )
    ).select(
       set( df.columns + update.columns )
    )
#/def add_values_from_join

def update_df_on_keys(
    df: pl.DataFrame | None,
    update: pl.DataFrame,
    keys: tuple[ str,... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        Use a left join and merge to update keys
    """
    
    # If no df to update, just set it to the new update
    if df is None:
        return update
    #
    
    # Gather non id columns to join in
    join_columns: tuple[ str, ... ] = tuple(
        col for col in update.columns\
            if col in df.columns\
            and col not in keys
        #/
    )
    
    if not join_columns:
        raise ValueError(
            "Missing columns for update; found `update.columns={}` with `keys={}`".format(
                update.columns,
                keys
            )
        )
    #/if not join_columns
    
    return df.join(
        update.select(
            keys + join_columns
        ).with_row_index( '_index' ),
        on = keys,
        how = 'outer_coalesce',
        **kwargs
    ).with_columns(
        tuple(
            pl.when(
                pl.col('_index').is_null()
            ).then(
                pl.col( col )
            ).otherwise(
                pl.col( col + '_right' )
            ).alias( col )\
                for col in join_columns
            #/
        )
    ).drop(
        tuple(
            col +'_right'\
                for col in join_columns
            #/
        ) + ('_index',)
    )
#/def update_df_on_keys

def update_df_on_keys_by_namerColumn(
    df: pl.DataFrame,
    update: pl.DataFrame,
    keys: tuple[ str ],
    namerCol: str,
    sourceCol: str,
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        When we get a join match based on keys, we look at the value in update of `namerCol`. This will be a column in `df`. Set the value of that column to the column `sourceCol` from update.
        
        If you want to have some function instead of `sourceCol`, then make another transformer to make that column.
    """
    # Our namer and source might be in df, so we have to rename them to properly
    #   grab them after the join on keys
    namerCol_join = namerCol + '_right' if namerCol in df.columns else namerCol
    sourceCol_join = sourceCol + '_right' if sourceCol in df.columns else sourceCol
    
    # Get the expression to find the col named by `namerCol`, after joining
    colNamer_expr_tup: tuple[ pl.Expr,... ] = tuple(
        pl.when(
            pl.col( namerCol_join ) == targetCol
        ).then(
            pl.col( sourceCol_join )
        ).otherwise(
            pl.col( targetCol )
        ).alias(
            targetCol
        )\
            for targetCol in update[ namerCol ].drop_nulls().unique()
        #/
    )
    
    return df.join(
        update.select(
            keys + (namerCol,) + (sourceCol,)
        ),
        on = keys,
        how = 'left',
        **kwargs
    ).with_columns(
        colNamer_expr_tup
    ).drop(
        ( sourceCol_join, namerCol_join )
    )
#/def update_df_on_keys_by_namerColumn

# -- Single Table Ops

class TableOp( ABC ):
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        ) -> None:
        self.rng = rng
        return
    #/def __init__
    
    @abstractmethod
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def __call__
#/class TableOp

# -- Atomic Table Ops

class WriteParquetOp( TableOp ):
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        """
            :param dfs: `dfs[0]` will be written to the value `fp` in `dfs[1]`. Make sure this is an absolute path, or be very careful with your relative paths
            :returns: a dfs with (fp:str,isfile:bool)
        """
        fp: str = dfs[1]['fp'].item()
        
        if verbose > 0:
            print('{}Writing to {}'.format(verbose_prefix,fp))
        #
        
        dfs[0].write_parquet(
            fp
        )
        
        _isfile: bool = path.isfile( fp )
        return (
            dfs[1].with_columns(
                isfile = pl.when(
                    pl.col('fp') == pl.lit( fp )
                ).then(
                    pl.lit(_isfile)
                ).otherwise(
                    pl.lit( False )
                )
            ),
        )
    #/def __call__
#/class WriteParquetOp

class SimpleGetOp( TableOp ):
    """
        Just return the input. Used to get tables in `Web.Tables` before using them
        for subsequent Ops in a TableOpProcessor
    """
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        return dfs
    #/def __call__
#/class SimpleGetOp

class SimplePrintOp( SimpleGetOp ):
    """
        Print `self.message` followed by the schema and value of each input, and passing them through
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        message: str = '',
        ) -> None:
        super().__init__(
            rng = rng,
        )
        
        self.message = message
        return
    #/def __init__

    def _print(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose_prefix: str = '',
        ) -> None:
        if self.message:
            print( verbose_prefix + self.message )
        #
        
        for i in range( len( dfs ) ):
            print( verbose_prefix + "{}".format(i) )
            print( verbose_prefix + "  " + str(dfs[i].schema) )
            print( dfs[i] )
        #/for i in range( len( dfs ) )
        
        return
    #/def _print
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        self._print(
            dfs = dfs,
            verbose_prefix = verbose_prefix,
        )
        return dfs
    #/def __call__
#/class SimplePrintOp

class TransformPrintOp( SimplePrintOp ):
    """
        Like SimplePrintOp but prints and simple transform of the tuple of dfs via `lam`. Provide a custom `lam` on initialization. Returns original dfs
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        message: str = '',
        lam: types.TableProcess = lambda dfs, verbose = 0, verbose_prefix = '':\
            dfs,
        #/
        ) -> None:
        
        super().__init__(
            rng = rng,
            message = message,
        )
        
        self.lam = lam

        return
    #/def __init__
    
    #def _print
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        
        transformedDfs: tuple[ pl.DataFrame,... ] = self.lam(
            dfs, verbose, verbose_prefix
        )
        
        self._print(
            dfs = transformedDfs,
            verbose_prefix = verbose_prefix,
        )
        
        return dfs
    #/def __call__
#/class TransformPrintOp

class AssertOp( SimpleGetOp ):
    """
        Assert the value of `lam(dfs)` before passing them though. Provide a custom `lam` on initialization
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        lam: types.TableCheck = lambda dfs, verbose = 0, verbose_prefix = '':\
            True,
        #/
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.lam = lam

        return
    #/def __init__
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        assert self.lam( dfs )
        return dfs
    #/def __call__
#/class AssertOp

class FilterOp( TableOp ):
    """
        Take one dataframe as input; return it with the filter applied
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        filter_expr: pl.Expr = pl.lit(True),
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.filter_expr = filter_expr

        return
    #/def __init__
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,]:
        return (
            dfs[0].filter( self.filter_expr ),
        )
    #/def __call__
#/class FilterOp

class HeadOp( TableOp ):
    """
        Return the first `count` rows from the one dataframe input
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        count: int = 1,
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.count = count

        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,]:
        return (
            dfs[0].head( self.count ),
        )
    #/def __call__
#/class HeadOp

class TailOp( TableOp ):
    """
        Return the last `count` rows from the one dataframe input
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        count: int = 1,
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.count = count

        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,]:
        return (
            dfs[0].tail( self.count ),
        )
    #/def __call__
#/class TailOp

class TransformOp( TableOp ):
    """
        Applies a simple transformation to inputs as `lam(dfs, verbose, verbose_prefix)`. Provide a custom `lam` upon initialization. To help this, a series of classmethods are provided
        
        :param lam: lambda dfs, verbose, verbose_prefix -> dfs_out
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        lam: types.TableProcess = lambda dfs, verbose = 0, verbose_prefix = '':\
            dfs,
        #/
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.lam = lam

        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,...]:
        return self.lam(
            dfs,
            verbose,
            verbose_prefix,
        )
    #/def __call__
    
    @classmethod
    def update_target_on_keys(
        cls: Type,
        keys: tuple[ str, ... ],
        rng: np.random.Generator | None = None,
        **kwargs # passed to dfs[0].join
        ) -> Self:
        """
            Update dfs[1] using dfs[0]
            For a target table, update it using the single source table. (As usual, the input to the lam will be tableIds `tuple(source[0], target)`
        """
        
        return cls(
            rng = rng,
            lam = lambda dfs, verbose=0, verbose_prefix='': (
                update_df_on_keys(
                    df = dfs[1],
                    update = dfs[0],
                    keys = keys,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix,
                    **kwargs
                ),
            )
        )
    #/def update_target_on_keys
    
    @classmethod
    def update_target_on_keys_by_namerColumn(
        cls: Type,
        keys: tuple[ str, ... ],
        namerCol: str,
        sourceCol: str,
        rng: np.random.Generator | None = None,
        **kwargs # passed to dfs[0].join
        ) -> Self:
        """
            Pattern of updating a column in dfs[1], where the value comes from dfs[0][sourceCol], with a target column from `namerCol`
        """
        
        return cls(
            rng = rng,
            lam = lambda dfs, verbose=0, verbose_prefix='': (
                update_df_on_keys_by_namerColumn(
                    df = dfs[1],
                    update = dfs[0],
                    keys = keys,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix,
                    **kwargs
                ),
            )
        )
    #/def update_target_on_keys_by_namerColumn
#/class TransformOp

class TableOpSequence( TableOp ):
    """
        Runs a series of pure ops in a row
        
        Conforms to `types.TableProcess`
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        terms: Sequence[ types.TableProcess ] | None = None,
        ) -> None:
        
        super().__init__(
            rng = rng,
        )
        
        self.terms = terms or []
        
        return
    #/def __init__
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        
        for op in self.terms:
            dfs = op(
                dfs = dfs,
                verbose = verbose,
                verbose_prefix = verbose_prefix,
            )
        #/for op in self.terms
        
        return dfs
    #/def __call__
#/class TableOpSequence

class TableOpWhile():
    """
        Apply `self.process` as long as `self.condition` holds.

        Conforms to `types.TableProcess`
    """
    def __init__(
        self: Self,
        condition: types.TableCheck,
        process: types.TableProcess,
        op_id: str,
        maxIter: int = 100,
        ) -> None:
        self.condition = condition
        self.process = process
        self.op_id = op_id
        self.maxIter = maxIter
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        iterations: int = 0

        while self.condition(
            dfs = dfs,
            verbose = verbose,
            verbose_prefix = verbose_prefix + "  ",
        ):
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({})".format( iterations )
                )
            #
            if iterations >= self.maxIter:
                raise Exception(
                    "reached maxIter={}".format( self.maxIter )
                )
            #/if iterations >= self.maxIter

            dfs = self.process(
                dfs = dfs,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )
            iterations += 1
        #/while self.condition( dfs,... )

        return dfs
    #/def __call__
#/class TableOpWhile

class TableOpCount():
    """
        Apply `self.process` a finite number of times.

        Conforms to `types.TableProcess`
    """
    def __init__(
        self: Self,
        count: int,
        process: types.TableProcess,
        op_id: str,
        ) -> None:
        self.count = count
        self.process = process
        self.op_id = op_id
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        for j in range( self.count ):
            if verbose > 0:
                print(
                    verbose_prefix\
                        + self.op_id\
                        + " ({}/{})".format( j+1, self.count )
                    #/
                )
            #

            dfs = self.process(
                dfs = dfs,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )
        #/for j in range( self.count )

        return dfs
    #/def __call__
#/class TableOpCount

class TableOpBranch():
    """
        Implements if/else/otherwise, running only a single branch.

        Check each of `self.ifs` in turn, and if evaluates to true, run the corresponding `self.thens` on dfs. If none do, run `self.otherwise` if present, or does nothing.

        Conforms to `types.TableProcess`
    """
    def __init__(
        self: Self,
        ifs: Sequence[ types.TableCheck | bool ],
        thens: Sequence[ types.TableProcess ],
        otherwise: types.TableProcess | None,
        op_id: str,
        ) -> None:

        assert len( ifs ) == len( thens )

        self.ifs = ifs
        self.thens = thens
        self.otherwise = otherwise
        self.op_id = op_id
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        j: int = 0

        check: bool
        while j < len( self.ifs ):
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({}/{})".format(
                        j+1, len( self.ifs )
                    )
                )
            #
            if isinstance( self.ifs[j], bool ):
                check = self.ifs[j]
            #
            else:
                check = self.ifs[j](
                    dfs = dfs,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix + "  ",
                )
            #

            if check:
                dfs = self.thens[j](
                    dfs = dfs,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix + "  ",
                )
                break
            #/if check
            else:
                j += 1
            #
        #/while j < len( self.ifs )

        if j >= len( self.ifs ) and self.otherwise is not None:
            if verbose > 0:
                print(
                    verbose_prefix + " (Otherwise)"
                )
            #

            dfs = self.otherwise(
                dfs = dfs,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )
        #/if j >= len( self.ifs ) and self.otherwise is not None

        return dfs
    #/def __call__
#/class TableOpBranch
