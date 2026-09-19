#
#//  tableOp.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/17/26.
#//
"""
    Atomic operations on tuples of tables. Most are memoryless, though some hold tables in memory. Backend-specific ops (parquet/sqlite/postgres readers and writers) live in silverflow, not here -- this module only holds the backend-agnostic core.
"""

from . import bdd, types
from .polarsDataTypeStrings import dtype_to_str, str_to_dtype
from .schema import Effect, InputSchema, OutputSchema, TableOpSchema, table_schemas

import polars as pl
import numpy as np

from abc import ABC, abstractmethod
from typing import Self, Type
from uuid import uuid4
import threading

def _open_passthrough_schema() -> tuple[ list[ InputSchema ], list[ OutputSchema ] ]:
    """
    input[0] accepts any columns; output[0] is exactly what input[0]
    received, unchanged -- a fresh ColVar per call (not a shared literal
    name) so unrelated op instances never accidentally share identity once
    compiled through the same BDDManager (ColVar scope is global -- see
    bdd.py's module docstring).
    """
    rest = bdd.ColVar( f'_passthrough_{uuid4().hex}' )
    return (
        [ InputSchema( expr = bdd.Union( rest ) ) ],
        [ OutputSchema( expr = bdd.Union( rest ) ) ],
    )
#/def _open_passthrough_schema

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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        self.rng     = rng
        self.input   = input
        self.output  = output
        self.effects = effects
        return
    #/def __init__

    @abstractmethod
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def __call__
#/class TableOp

# -- Atomic Table Ops

class SimpleGetOp( TableOp ):
    """
        Just return the input. Used to get tables in `Web.Tables` before using them
        for subsequent Ops in a TableOpProcessor
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__( rng = rng, effects = [] )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        corr_id: tuple[ str, ... ] = (),
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
        corr_id: tuple[ str, ... ] = (),
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
        corr_id: tuple[ str, ... ] = (),
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
        corr_id: tuple[ str, ... ] = (),
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else _open_passthrough_schema()[ 0 ],
            output  = output  if output  is not None else _open_passthrough_schema()[ 1 ],
            effects = effects if effects is not None else [],
        )
        self.filter_expr = filter_expr
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        corr_id: tuple[ str, ... ] = (),
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else _open_passthrough_schema()[ 0 ],
            output  = output  if output  is not None else _open_passthrough_schema()[ 1 ],
            effects = effects if effects is not None else [],
        )
        self.count = count
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        corr_id: tuple[ str, ... ] = (),
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else _open_passthrough_schema()[ 0 ],
            output  = output  if output  is not None else _open_passthrough_schema()[ 1 ],
            effects = effects if effects is not None else [],
        )
        self.count = count
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ],
        corr_id: tuple[ str, ... ] = (),
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        #/
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input,
            output  = output,
            effects = effects if effects is not None else [],
        )
        self.lam = lam
        return
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        corr_id: tuple[ str, ... ] = (),
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
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
            ),
            input = input,
            output = output,
            effects = effects,
        )
    #/def update_target_on_keys

    @classmethod
    def update_target_on_keys_by_namerColumn(
        cls: Type,
        keys: tuple[ str, ... ],
        namerCol: str,
        sourceCol: str,
        rng: np.random.Generator | None = None,
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
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
            ),
            input = input,
            output = output,
            effects = effects,
        )
    #/def update_target_on_keys_by_namerColumn
#/class TransformOp

class SQLOp( TableOp ):
    """
        Executes SQL strings, with a dataframe as the result from each entry in the tuple `self.sql`

        Refer to inputs as 'df_{i}' for i in range(len( dfs ) )
    """
    def __init__(
        self: Self,
        sql: tuple[ str,... ],
        rng: np.random.Generator | None = None,
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input = input,
            output = output,
            effects = effects,
        )

        self.sql = sql
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        """
            Use a pl.SQLContext, naming dataframes as 'df_0, ...'
        """
        ctx = pl.SQLContext(
            frames = {
                'df_{}'.format( i ): dfs[i]
                for i in range( len( dfs ) )
            }
        )

        return tuple(
            ctx.execute(
                _sql for _sql in self.sql
            )
        )
    #/def __call__
#/class SQLOp

# -- In-Memory Table Ops (no disk/backend access of their own)

class MemoryOp( TableOp ):
    """
    Purely in-memory table cache -- holds the last value it was given
    across repeated firings (even across separate Cell.__call__
    invocations, since state lives on self, not per-call wiring state).
    No disk/backend access of its own: seed it by wiring a reader op's
    output (or the Cell's own external input) into its source; persist
    it by wiring its output into a separate writer op elsewhere. This is
    what makes "read once, mutate many times in memory, write back later"
    possible entirely through ordinary wiring -- no read/write logic
    lives here.

    dfs[0]: next state -- replaces self._table
    Returns self._table (current remembered state).

    A lock guards the read-modify-write of self._table: the same
    instance is shared across every _WiringEngine dispatch of this node,
    including two concurrent external calls to the same Cell (each gets
    its own _WiringEngine, but both dispatch through this one MemoryOp).
    This prevents a torn read of self._table mid-assignment -- it does
    NOT make concurrent ordering well-defined (two concurrent writers
    still race on which value wins).
    """
    def __init__(
        self: Self,
        columns: dict[ str, pl.DataType ],
        rng: np.random.Generator | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input  = [ InputSchema.from_columns( columns, extra = 'open' ) ],
            output = [ OutputSchema.from_columns( columns ) ],
        )
        self._table: pl.DataFrame | None = None
        self._lock = threading.Lock()
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        with self._lock:
            self._table = dfs[ 0 ]
            if verbose > 0:
                print( f'{verbose_prefix}MemoryOp remembering {self._table.shape[0]} row(s)' )
            return ( self._table, )
    #/def __call__
#/class MemoryOp

class EmptyTableOp( TableOp ):
    """
    Ignores its input and always returns a 0-row DataFrame with the
    schema named by `table_schema` (a schema.table_schemas key) -- for
    bootstrapping a self-referencing loop's first iteration (see
    _WiringEngine's docstring / README.md's ### Loops section: a node
    needs *some* initial token on a name a downstream loop-controller
    reads, before that name's real producer -- the loop body -- ever
    fires).

    NOTE -- resolves against *core* silverbrain.schema.table_schemas
    specifically, not whichever table_schemas is furthest extended for
    the actual deployment (e.g. duckpond's own, which extends
    silverflow's, which extends this core one, and might one day add
    columns of its own to a schema this op needs). There's no clean way
    to fix that from inside silverbrain-arrow itself (this class lives in
    core, one layer below where any such extension would be defined) --
    likely needs either the caller to inject its own table_schemas dict
    at construction time, or a thin per-deployment subclass that
    overrides which table_schemas it resolves against. Left as a known
    limitation, not solved here.
    """
    def __init__(
        self: Self,
        table_schema: str,
        rng: np.random.Generator | None = None,
        ) -> None:
        if table_schema not in table_schemas:
            raise ValueError(
                f"EmptyTableOp: table_schema={table_schema!r} not found in "
                f"table_schemas; expected one of {tuple(table_schemas)}"
            )
        self._resolved_schema = table_schemas[ table_schema ]
        super().__init__(
            rng    = rng,
            input  = [ InputSchema.from_columns( {}, extra = 'open' ) ],
            output = [ OutputSchema( expr = bdd.Union( bdd.ColVar( f'_empty_table_out_{uuid4().hex}' ) ) ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        return ( pl.DataFrame( schema = self._resolved_schema ), )
    #/def __call__
#/class EmptyTableOp

# -- Placeholder Table Ops (interface known, implementation not bound yet)

class PlaceholderOp( TableOp ):
    """
    Stands in for a node whose interface is declared but whose
    implementation isn't bound yet -- built from a kind="variable"
    [[nodes]] entry (configTree.py). Declares the same arity-1
    input/output shape MemoryOp and the static-path read ops use, so
    wiring, schema checks, and __table_op_schema__ all see a real
    interface immediately. __call__ always raises: a placeholder is never
    meant to actually run -- replace self.tableOps[node_idn] with a real
    TableOp/Cell before this node is reached on a live execution path.

    The declared interface is a bdd.Expr -- pass it directly via `expr`
    (may reference ColVar, e.g. a config-map placeholder parsed by
    exprParse.py), or pass `columns` for the common fully-literal case
    (builds `Union(*[Atom(n,t) for n,t in columns.items()])` internally).
    Exactly one of expr/columns must be given.
    """
    def __init__(
        self: Self,
        expr: bdd.Expr | None = None,
        columns: dict[ str, pl.DataType ] | None = None,
        rng: np.random.Generator | None = None,
        ) -> None:
        if ( expr is None ) == ( columns is None ):
            raise ValueError( 'PlaceholderOp requires exactly one of expr or columns' )
        if columns is not None:
            expr = bdd.Union( *( bdd.Atom( n, t ) for n, t in columns.items() ) )
        self.expr = expr
        # input tolerates extra columns beyond the declared interface (a
        # fresh anonymous ColVar, same 'open' behavior the old columns-only
        # constructor had) -- independent of whether expr itself already
        # references its own ColVars.
        _extra = bdd.ColVar( f'_placeholder_extra_{uuid4().hex}' )
        super().__init__(
            rng = rng,
            input  = [ InputSchema( expr = bdd.Union( expr, _extra ) ) ],
            output = [ OutputSchema( expr = expr ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        raise NotImplementedError(
            "PlaceholderOp has no implementation -- swap self.tableOps"
            "[node_idn] for a real TableOp/Cell before this node is reached"
        )
    #/def __call__
#/class PlaceholderOp

# -- Expression Resolution Table Ops

class ResolveExpressionOp( TableOp ):
    """
    Resolves a Cell's table-processes signature map (each child actor's
    declared inputs/outputs, written as bdd.Expr set-expressions --
    bdd.signatures_to_polars schema) against a var_name-tagged bindings
    forest (bdd.bindings_to_polars schema), substituting every ColVar the
    bindings cover -- transitively, since a binding may itself reference
    other ColVars -- and leaving the rest untouched. This is "resolve as
    much as possible," not "require full grounding": a ColVar with no
    matching binding stays a ColVar leaf in the output, it isn't an error.
    ColVar scope is global across the whole call -- one BDDManager, so the
    same ColVar name referenced by two different actors' signatures is the
    same variable. See bdd.py's module docstring ("RESOLUTION") for the
    algorithm (bdd.close_bindings/bdd.resolve_signatures) and why this
    only ever resolves column *names* -- dtype attachment is a separate,
    later step.

    dfs[0]: signature forest (bdd.signatures_to_polars schema)
    dfs[1]: bindings forest (bdd.bindings_to_polars schema)
    Returns the resolved signature forest (same schema as dfs[0]).
    """
    def __init__(
        self: Self,
        rng: np.random.Generator | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema.from_columns( bdd._SIGNATURE_TREE_COLS ),
                InputSchema.from_columns( bdd._BINDINGS_TREE_COLS ),
            ],
            output = [ OutputSchema.from_columns( bdd._SIGNATURE_TREE_COLS ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        manager    = bdd.BDDManager()
        signatures = bdd.polars_to_signatures( dfs[0] )
        bindings   = bdd.polars_to_bindings( dfs[1] )
        resolved   = bdd.resolve_signatures( manager, signatures, bindings )
        out_df, _  = bdd.signatures_to_polars( resolved )
        return ( out_df, )
    #/def __call__
#/class ResolveExpressionOp
