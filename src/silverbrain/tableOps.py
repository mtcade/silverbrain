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
from .polarsDataTypeStrings import dtype_to_str, str_to_dtype
from .schema import table_schemas

import polars as pl
import numpy as np

from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass, field
from itertools import chain
from os import path
from typing import Literal, Self, Sequence, Type
import json
import sqlite3

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

# -- TableOpSchema

Extra = Literal['forbidden', 'open', 'transparent']

@dataclass
class DiskWrite:
    path_input: int     # index into inputs — which df holds the path
    path_column: str    # column in that df containing the file path

    def to_polars( self: Self, op_idn: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':             [ op_idn ],
                'effect_index':       [ effect_index ],
                'type':               [ 'DiskWrite' ],
                'path_input':         [ self.path_input ],
                'path_column':        [ self.path_column ],
                'output':             [ None ],
                'format':             [ None ],
                'backend_input':      [ None ],
                'backend_id_column':  [ None ],
            },
            schema = table_schemas['__table_op_effects__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DiskWrite',
            'path_input': self.path_input,
            'path_column': self.path_column,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls( path_input = d['path_input'], path_column = d['path_column'] )
#/class DiskWrite

@dataclass
class DiskRead:
    path_input: int     # index into inputs — which df holds the path
    path_column: str    # column in that df containing the file path
    output: int         # index into outputs — which output receives the loaded data
    format: str         # e.g. 'parquet', 'json'

    def to_polars( self: Self, op_idn: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':             [ op_idn ],
                'effect_index':       [ effect_index ],
                'type':               [ 'DiskRead' ],
                'path_input':         [ self.path_input ],
                'path_column':        [ self.path_column ],
                'output':             [ self.output ],
                'format':             [ self.format ],
                'backend_input':      [ None ],
                'backend_id_column':  [ None ],
            },
            schema = table_schemas['__table_op_effects__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DiskRead',
            'path_input': self.path_input, 'path_column': self.path_column,
            'output': self.output, 'format': self.format,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            path_input = d['path_input'], path_column = d['path_column'],
            output = d['output'], format = d['format'],
        )
#/class DiskRead

@dataclass
class Mkdir:
    path_input: int     # index into inputs — which df holds the paths
    path_column: str    # column in that df containing the directory path

    def to_polars( self: Self, op_idn: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':             [ op_idn ],
                'effect_index':       [ effect_index ],
                'type':               [ 'Mkdir' ],
                'path_input':         [ self.path_input ],
                'path_column':        [ self.path_column ],
                'output':             [ None ],
                'format':             [ None ],
                'backend_input':      [ None ],
                'backend_id_column':  [ None ],
            },
            schema = table_schemas['__table_op_effects__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'Mkdir',
            'path_input': self.path_input,
            'path_column': self.path_column,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls( path_input = d['path_input'], path_column = d['path_column'] )
#/class Mkdir

@dataclass
class DBWrite:
    backend_input: int      # index into inputs — which df holds the backend_ref
    backend_id_column: str  # column in that df containing the backend_id

    def to_polars( self: Self, op_idn: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':             [ op_idn ],
                'effect_index':       [ effect_index ],
                'type':               [ 'DBWrite' ],
                'path_input':         [ None ],
                'path_column':        [ None ],
                'output':             [ None ],
                'format':             [ None ],
                'backend_input':      [ self.backend_input ],
                'backend_id_column':  [ self.backend_id_column ],
            },
            schema = table_schemas['__table_op_effects__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DBWrite',
            'backend_input': self.backend_input,
            'backend_id_column': self.backend_id_column,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            backend_input = d['backend_input'],
            backend_id_column = d['backend_id_column'],
        )
#/class DBWrite

@dataclass
class DBRead:
    backend_input: int      # index into inputs — which df holds the backend_ref
    backend_id_column: str  # column in that df containing the backend_id
    output: int             # index into outputs — which output receives the loaded data

    def to_polars( self: Self, op_idn: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':             [ op_idn ],
                'effect_index':       [ effect_index ],
                'type':               [ 'DBRead' ],
                'path_input':         [ None ],
                'path_column':        [ None ],
                'output':             [ self.output ],
                'format':             [ None ],
                'backend_input':      [ self.backend_input ],
                'backend_id_column':  [ self.backend_id_column ],
            },
            schema = table_schemas['__table_op_effects__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DBRead',
            'backend_input': self.backend_input,
            'backend_id_column': self.backend_id_column,
            'output': self.output,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            backend_input = d['backend_input'],
            backend_id_column = d['backend_id_column'],
            output = d['output'],
        )
#/class DBRead

Effect = DiskWrite | DiskRead | Mkdir | DBWrite | DBRead

_EFFECT_CLASSES: dict[ str, type ] = {
    'DiskWrite': DiskWrite,
    'DiskRead':  DiskRead,
    'Mkdir':     Mkdir,
    'DBWrite':   DBWrite,
    'DBRead':    DBRead,
}

@dataclass
class InputSchema():
    columns: dict[ str, pl.DataType ] = field( default_factory = dict )
    extra: Extra = 'open'
    #
    # 'forbidden'   : no extra columns allowed
    # 'open'        : extra columns allowed, ignored
    # 'transparent' : extra columns allowed, flow to output (see OutputSchema.passthrough_from)
    #

    def to_polars( self: Self, op_idn: str, index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':           [ op_idn ],
                'direction':        [ 'input' ],
                'index':            [ index ],
                'column_names':     [ list( self.columns.keys() ) ],
                'column_types':     [
                    [ dtype_to_str( t ) for t in self.columns.values() ]
                ],
                'extra':            [ self.extra ],
                'passthrough_from': [ None ],
            },
            schema = table_schemas['__table_op_schema__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'columns': { col: dtype_to_str( t ) for col, t in self.columns.items() },
            'extra': self.extra,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            columns = { col: str_to_dtype( s ) for col, s in d['columns'].items() },
            extra = d['extra'],
        )
#/class InputSchema

@dataclass
class OutputSchema():
    columns: dict[ str, pl.DataType ] = field( default_factory = dict )
    passthrough_from: list[ int ] = field( default_factory = list )
    #
    # passthrough_from: indices into TableOpSchema.inputs —
    #   extra columns from each listed input appear here
    # columns: columns explicitly constructed by this op (added on top of any passthrough)
    #

    def to_polars( self: Self, op_idn: str, index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_idn':           [ op_idn ],
                'direction':        [ 'output' ],
                'index':            [ index ],
                'column_names':     [ list( self.columns.keys() ) ],
                'column_types':     [
                    [ dtype_to_str( t ) for t in self.columns.values() ]
                ],
                'extra':            [ None ],
                'passthrough_from': [ self.passthrough_from ],
            },
            schema = table_schemas['__table_op_schema__'],
        )

    def to_dict( self: Self ) -> dict:
        return {
            'columns': { col: dtype_to_str( t ) for col, t in self.columns.items() },
            'passthrough_from': self.passthrough_from,
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            columns = { col: str_to_dtype( s ) for col, s in d['columns'].items() },
            passthrough_from = d.get( 'passthrough_from', [] ),
        )
#/class OutputSchema

@dataclass
class TableOpSchema():
    inputs: list[ InputSchema ]
    outputs: list[ OutputSchema ]
    effects: list[ Effect ] = field( default_factory = list )

    def to_polars_schema( self: Self, op_idn: str ) -> pl.DataFrame:
        frames = [
            inp.to_polars( op_idn, i ) for i, inp in enumerate( self.inputs )
        ] + [
            out.to_polars( op_idn, i ) for i, out in enumerate( self.outputs )
        ]
        return pl.concat( frames, how = 'vertical' )

    def to_polars_effects( self: Self, op_idn: str ) -> pl.DataFrame:
        frames = [ eff.to_polars( op_idn, i ) for i, eff in enumerate( self.effects ) ]
        return (
            pl.concat( frames, how = 'vertical' ) if frames
            else pl.DataFrame( schema = table_schemas['__table_op_effects__'] )
        )

    def to_polars( self: Self, op_idn: str ) -> tuple[ pl.DataFrame, pl.DataFrame ]:
        return ( self.to_polars_schema( op_idn ), self.to_polars_effects( op_idn ) )

    def to_dict( self: Self ) -> dict:
        return {
            'inputs':  [ inp.to_dict() for inp in self.inputs ],
            'outputs': [ out.to_dict() for out in self.outputs ],
            'effects': [ eff.to_dict() for eff in self.effects ],
        }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            inputs  = [ InputSchema.from_dict( i ) for i in d['inputs'] ],
            outputs = [ OutputSchema.from_dict( o ) for o in d['outputs'] ],
            effects = [
                _EFFECT_CLASSES[ e['type'] ].from_dict( e )
                for e in d.get( 'effects', [] )
            ],
        )
#/class TableOpSchema

class TableOpSchemaDict( UserDict[ str, TableOpSchema ] ):
    """A typed dict[str, TableOpSchema] mapping opId strings to their schemas."""

    def to_dict( self: Self ) -> dict[ str, dict ]:
        return { op_idn: op.to_dict() for op_idn, op in self.data.items() }

    @classmethod
    def from_dict( cls, d: dict[ str, dict ] ) -> Self:
        return cls( { op_idn: TableOpSchema.from_dict( v ) for op_idn, v in d.items() } )

    def to_polars_schema( self: Self ) -> pl.DataFrame:
        if not self.data:
            return pl.DataFrame( schema = table_schemas['__table_op_schema__'] )
        return pl.concat(
            [ op.to_polars_schema( op_idn ) for op_idn, op in self.data.items() ],
            how = 'vertical',
        )

    def to_polars_effects( self: Self ) -> pl.DataFrame:
        if not self.data:
            return pl.DataFrame( schema = table_schemas['__table_op_effects__'] )
        return pl.concat(
            [ op.to_polars_effects( op_idn ) for op_idn, op in self.data.items() ],
            how = 'vertical',
        )

    def to_polars_tuple( self: Self ) -> tuple[ pl.DataFrame, pl.DataFrame ]:
        return ( self.to_polars_schema(), self.to_polars_effects() )
#/class TableOpSchemaDict



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
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def __call__
#/class TableOp

# -- Atomic Table Ops

class WriteParquetOp( TableOp ):
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema( extra = 'open' ),
                InputSchema( columns = { 'fp': pl.Utf8 }, extra = 'transparent' ),
            ],
            output = [
                OutputSchema(
                    columns = { 'fp': pl.Utf8, 'isfile': pl.Boolean },
                    passthrough_from = [ 1 ],
                ),
            ],
            effects = [ DiskWrite( path_input = 1, path_column = 'fp' ) ],
        )
    #/def __init__

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
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__( rng = rng, effects = [] )
    #/def __init__

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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else [ InputSchema( extra = 'open' ) ],
            output  = output  if output  is not None else [ OutputSchema( passthrough_from = [ 0 ] ) ],
            effects = effects if effects is not None else [],
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else [ InputSchema( extra = 'open' ) ],
            output  = output  if output  is not None else [ OutputSchema( passthrough_from = [ 0 ] ) ],
            effects = effects if effects is not None else [],
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
        input: list[ InputSchema ] | None = None,
        output: list[ OutputSchema ] | None = None,
        effects: list[ Effect ] | None = None,
        ) -> None:
        super().__init__(
            rng = rng,
            input   = input   if input   is not None else [ InputSchema( extra = 'open' ) ],
            output  = output  if output  is not None else [ OutputSchema( passthrough_from = [ 0 ] ) ],
            effects = effects if effects is not None else [],
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

# -- Backend Storage Helpers

def _pl_dtype_to_sqlite_type( dtype: pl.DataType ) -> str:
    if dtype == pl.Boolean:
        return 'INTEGER'
    if dtype.is_integer():
        return 'INTEGER'
    if dtype.is_float():
        return 'REAL'
    return 'TEXT'
#/def _pl_dtype_to_sqlite_type

def _prepare_df_for_sqlite( df: pl.DataFrame ) -> pl.DataFrame:
    """Serialize List/Struct/Array columns to JSON strings for SQLite storage."""
    exprs = [
        pl.col( col ).map_elements( json.dumps, return_dtype = pl.String ).alias( col )
        for col, dtype in df.schema.items()
        if isinstance( dtype, ( pl.List, pl.Array, pl.Struct ) )
    ]
    return df.with_columns( exprs ) if exprs else df
#/def _prepare_df_for_sqlite

def _write_sqlite(
    df: pl.DataFrame,
    db_path: str,
    table_name: str,
    if_exists: str,
) -> None:
    df = _prepare_df_for_sqlite( df )
    conn = sqlite3.connect( db_path )
    try:
        cursor = conn.cursor()
        exists = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ( table_name, )
        ).fetchone() is not None
        if exists:
            if if_exists == 'fail':
                raise ValueError( f"Table '{table_name}' already exists in {db_path}" )
            elif if_exists == 'replace':
                cursor.execute( f'DROP TABLE "{table_name}"' )
                exists = False
        if not exists:
            col_defs = ', '.join(
                f'"{col}" {_pl_dtype_to_sqlite_type( dtype )}'
                for col, dtype in df.schema.items()
            )
            cursor.execute( f'CREATE TABLE "{table_name}" ({col_defs})' )
        placeholders = ', '.join( '?' for _ in df.columns )
        cursor.executemany(
            f'INSERT INTO "{table_name}" VALUES ({placeholders})',
            df.rows(),
        )
        conn.commit()
    finally:
        conn.close()
#/def _write_sqlite

def _read_sqlite( db_path: str, table_name: str ) -> pl.DataFrame:
    conn = sqlite3.connect( db_path )
    try:
        cursor = conn.cursor()
        cursor.execute( f'SELECT * FROM "{table_name}"' )
        rows = cursor.fetchall()
        col_names = [ desc[0] for desc in cursor.description ]
        if not rows:
            return pl.DataFrame( { name: [] for name in col_names } )
        return pl.DataFrame( rows, schema = col_names, orient = 'row' )
    finally:
        conn.close()
#/def _read_sqlite

# -- Backend Storage Ops (per-backend)

class ReadParquetOp( TableOp ):
    """
    Read data from a parquet file.
    dfs[0]: row(s) with 'fp' column (e.g. from __parquet_backend__)
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input   = [ InputSchema( columns = { 'fp': pl.Utf8 }, extra = 'forbidden' ) ],
            output  = [ OutputSchema() ],
            effects = [ DiskRead( path_input = 0, path_column = 'fp', output = 0, format = 'parquet' ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        fp = dfs[0]['fp'].item()
        if verbose > 0:
            print( f'{verbose_prefix}Reading parquet: {fp}' )
        return ( pl.read_parquet( fp ), )
    #/def __call__
#/class ReadParquetOp

class WriteSQLiteOp( TableOp ):
    """
    Write data to a SQLite table.
    dfs[0]: data to write
    dfs[1]: config row with 'db_path', 'table_name', 'if_exists'
    Returns dfs[1] unchanged.
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema( extra = 'open' ),
                InputSchema(
                    columns = { 'db_path': pl.Utf8, 'table_name': pl.Utf8, 'if_exists': pl.Utf8 },
                    extra = 'forbidden',
                ),
            ],
            output  = [ OutputSchema( passthrough_from = [ 1 ] ) ],
            effects = [ DBWrite( backend_input = 1, backend_id_column = 'db_path' ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        data, config = dfs[0], dfs[1]
        cfg = config.row( 0, named = True )
        if verbose > 0:
            print( f"{verbose_prefix}Writing SQLite: {cfg['db_path']}:{cfg['table_name']} (if_exists={cfg['if_exists']})" )
        _write_sqlite( data, cfg['db_path'], cfg['table_name'], cfg['if_exists'] )
        return ( config, )
    #/def __call__
#/class WriteSQLiteOp

class ReadSQLiteOp( TableOp ):
    """
    Read data from a SQLite table.
    dfs[0]: config row with 'db_path', 'table_name'
    Returns the loaded DataFrame.
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input   = [ InputSchema( columns = { 'db_path': pl.Utf8, 'table_name': pl.Utf8 }, extra = 'forbidden' ) ],
            output  = [ OutputSchema() ],
            effects = [ DBRead( backend_input = 0, backend_id_column = 'db_path', output = 0 ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        cfg = dfs[0].row( 0, named = True )
        if verbose > 0:
            print( f"{verbose_prefix}Reading SQLite: {cfg['db_path']}:{cfg['table_name']}" )
        return ( _read_sqlite( cfg['db_path'], cfg['table_name'] ), )
    #/def __call__
#/class ReadSQLiteOp

class WritePostgresOp( TableOp ):
    """UC: postgres backend not yet implemented."""
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema( extra = 'open' ),
                InputSchema(
                    columns = {
                        'connection_string': pl.Utf8, 'table_name': pl.Utf8,
                        'schema_name': pl.Utf8, 'if_exists': pl.Utf8,
                    },
                    extra = 'forbidden',
                ),
            ],
            output  = [ OutputSchema( passthrough_from = [ 1 ] ) ],
            effects = [ DBWrite( backend_input = 1, backend_id_column = 'connection_string' ) ],
        )
    #/def __init__

    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise NotImplementedError( 'UC: postgres backend not yet implemented' )
    #/def __call__
#/class WritePostgresOp

class ReadPostgresOp( TableOp ):
    """UC: postgres backend not yet implemented."""
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input   = [ InputSchema(
                columns = { 'connection_string': pl.Utf8, 'table_name': pl.Utf8, 'schema_name': pl.Utf8 },
                extra = 'forbidden',
            ) ],
            output  = [ OutputSchema() ],
            effects = [ DBRead( backend_input = 0, backend_id_column = 'connection_string', output = 0 ) ],
        )
    #/def __init__

    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise NotImplementedError( 'UC: postgres backend not yet implemented' )
    #/def __call__
#/class ReadPostgresOp

# -- Backend Storage Ops (dispatch)

class IsParquetBackendOp:
    """Check: dfs[0]['backend_table'] == '__parquet_backend__'. Source: [backend_resolved]."""
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['backend_table'].item() == '__parquet_backend__'
#/class IsParquetBackendOp

class IsSQLiteBackendOp:
    """Check: dfs[0]['backend_table'] == '__sqlite_backend__'. Source: [backend_resolved]."""
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['backend_table'].item() == '__sqlite_backend__'
#/class IsSQLiteBackendOp

class IsPostgresBackendOp:
    """Check: dfs[0]['backend_table'] == '__postgres_backend__'. Source: [backend_resolved]."""
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['backend_table'].item() == '__postgres_backend__'
#/class IsPostgresBackendOp

class JoinTableBackendOp:
    """UC: join table_ref + __backend_index__ → _table_backend_ref."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class JoinTableBackendOp

class JoinParquetBackendOp:
    """UC: join _table_backend_ref + __parquet_backend__ → _table_backend_ref_written."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class JoinParquetBackendOp

class JoinSQLiteBackendOp:
    """UC: join _table_backend_ref + __sqlite_backend__ → _table_backend_ref_sqlite."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class JoinSQLiteBackendOp

class JoinPostgresBackendOp:
    """UC: join _table_backend_ref + __postgres_backend__ → _table_backend_ref_postgres."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class JoinPostgresBackendOp

class WriteParquetFragmentOp:
    """UC: write _table to parquet backend given _table_backend_ref_parquet config."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class WriteParquetFragmentOp

class WriteSQLiteFragmentOp:
    """UC: write _table to sqlite backend given _table_backend_ref_sqlite config."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class WriteSQLiteFragmentOp

class WritePostgresFragmentOp:
    """UC: write _table to postgres backend given _table_backend_ref_postgres config."""
    def __call__( self, dfs, verbose = 0, verbose_prefix = '' ):
        raise Exception( "UC" )
#/class WritePostgresFragmentOp

def _resolve_backend(
    backend_ref: pl.DataFrame,
    backend_index: pl.DataFrame,
) -> tuple[ str, str, dict ]:
    """
    Returns (backend_id, backend_table, backend_ref_row).
    backend_ref_row includes all columns from backend_ref as a dict.
    """
    backend_id = backend_ref['backend_id'].item()
    row = backend_index.filter( pl.col('backend_id') == backend_id )
    if row.is_empty():
        raise KeyError( f"backend_id '{backend_id}' not found in __backend_index__" )
    backend_table = row['backend_table'].item()
    return backend_id, backend_table, backend_ref.row( 0, named = True )
#/def _resolve_backend

class BackendReadOp( TableOp ):
    """
    Dispatches a read to the correct storage backend.

    dfs[0]: backend_ref — single row with 'backend_id'
    dfs[1]: __backend_index__
    dfs[2]: __parquet_backend__
    dfs[3]: __sqlite_backend__
    dfs[4]: __postgres_backend__

    Returns the loaded data as a single DataFrame.
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema( columns = { 'backend_id': pl.Utf8 }, extra = 'open' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'backend_table': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'fp': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'db_path': pl.Utf8, 'table_name': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'connection_string': pl.Utf8, 'table_name': pl.Utf8, 'schema_name': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
            ],
            output  = [ OutputSchema() ],
            effects = [ DBRead( backend_input = 0, backend_id_column = 'backend_id', output = 0 ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        backend_ref, backend_index, parquet_cfg, sqlite_cfg, postgres_cfg = dfs
        backend_id, backend_table, _ = _resolve_backend( backend_ref, backend_index )

        if backend_table == '__parquet_backend__':
            row = parquet_cfg.filter( pl.col('backend_id') == backend_id ).row( 0, named = True )
            if verbose > 0:
                print( f"{verbose_prefix}BackendRead parquet: {row['fp']}" )
            return ( pl.read_parquet( row['fp'] ), )

        elif backend_table == '__sqlite_backend__':
            row = sqlite_cfg.filter( pl.col('backend_id') == backend_id ).row( 0, named = True )
            if verbose > 0:
                print( f"{verbose_prefix}BackendRead sqlite: {row['db_path']}:{row['table_name']}" )
            return ( _read_sqlite( row['db_path'], row['table_name'] ), )

        elif backend_table == '__postgres_backend__':
            raise NotImplementedError( 'UC: postgres backend not yet implemented' )

        else:
            raise ValueError( f"Unknown backend_table: '{backend_table}'" )
    #/def __call__
#/class BackendReadOp

class BackendWriteOp( TableOp ):
    """
    Dispatches a write to the correct storage backend.

    dfs[0]: data to write
    dfs[1]: backend_ref — single row with 'backend_id'; optionally 'if_exists' to override config default
    dfs[2]: __backend_index__
    dfs[3]: __parquet_backend__
    dfs[4]: __sqlite_backend__
    dfs[5]: __postgres_backend__

    Returns dfs[1] (backend_ref) unchanged.
    """
    def __init__( self: Self, rng: np.random.Generator | None = None ) -> None:
        super().__init__(
            rng = rng,
            input = [
                InputSchema( extra = 'open' ),
                InputSchema( columns = { 'backend_id': pl.Utf8 }, extra = 'open' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'backend_table': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'fp': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'db_path': pl.Utf8, 'table_name': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
                InputSchema( columns = { 'backend_id': pl.Utf8, 'connection_string': pl.Utf8, 'table_name': pl.Utf8, 'schema_name': pl.Utf8, 'if_exists': pl.Utf8 }, extra = 'forbidden' ),
            ],
            output  = [ OutputSchema( passthrough_from = [ 1 ] ) ],
            effects = [ DBWrite( backend_input = 1, backend_id_column = 'backend_id' ) ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        data, backend_ref, backend_index, parquet_cfg, sqlite_cfg, postgres_cfg = dfs
        backend_id, backend_table, ref_row = _resolve_backend( backend_ref, backend_index )
        override_if_exists: str | None = ref_row.get( 'if_exists' )

        if backend_table == '__parquet_backend__':
            row = parquet_cfg.filter( pl.col('backend_id') == backend_id ).row( 0, named = True )
            if_exists = override_if_exists if override_if_exists is not None else row['if_exists']
            fp = row['fp']
            if verbose > 0:
                print( f"{verbose_prefix}BackendWrite parquet: {fp} (if_exists={if_exists})" )
            if if_exists == 'fail' and path.isfile( fp ):
                raise FileExistsError( f"Parquet file already exists: {fp}" )
            if if_exists == 'append' and path.isfile( fp ):
                data = pl.concat( [ pl.read_parquet( fp ), data ] )
            data.write_parquet( fp )

        elif backend_table == '__sqlite_backend__':
            row = sqlite_cfg.filter( pl.col('backend_id') == backend_id ).row( 0, named = True )
            if_exists = override_if_exists if override_if_exists is not None else row['if_exists']
            if verbose > 0:
                print( f"{verbose_prefix}BackendWrite sqlite: {row['db_path']}:{row['table_name']} (if_exists={if_exists})" )
            _write_sqlite( data, row['db_path'], row['table_name'], if_exists )

        elif backend_table == '__postgres_backend__':
            raise NotImplementedError( 'UC: postgres backend not yet implemented' )

        else:
            raise ValueError( f"Unknown backend_table: '{backend_table}'" )

        return ( backend_ref, )
    #/def __call__
#/class BackendWriteOp
