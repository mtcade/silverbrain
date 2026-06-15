#
#//  parameterParse.py
#//  silverbrain
#
"""
Generic parameter-spec parsing: fixed, random, and searched values.

Downstream applications register par_name→dtype mappings via ParameterParser
and call to_polars() to convert JSON spec dicts to a flat DataFrame.

Each row in the resulting DataFrame has exactly one non-null value column;
which one depends on the registered dtype for that par_name.
"""

import itertools
import polars as pl

from typing import Any, Literal, Type
from typing_extensions import TypedDict, NotRequired, Self

from .schema import cannonical_schema as _cannonical_schema
from .utilities import TableBuilder
from .polarsDataTypeStrings import dtype_to_str


# -- Column naming

_PAR_IDX_DTYPE: pl.DataType = _cannonical_schema['par_idx']

_IDENTIFIER_COLS: dict[ str, pl.DataType ] = {
    'par_group':   pl.Utf8,
    'par_type':    pl.Utf8,
    'par_idx':     _PAR_IDX_DTYPE,
    'par_name':    pl.Utf8,
    'par_feature': pl.Utf8,
}


def col_for_dtype( dtype: pl.DataType ) -> str:
    """
    Derive a par_val_* column name from a polars DataType.

    pl.Int64            → 'par_val_int64'
    pl.Float64          → 'par_val_float64'
    pl.Utf8             → 'par_val_string'
    pl.List(pl.Int64)   → 'par_val_list_int64'
    pl.List(pl.Float64) → 'par_val_list_float64'
    pl.List(pl.Utf8)    → 'par_val_list_string'
    """
    s = dtype_to_str( dtype ).lower().replace( '(', '_' ).replace( ')', '' )
    return 'par_val_' + s


# Cached names for the six common types used in parsing dispatch.
_COL_STR    = col_for_dtype( pl.Utf8 )              # 'par_val_string'
_COL_INT    = col_for_dtype( pl.Int64 )              # 'par_val_int64'
_COL_FLOAT  = col_for_dtype( pl.Float64 )            # 'par_val_float64'
_COL_STRL   = col_for_dtype( pl.List( pl.Utf8 ) )    # 'par_val_list_string'
_COL_INTL   = col_for_dtype( pl.List( pl.Int64 ) )   # 'par_val_list_int64'
_COL_FLOATL = col_for_dtype( pl.List( pl.Float64 ) ) # 'par_val_list_float64'


# -- ValueDict TypedDicts
# Describe the valid shapes of per-parameter spec dicts in JSON settings files.

PrimitiveValue: Type = str | int | float
RegularValue:   Type = PrimitiveValue | list[ PrimitiveValue ]


class ValueDict_Search_Values( TypedDict, closed = True ):
    values_search: list[ PrimitiveValue ]

class ValueDict_Search_Int( TypedDict, closed = True ):
    start:            int
    step:             int
    min: NotRequired[ int ]
    max: NotRequired[ int ]

class ValueDict_Search_LogInt( TypedDict, closed = True ):
    start:            int
    log_step:         float
    min: NotRequired[ int ]
    max: NotRequired[ int ]

class ValueDict_Search_Float( TypedDict, closed = True ):
    start:              float
    step:               float
    min: NotRequired[ float ]
    max: NotRequired[ float ]

class ValueDict_Search_Log( TypedDict, closed = True ):
    start:              float
    log_step:           float
    min: NotRequired[ float ]
    max: NotRequired[ float ]

class ValueDict_Search_Logit( TypedDict, closed = True ):
    start:      float
    logit_step: float

class ValueDict_Random_Values( TypedDict, closed = True ):
    values_random: list[ PrimitiveValue ]

class ValueDict_Random_UniformInt( TypedDict, closed = True ):
    distribution: Literal[ "uniform_int" ]
    min: int
    max: int

class ValueDict_Random_LogUniformInt( TypedDict, closed = True ):
    distribution: Literal[ "log_uniform_int" ]
    min: int
    max: int

class ValueDict_Random_Uniform( TypedDict, closed = True ):
    distribution: Literal[ "uniform" ]
    min: float
    max: float

class ValueDict_Random_LogUniform( TypedDict, closed = True ):
    distribution: Literal[ "log_uniform" ]
    min: float
    max: float

class ValueDict_Random_LogitUniform( TypedDict, closed = True ):
    distribution: Literal[ "logit_uniform" ]
    min: float
    max: float

ValueDict: Type = (
    ValueDict_Search_Values
    | ValueDict_Search_Int | ValueDict_Search_LogInt
    | ValueDict_Search_Float | ValueDict_Search_Log | ValueDict_Search_Logit
    | ValueDict_Random_Values
    | ValueDict_Random_UniformInt | ValueDict_Random_LogUniformInt
    | ValueDict_Random_Uniform | ValueDict_Random_LogUniform
    | ValueDict_Random_LogitUniform
)


class ParamGroupDict( TypedDict, closed = False ):
    _groups: NotRequired[ list[ Self ] ]
    __extra_items__: RegularValue | ValueDict

class ParamDict( TypedDict, closed = False ):
    _groups: NotRequired[ list[ Self ] ]
    __extra_items__: ParamGroupDict


# -- ParameterParser

class ParameterParser:
    """
    Maps par_name strings to polars DataTypes, derives the parameters DataFrame
    schema, and converts JSON spec dicts to flat DataFrames.

    Usage::

        parser = ParameterParser()
        parser.register('n', pl.Int64)
        parser.register('rho', pl.Float64)
        df, _ = parser.to_polars(json_dict)
    """

    def __init__( self ) -> None:
        self._dtype_map: dict[ str, pl.DataType ] = {}

    def register( self, name: str, dtype: pl.DataType ) -> None:
        self._dtype_map[ name ] = dtype

    @property
    def schema( self ) -> dict[ str, pl.DataType ]:
        """Full row schema: identifier columns + one value column per unique registered dtype."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        return dict( _IDENTIFIER_COLS ) | val_cols

    @property
    def val_cols( self ) -> list[ str ]:
        """All par_val_* column names, in schema order."""
        return [ c for c in self.schema if c.startswith( 'par_val_' ) ]

    def col_for_name( self, name: str ) -> str:
        """Return the value column name for a registered par_name."""
        return col_for_dtype( self._dtype_map[ name ] )

    def col_for_value( self, value: Any ) -> tuple[ str, Any ]:
        """Map a Python value to (column_name, coerced_value) based on its runtime type."""
        if isinstance( value, bool ):
            return ( _COL_STR, str( value ) )
        if isinstance( value, int ):
            return ( _COL_INT, value )
        if isinstance( value, float ):
            return ( _COL_FLOAT, value )
        if isinstance( value, str ):
            return ( _COL_STR, value )
        if isinstance( value, list ) and value:
            if isinstance( value[0], int ):
                return ( _COL_INTL, value )
            if isinstance( value[0], float ):
                return ( _COL_FLOATL, value )
            return ( _COL_STRL, [ str( v ) for v in value ] )
        return ( _COL_STR, str( value ) )

    # -- Parsing

    def _valueDict_to_polars(
        self,
        valueDict: ValueDict,
        par_group: str,
        par_idx:   int,
        par_name:  str,
    ) -> pl.DataFrame:
        tb   = TableBuilder( schema = self.schema )
        base = { 'par_group': par_group, 'par_idx': par_idx, 'par_name': par_name }

        def _app( par_type: str, par_feature: str, val: dict ) -> None:
            tb.append(
                base | { 'par_type': par_type, 'par_feature': par_feature } | val,
                allow_missing = True,
            )

        d = valueDict

        if 'values_search' in d:
            col = self.col_for_name( par_name )
            _app( 'search', 'value', { col: d['values_search'] } )

        elif 'values_random' in d:
            col = self.col_for_name( par_name )
            _app( 'random', 'value', { col: d['values_random'] } )

        elif 'logit_step' in d:
            if not ( 0 < d['start'] < 1 ):
                raise ValueError( f"logit_step start must be in (0, 1), got {d['start']!r}" )
            _app( 'search', 'start',      { _COL_FLOAT: d['start']      } )
            _app( 'search', 'logit_step', { _COL_FLOAT: d['logit_step'] } )

        elif 'log_step' in d:
            if d['start'] <= 0:
                raise ValueError( f"log_step start must be > 0, got {d['start']!r}" )
            if isinstance( d['start'], int ):
                _app( 'search', 'start',    { _COL_INT:   d['start']    } )
                _app( 'search', 'log_step', { _COL_FLOAT: d['log_step'] } )
                if 'min' in d:
                    if d['min'] <= 0: raise ValueError( f"log_step min must be > 0, got {d['min']!r}" )
                    _app( 'search', 'min', { _COL_INT: d['min'] } )
                if 'max' in d:
                    if d['max'] <= 0: raise ValueError( f"log_step max must be > 0, got {d['max']!r}" )
                    _app( 'search', 'max', { _COL_INT: d['max'] } )
            else:
                _app( 'search', 'start',    { _COL_FLOAT: d['start']    } )
                _app( 'search', 'log_step', { _COL_FLOAT: d['log_step'] } )
                if 'min' in d:
                    if d['min'] <= 0: raise ValueError( f"log_step min must be > 0, got {d['min']!r}" )
                    _app( 'search', 'min', { _COL_FLOAT: d['min'] } )
                if 'max' in d:
                    if d['max'] <= 0: raise ValueError( f"log_step max must be > 0, got {d['max']!r}" )
                    _app( 'search', 'max', { _COL_FLOAT: d['max'] } )

        elif 'step' in d:
            if isinstance( d['start'], int ):
                _app( 'search', 'start', { _COL_INT: d['start'] } )
                _app( 'search', 'step',  { _COL_INT: d['step']  } )
                if 'min' in d: _app( 'search', 'min', { _COL_INT: d['min'] } )
                if 'max' in d: _app( 'search', 'max', { _COL_INT: d['max'] } )
            else:
                _app( 'search', 'start', { _COL_FLOAT: d['start'] } )
                _app( 'search', 'step',  { _COL_FLOAT: d['step']  } )
                if 'min' in d: _app( 'search', 'min', { _COL_FLOAT: d['min'] } )
                if 'max' in d: _app( 'search', 'max', { _COL_FLOAT: d['max'] } )

        elif 'distribution' in d:
            dist = d['distribution']
            _app( 'random', 'distribution', { _COL_STR: dist } )
            is_log   = dist.startswith( 'log' )
            is_logit = 'logit' in dist
            if dist in ( 'uniform_int', 'log_uniform_int' ):
                if 'min' in d:
                    if is_log   and d['min'] <= 0:               raise ValueError( f"min must be > 0 for {dist!r}, got {d['min']!r}" )
                    if is_logit and not ( 0 < d['min'] < 1 ):   raise ValueError( f"min must be in (0, 1) for {dist!r}, got {d['min']!r}" )
                    _app( 'random', 'min', { _COL_INT: d['min'] } )
                if 'max' in d:
                    if is_log   and d['max'] <= 0:               raise ValueError( f"max must be > 0 for {dist!r}, got {d['max']!r}" )
                    if is_logit and not ( 0 < d['max'] < 1 ):   raise ValueError( f"max must be in (0, 1) for {dist!r}, got {d['max']!r}" )
                    _app( 'random', 'max', { _COL_INT: d['max'] } )
            else:
                if 'min' in d:
                    if is_log   and d['min'] <= 0:               raise ValueError( f"min must be > 0 for {dist!r}, got {d['min']!r}" )
                    if is_logit and not ( 0 < d['min'] < 1 ):   raise ValueError( f"min must be in (0, 1) for {dist!r}, got {d['min']!r}" )
                    _app( 'random', 'min', { _COL_FLOAT: d['min'] } )
                if 'max' in d:
                    if is_log   and d['max'] <= 0:               raise ValueError( f"max must be > 0 for {dist!r}, got {d['max']!r}" )
                    if is_logit and not ( 0 < d['max'] < 1 ):   raise ValueError( f"max must be in (0, 1) for {dist!r}, got {d['max']!r}" )
                    _app( 'random', 'max', { _COL_FLOAT: d['max'] } )

        else:
            raise ValueError( "Unrecognized valueDict={!r}".format( valueDict ) )

        return tb.to_polars()

    def _paramGroup_to_polars(
        self,
        paramGroup_dict: ParamGroupDict,
        par_group:       str,
        sibling_df:      pl.DataFrame | None = None,
        par_idx_start:   int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
        schema       = self.schema
        df_own_list: list[ pl.DataFrame ] = []

        for k, v in paramGroup_dict.items():
            if k == '_groups':
                continue
            if isinstance( v, dict ):
                df_own_list.append( self._valueDict_to_polars(
                    valueDict = v, par_group = par_group,
                    par_idx   = par_idx_start, par_name = k,
                ) )
            else:
                col = self.col_for_name( k )
                df_own_list.append( pl.DataFrame(
                    ( { 'par_group': par_group, 'par_type': 'fixed',
                        'par_idx':   par_idx_start, 'par_name': k,
                        'par_feature': 'value', col: v }, ),
                    schema = schema,
                ) )

        if sibling_df is not None:
            reindexed = sibling_df.with_columns( pl.lit( par_idx_start, dtype=_PAR_IDX_DTYPE ).alias( 'par_idx' ) )
            df_combined = (
                pl.concat( [ reindexed ] + df_own_list, how = 'vertical' )
                if df_own_list else reindexed
            )
        elif df_own_list:
            df_combined = pl.concat( df_own_list, how = 'vertical' )
        else:
            df_combined = pl.DataFrame( schema = schema )

        if '_groups' not in paramGroup_dict:
            return df_combined, par_idx_start + 1

        par_idx = par_idx_start
        df_out_list: list[ pl.DataFrame ] = []
        for child in paramGroup_dict['_groups']:
            df_next, par_idx = self._paramGroup_to_polars(
                paramGroup_dict = child, par_group = par_group,
                sibling_df = df_combined, par_idx_start = par_idx,
            )
            df_out_list.append( df_next )
        return pl.concat( df_out_list, how = 'vertical' ), par_idx

    def to_polars(
        self,
        param_dict:    ParamDict,
        sibling_df:    pl.DataFrame | None = None,
        par_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
        """
        Convert a top-level parameter spec dict to a flat DataFrame.

        :param param_dict:    Top-level dict keyed by par_group (e.g. 'X', 'Xk').
        :param sibling_df:    Rows from an outer _groups scope; par_idx is overwritten.
        :param par_idx_start: First par_idx value to assign.
        :returns:             (DataFrame with self.schema, next available par_idx)
        """
        schema         = self.schema
        par_group_dfs: dict[ str, pl.DataFrame ] = {}

        for k, v in param_dict.items():
            if k == '_groups':
                continue
            df_k, _ = self._paramGroup_to_polars(
                paramGroup_dict = v, par_group = k, par_idx_start = 0,
            )
            par_group_dfs[ k ] = df_k

        if par_group_dfs:
            local_idx_sets = [
                sorted( df['par_idx'].unique().to_list() )
                for df in par_group_dfs.values()
            ]
            group_keys   = list( par_group_dfs.keys() )
            combinations = list( itertools.product( *local_idx_sets ) )

            record_dfs: list[ pl.DataFrame ] = []
            for offset, combo in enumerate( combinations ):
                global_idx = par_idx_start + offset
                record_dfs.append( pl.concat(
                    [
                        par_group_dfs[ k ]
                            .filter( pl.col( 'par_idx' ) == local_idx )
                            .with_columns( pl.lit( global_idx, dtype=_PAR_IDX_DTYPE ).alias( 'par_idx' ) )
                        for k, local_idx in zip( group_keys, combo )
                    ],
                    how = 'vertical',
                ) )
            df_own    = pl.concat( record_dfs, how = 'vertical' )
            n_records = len( combinations )
        else:
            df_own    = pl.DataFrame( schema = schema )
            n_records = 0

        if sibling_df is not None and n_records > 0:
            own_par_idxs = sorted( df_own['par_idx'].unique().to_list() )
            df_combined  = pl.concat(
                [
                    pl.concat(
                        [ sibling_df.with_columns( pl.lit( p, dtype=_PAR_IDX_DTYPE ).alias( 'par_idx' ) ),
                          df_own.filter( pl.col( 'par_idx' ) == p ) ],
                        how = 'vertical',
                    )
                    for p in own_par_idxs
                ],
                how = 'vertical',
            )
        elif sibling_df is not None:
            df_combined = sibling_df.with_columns( pl.lit( par_idx_start, dtype=_PAR_IDX_DTYPE ).alias( 'par_idx' ) )
            n_records   = 1
        else:
            df_combined = df_own

        par_idx_after_own = par_idx_start + n_records

        if '_groups' not in param_dict:
            return df_combined, par_idx_after_own

        par_idx      = par_idx_after_own
        df_out_list: list[ pl.DataFrame ] = []

        if n_records > 0:
            distinct_par_idxs = sorted( df_combined['par_idx'].unique().to_list() )
            for child in param_dict['_groups']:
                for p in distinct_par_idxs:
                    df_next, par_idx = self.to_polars(
                        param_dict    = child,
                        sibling_df    = df_combined.filter( pl.col( 'par_idx' ) == p ),
                        par_idx_start = par_idx,
                    )
                    df_out_list.append( df_next )
        else:
            for child in param_dict['_groups']:
                df_next, par_idx = self.to_polars(
                    param_dict = child, sibling_df = None, par_idx_start = par_idx,
                )
                df_out_list.append( df_next )

        return pl.concat( df_out_list, how = 'vertical' ), par_idx
#/class ParameterParser
