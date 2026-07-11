#
#//  parameterParse.py
#//  silverbrain
#
"""
Generic parameter-spec parsing: fixed, random, and searched values.

Downstream applications register par_name→dtype mappings via ParameterParser,
then call either to_polars() or to_polars_flat() to convert a JSON-like spec
dict to a DataFrame.

Each row has exactly one non-null value column, depending on the registered
dtype for that par_name.

TWO REPRESENTATIONS

to_polars() / schema — tree-shaped: one row per node, no duplication of
shared content. Every row carries node_idx (a permanent id) and parent_idx
(the enclosing node's node_idx, or null at the root). No par_idx.

to_polars_flat() / schema_flat — flat, fully cross-producted: one row-group
per par_idx, with shared content duplicated across every combination it
belongs to. par_idx is a flat, reassignable id — only useful for grouping
rows within the same DataFrame, never meaningful across calls or compared to
a "before" snapshot. No node_idx/parent_idx. to_polars_flat() derives this by
calling to_polars() and expanding the resulting tree (see
_expand_tree_to_flat()).

RESERVED KEYS: `_or` AND `_and`

At any point, a dict may contain `_or`, a list of alternative dicts: exactly
one is chosen per combination (mutually exclusive), each combined with the
rest of the enclosing dict. `_and` is the AND counterpart: a list of dicts
that must ALL be included together (cross-producted with each other and the
rest of the enclosing dict) — useful for crossing multiple independent `_or`
blocks that have no natural owning key of their own, e.g.
{'_and': [{'_or': [...]}, {'_or': [...]}]}. Both may nest arbitrarily deep,
including directly inside each other, with no restriction.

Every `_or`/`_and` always mints an explicit junction node in the tree (row
with par_type='_or' or '_and', no par_name/value) — never a silent
pass-through. A node's own par_type fully determines how ITS children
relate: par_type == '_or' -> children are mutually exclusive alternatives;
anything else (a content node, or an '_and' junction) -> children are
unconditionally combined. This is what makes the tree walkable back into the
flat cross-product with no ambiguity, even when `_or`/`_and` sit at the top
level with no owning par_group (where a fixed sibling par_group and an
`_or`'s alternatives would otherwise be indistinguishable from node_idx/
parent_idx alone).

Top-level `_or`/`_and` alternatives that bundle multiple par_groups (e.g.
{'y': {...}, 'importances': {...}}) are wrapped in an implicit '_and'
junction per alternative, so that alternative's par_groups stay tied
together as one atomic choice rather than fanning out as independent
children of the outer junction.

to_polars_deprecated() / schema_deprecated() are a frozen, unmodified
snapshot of the pre-refactor implementation (par_idx + node_idx/parent_idx
in one flat table, reserved key `_groups`), kept only as a regression
oracle — do not use in new code.
"""

import itertools
import polars as pl

from typing import Any, Literal, Type
from typing_extensions import TypedDict, NotRequired, Self

from .schema import cannonical_schema as _cannonical_schema
from .utilities import TableBuilder
from .polarsDataTypeStrings import dtype_to_str


# -- Column naming

_PAR_IDX_DTYPE:    pl.DataType = _cannonical_schema['par_idx']

_IDENTIFIER_COLS: dict[ str, pl.DataType ] = {
    'par_group':   pl.Utf8,
    'par_type':    pl.Utf8,
    'par_idx':     _PAR_IDX_DTYPE,
    'par_name':    pl.Utf8,
    'par_feature': pl.Utf8,
}

# node_idx/parent_idx are purely internal to ParameterParser's tree schema now
# (no longer part of cannonical_schema — see parameterParse refactor plan).
_NODE_IDX_DTYPE:   pl.DataType = pl.UInt32
_PARENT_IDX_DTYPE: pl.DataType = pl.UInt32

_TREE_IDENTIFIER_COLS: dict[ str, pl.DataType ] = {
    'par_group':   pl.Utf8,
    'par_type':    pl.Utf8,
    'par_name':    pl.Utf8,
    'par_feature': pl.Utf8,
    'node_idx':    _NODE_IDX_DTYPE,
    'parent_idx':  _PARENT_IDX_DTYPE,
}

# Frozen dtypes for the deprecated oracle below — hardcoded rather than read from
# cannonical_schema, since node_idx/parent_idx are removed from cannonical_schema
# once the tree/flat split lands (see parameterParse refactor plan).
_NODE_IDX_DTYPE_DEPRECATED:   pl.DataType = pl.UInt32
_PARENT_IDX_DTYPE_DEPRECATED: pl.DataType = pl.UInt32

_IDENTIFIER_COLS_DEPRECATED: dict[ str, pl.DataType ] = {
    'par_group':   pl.Utf8,
    'par_type':    pl.Utf8,
    'par_idx':     _PAR_IDX_DTYPE,
    'par_name':    pl.Utf8,
    'par_feature': pl.Utf8,
    'node_idx':    _NODE_IDX_DTYPE_DEPRECATED,
    'parent_idx':  _PARENT_IDX_DTYPE_DEPRECATED,
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
_COL_BOOL   = col_for_dtype( pl.Boolean )            # 'par_val_boolean'


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
    _or: NotRequired[ list[ Self ] ]
    __extra_items__: RegularValue | ValueDict

class ParamDict( TypedDict, closed = False ):
    _or: NotRequired[ list[ Self ] ]
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
        df, _ = parser.to_polars_flat(json_dict)
    """

    def __init__( self ) -> None:
        self._dtype_map: dict[ str, pl.DataType ] = {}

    def register( self, name: str, dtype: pl.DataType ) -> None:
        self._dtype_map[ name ] = dtype
    #/def register
    
    @property
    def schema_flat( self ) -> dict[ str, pl.DataType ]:
        """Flat row schema: identifier columns (incl. par_idx, no node_idx/parent_idx)
        + one value column per unique registered dtype."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        return dict( _IDENTIFIER_COLS ) | val_cols
    #/def schema_flat

    @property
    def val_cols( self ) -> list[ str ]:
        """All par_val_* column names, in schema order."""
        return [ c for c in self.schema_flat if c.startswith( 'par_val_' ) ]
    #/def val_cols
    
    def col_for_name( self, name: str ) -> str:
        """Return the value column name for a registered par_name."""
        return col_for_dtype( self._dtype_map[ name ] )

    def col_for_value( self, value: Any ) -> tuple[ str, Any ]:
        """Map a Python value to (column_name, coerced_value) based on its runtime type."""
        if isinstance( value, bool ):
            return ( _COL_BOOL, value )
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

    # -- Tree (node_idx/parent_idx, no par_idx)
    #
    # A node's own par_type fully determines how ITS children relate:
    # par_type == '_or'  -> children are mutually exclusive alternatives.
    # anything else (a content node, or an '_and' junction) -> children are
    # unconditionally combined. '_or'/'_and' always mint an explicit junction
    # node (no silent pass-through), so this composes at arbitrary nesting
    # depth with no guard needed.

    @property
    def schema( self ) -> dict[ str, pl.DataType ]:
        """Tree row schema: identifier columns (incl. node_idx/parent_idx, no par_idx)
        + one value column per unique registered dtype. Junction rows (par_type in
        {'_or', '_and'}) carry no par_name/value, just identity."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        return dict( _TREE_IDENTIFIER_COLS ) | val_cols
    #/def schema

    def _junction_row(
        self,
        kind:       Literal[ '_or', '_and' ],
        par_group:  str | None,
        node_idx:   int,
        parent_idx: int | None,
    ) -> pl.DataFrame:
        row: dict[ str, Any ] = { 'par_type': kind, 'node_idx': node_idx, 'parent_idx': parent_idx }
        if par_group is not None:
            row['par_group'] = par_group
        return pl.DataFrame( ( row, ), schema = self.schema )
    #/def _junction_row

    def _valueDict_to_tree(
        self,
        valueDict:  ValueDict,
        par_group:  str,
        par_name:   str,
        node_idx:   int,
        parent_idx: int | None,
    ) -> pl.DataFrame:
        tb   = TableBuilder( schema = self.schema )
        base = {
            'par_group': par_group, 'par_name': par_name,
            'node_idx': node_idx, 'parent_idx': parent_idx,
        }

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
    #/def _valueDict_to_tree

    def _paramGroup_to_tree(
        self,
        paramGroup_dict:  ParamGroupDict,
        par_group:        str,
        parent_node_idx:  int | None = None,
        node_idx_start:   int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
        node_idx = node_idx_start
        rows: list[ pl.DataFrame ] = []

        own_keys = { k: v for k, v in paramGroup_dict.items() if k not in ( '_or', '_and' ) }

        if own_keys:
            this_node_idx = node_idx
            node_idx += 1
            for name, v in own_keys.items():
                if isinstance( v, dict ):
                    rows.append( self._valueDict_to_tree(
                        valueDict = v, par_group = par_group, par_name = name,
                        node_idx = this_node_idx, parent_idx = parent_node_idx,
                    ) )
                else:
                    col = self.col_for_name( name )
                    rows.append( pl.DataFrame(
                        ( { 'par_group': par_group, 'par_type': 'fixed', 'par_name': name,
                            'par_feature': 'value', col: v,
                            'node_idx': this_node_idx, 'parent_idx': parent_node_idx }, ),
                        schema = self.schema,
                    ) )
            attach_point = this_node_idx
        else:
            attach_point = parent_node_idx

        for key in ( '_or', '_and' ):
            if key not in paramGroup_dict:
                continue
            junction_idx = node_idx
            node_idx += 1
            rows.append( self._junction_row(
                kind = key, par_group = par_group, node_idx = junction_idx, parent_idx = attach_point,
            ) )
            for child in paramGroup_dict[ key ]:
                df_child, node_idx = self._paramGroup_to_tree(
                    paramGroup_dict = child, par_group = par_group,
                    parent_node_idx = junction_idx, node_idx_start = node_idx,
                )
                rows.append( df_child )

        if rows:
            return pl.concat( rows, how = 'vertical' ), node_idx
        return pl.DataFrame( schema = self.schema ), node_idx
    #/def _paramGroup_to_tree

    def to_polars(
        self,
        param_dict:       ParamDict,
        node_idx_start:   int = 0,
        parent_node_idx:  int | None = None,
    ) -> tuple[ pl.DataFrame, int ]:
        """
        Convert a top-level parameter spec dict to a tree-shaped DataFrame: one row
        per node (no duplication of shared content), with node_idx/parent_idx
        reconstructing the original _or/_and nesting. No par_idx -- see
        to_polars_flat() for the derived, fully cross-producted view.

        :param param_dict:      Top-level dict keyed by par_group (e.g. 'X', 'Xk').
        :param node_idx_start:  First node_idx value to assign.
        :param parent_node_idx: node_idx this call's own content is nested under;
            None at the root.
        :returns: (DataFrame with self.schema, next available node_idx)
        """
        node_idx = node_idx_start
        rows: list[ pl.DataFrame ] = []

        for k, v in param_dict.items():
            if k in ( '_or', '_and' ):
                continue
            df_k, node_idx = self._paramGroup_to_tree(
                paramGroup_dict = v, par_group = k,
                parent_node_idx = parent_node_idx, node_idx_start = node_idx,
            )
            rows.append( df_k )

        for key in ( '_or', '_and' ):
            if key not in param_dict:
                continue
            junction_idx = node_idx
            node_idx += 1
            rows.append( self._junction_row(
                kind = key, par_group = None, node_idx = junction_idx, parent_idx = parent_node_idx,
            ) )
            for child in param_dict[ key ]:
                # A top-level alternative can bundle multiple par_groups (e.g.
                # {'y': {...}, 'importances': {...}}) -- wrap each alternative
                # in its own '_and' junction so those par_groups' nodes are
                # tied together as ONE alternative, not fanned out as
                # independent (and indistinguishable) children of `junction_idx`.
                wrapper_idx = node_idx
                node_idx += 1
                rows.append( self._junction_row(
                    kind = '_and', par_group = None, node_idx = wrapper_idx, parent_idx = junction_idx,
                ) )
                df_child, node_idx = self.to_polars(
                    param_dict = child, node_idx_start = node_idx, parent_node_idx = wrapper_idx,
                )
                rows.append( df_child )

        if rows:
            return pl.concat( rows, how = 'vertical' ), node_idx
        return pl.DataFrame( schema = self.schema ), node_idx
    #/def to_polars

    # -- Flat expansion (to_polars_flat derives its output by walking the tree)

    def _tree_children_maps(
        self, tree_df: pl.DataFrame,
    ) -> tuple[ dict[ int | None, list[ int ] ], dict[ int, str | None ] ]:
        """
        :returns: (parent_idx -> [child node_idx], node_idx -> junction kind
            ('_or'/'_and') or None for a content node).
        """
        meta = tree_df.select( [ 'node_idx', 'parent_idx', 'par_type' ] ).unique( subset = [ 'node_idx' ] )
        node_kind: dict[ int, str | None ] = {}
        children:  dict[ int | None, list[ int ] ] = {}
        for row in meta.iter_rows( named = True ):
            n, p, t = row['node_idx'], row['parent_idx'], row['par_type']
            node_kind[ n ] = t if t in ( '_or', '_and' ) else None
            children.setdefault( p, [] ).append( n )
        return children, node_kind
    #/def _tree_children_maps

    def _expand_node(
        self,
        node_idx: int | None,
        children: dict[ int | None, list[ int ] ],
        node_kind: dict[ int, str | None ],
    ) -> list[ list[ int ] ]:
        """All full node_idx combinations required when `node_idx` (or the
        virtual root, if None) is included. '_or' junctions union their
        children's expansions; everything else (content node, '_and'
        junction, or root) cross-products them. Junction nodes never
        contribute their own node_idx to the result."""
        kids = children.get( node_idx, [] )
        kind = node_kind.get( node_idx ) if node_idx is not None else None

        if kind == '_or':
            combos: list[ list[ int ] ] = []
            for kid in kids:
                combos.extend( self._expand_node( kid, children, node_kind ) )
            return combos

        combos = [ [] ]
        for kid in kids:
            kid_combos = self._expand_node( kid, children, node_kind )
            combos = [ a + b for a in combos for b in kid_combos ]

        if node_idx is not None and kind is None:
            combos = [ [ node_idx ] + c for c in combos ]
        return combos
    #/def _expand_node

    def _expand_tree_to_flat(
        self, tree_df: pl.DataFrame, par_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
        content_df = tree_df.filter( ~pl.col( 'par_type' ).is_in( [ '_or', '_and' ] ) )
        children, node_kind = self._tree_children_maps( tree_df )
        combos = self._expand_node( None, children, node_kind )

        flat_cols = list( self.schema_flat.keys() )
        parts: list[ pl.DataFrame ] = []
        for offset, combo in enumerate( combos ):
            par_idx = par_idx_start + offset
            parts.append(
                content_df
                    .filter( pl.col( 'node_idx' ).is_in( combo ) )
                    .drop( [ 'node_idx', 'parent_idx' ] )
                    .with_columns( pl.lit( par_idx, dtype=_PAR_IDX_DTYPE ).alias( 'par_idx' ) )
                    .select( flat_cols )
            )

        flat_df = pl.concat( parts, how = 'vertical' ) if parts else pl.DataFrame( schema = self.schema_flat )
        return flat_df, len( combos )
    #/def _expand_tree_to_flat

    def to_polars_flat(
        self,
        param_dict:     ParamDict,
        par_idx_start:  int = 0,
        node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int, int ]:
        """
        Convert a top-level parameter spec dict to a flat, fully cross-producted
        DataFrame, derived by building the tree (to_polars()) and expanding it.

        :returns: (DataFrame with self.schema_flat, next available par_idx, next available node_idx)
        """
        tree_df, next_node_idx = self.to_polars( param_dict, node_idx_start = node_idx_start )
        flat_df, n_combos = self._expand_tree_to_flat( tree_df, par_idx_start = par_idx_start )
        return flat_df, par_idx_start + n_combos, next_node_idx
    #/def to_polars_flat

    # -- Deprecated oracle (frozen pre-refactor implementation)
    #
    # Verbatim snapshot of the original to_polars/_paramGroup_to_polars/
    # _valueDict_to_polars/.schema, from before the tree/flat split. Kept
    # unchanged as a regression oracle — later stages diff their output
    # against this. Do not modify.

    @property
    def schema_deprecated( self ) -> dict[ str, pl.DataType ]:
        """Frozen pre-refactor row schema: identifier columns (including
        node_idx/parent_idx) + one value column per unique registered dtype."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        return dict( _IDENTIFIER_COLS_DEPRECATED ) | val_cols
    #/def schema_deprecated

    def _valueDict_to_polars_deprecated(
        self,
        valueDict:  ValueDict,
        par_group:  str,
        par_idx:    int,
        par_name:   str,
        node_idx:   int,
        parent_idx: int | None,
    ) -> pl.DataFrame:
        tb   = TableBuilder( schema = self.schema_deprecated )
        base = {
            'par_group': par_group, 'par_idx': par_idx, 'par_name': par_name,
            'node_idx': node_idx, 'parent_idx': parent_idx,
        }

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
    #/def _valueDict_to_polars_deprecated

    def _paramGroup_to_polars_deprecated(
        self,
        paramGroup_dict:  ParamGroupDict,
        par_group:        str,
        sibling_df:       pl.DataFrame | None = None,
        par_idx_start:    int = 0,
        node_idx_start:   int = 0,
        parent_node_idx:  int | None = None,
    ) -> tuple[ pl.DataFrame, int, int ]:
        schema       = self.schema_deprecated
        df_own_list: list[ pl.DataFrame ] = []

        for k, v in paramGroup_dict.items():
            if k == '_groups':
                continue
            if isinstance( v, dict ):
                df_own_list.append( self._valueDict_to_polars_deprecated(
                    valueDict = v, par_group = par_group,
                    par_idx   = par_idx_start, par_name = k,
                    node_idx  = node_idx_start, parent_idx = parent_node_idx,
                ) )
            else:
                col = self.col_for_name( k )
                df_own_list.append( pl.DataFrame(
                    ( { 'par_group': par_group, 'par_type': 'fixed',
                        'par_idx':   par_idx_start, 'par_name': k,
                        'par_feature': 'value', col: v,
                        'node_idx': node_idx_start, 'parent_idx': parent_node_idx }, ),
                    schema = schema,
                ) )

        if df_own_list:
            this_node_idx = node_idx_start
            node_idx      = node_idx_start + 1
        else:
            this_node_idx = parent_node_idx
            node_idx      = node_idx_start

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
            return df_combined, par_idx_start + 1, node_idx

        par_idx = par_idx_start
        df_out_list: list[ pl.DataFrame ] = []
        for child in paramGroup_dict['_groups']:
            df_next, par_idx, node_idx = self._paramGroup_to_polars_deprecated(
                paramGroup_dict = child, par_group = par_group,
                sibling_df = df_combined, par_idx_start = par_idx,
                node_idx_start = node_idx, parent_node_idx = this_node_idx,
            )
            df_out_list.append( df_next )
        return pl.concat( df_out_list, how = 'vertical' ), par_idx, node_idx
    #/def _paramGroup_to_polars_deprecated

    def to_polars_deprecated(
        self,
        param_dict:       ParamDict,
        sibling_df:       pl.DataFrame | None = None,
        par_idx_start:    int = 0,
        node_idx_start:   int = 0,
        parent_node_idx:  int | None = None,
    ) -> tuple[ pl.DataFrame, int, int ]:
        """Frozen pre-refactor to_polars — see class-level note above. Do not modify."""
        schema         = self.schema_deprecated
        par_group_dfs: dict[ str, pl.DataFrame ] = {}

        node_idx = node_idx_start
        for k, v in param_dict.items():
            if k == '_groups':
                continue
            df_k, _, node_idx = self._paramGroup_to_polars_deprecated(
                paramGroup_dict = v, par_group = k, par_idx_start = 0,
                node_idx_start = node_idx, parent_node_idx = parent_node_idx,
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
            return df_combined, par_idx_after_own, node_idx

        for child in param_dict['_groups']:
            if '_groups' in child:
                raise ValueError(
                    "_groups cannot be nested directly inside another _groups "
                    "(no par_group dict in between). Nest the inner _groups inside "
                    "a specific par_group's own dict instead, e.g. "
                    "{'X': {'_groups': [...]}} rather than "
                    "{'_groups': [{'_groups': [...]}]}."
                )

        par_idx      = par_idx_after_own
        df_out_list: list[ pl.DataFrame ] = []

        if n_records > 0:
            distinct_par_idxs = sorted( df_combined['par_idx'].unique().to_list() )
            for child in param_dict['_groups']:
                for p in distinct_par_idxs:
                    df_next, par_idx, node_idx = self.to_polars_deprecated(
                        param_dict      = child,
                        sibling_df      = df_combined.filter( pl.col( 'par_idx' ) == p ),
                        par_idx_start   = par_idx,
                        node_idx_start  = node_idx,
                        parent_node_idx = parent_node_idx,
                    )
                    df_out_list.append( df_next )
        else:
            for child in param_dict['_groups']:
                df_next, par_idx, node_idx = self.to_polars_deprecated(
                    param_dict      = child, sibling_df = None, par_idx_start = par_idx,
                    node_idx_start  = node_idx, parent_node_idx = parent_node_idx,
                )
                df_out_list.append( df_next )

        return pl.concat( df_out_list, how = 'vertical' ), par_idx, node_idx
    #/def to_polars_deprecated
#/class ParameterParser
