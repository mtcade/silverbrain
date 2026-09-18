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

RESERVED KEYS: `_or` XOR `_and`

At any point, a dict may contain EITHER `_or`, a list of alternative dicts
(exactly one is chosen per combination -- mutually exclusive, each combined
with the rest of the enclosing dict), OR `_and`, the AND counterpart (a list
of dicts that must ALL be included together, cross-producted with each other
and the rest of the enclosing dict) -- never both in the same dict. There is
no expressive loss from this restriction: a dict that would otherwise need
both is just as easily written with one nested inside the other, e.g.
{'_and': [{'_or': [...]}, {...}]} instead of {'_or': [...], '_and': [...]}
as siblings -- so parameterParse simply never allows the ambiguous sibling
form, and raises ValueError on a dict containing both keys. Both `_or` and
`_and` may otherwise nest arbitrarily deep, including directly inside each
other, with no restriction.

Every `_or`/`_and` always mints an explicit junction node in the tree (row
with par_type='_or' or '_and', no par_name/value) — never a silent
pass-through. A node's own par_type fully determines how ITS children
relate: par_type == '_or' -> children are mutually exclusive alternatives;
anything else (a content node, or an '_and' junction) -> children are
unconditionally combined. This is what makes the tree walkable back into the
flat cross-product with no ambiguity, even at the top level, with no owning
node (where a fixed sibling group and an `_or`'s alternatives would
otherwise be indistinguishable from node_idx/parent_idx alone).

parameterParse has no concept of a named "domain group" or "par_group" at
any level, including the top level passed to to_polars()/to_polars_flat()
-- every dict, at every depth, is parsed by the exact same rule: its own
non-reserved keys are ordinary par_name -> value/ValueDict leaves (siblings,
sharing one content node_idx), plus at most one of `_or`/`_and` for
structure. Grouping/labeling par_names for an application's own purposes
(e.g. "this cluster of parameters is conceptually 'space' settings") is
entirely an application-level concern, done with an ordinary par_name of its
own if needed -- never a key parameterParse itself treats specially. A
consumer that needs a specific row back out of the result does so via
par_name (see ParameterParser.subtree() below for extracting a whole
subtree rooted at a given node_idx, and leaf_paths() for the branch from
root to each terminal leaf).

RESERVED KEY: `_comment`

At any point a dict may also contain `_comment` -- free-form documentation
for a human reading the JSON/TOML spec, never a par_name. Silently ignored
everywhere a dict's own keys are otherwise walked (own_keys, dtype
inference); its value is never inspected.
"""

import polars as pl

from dataclasses import dataclass, field
from typing import Any, Literal, Sequence, Type
from typing_extensions import TypedDict, NotRequired, Self

from .schema import (
    cannonical_schema as _cannonical_schema,
    col_for_dtype,
    _IDENTIFIER_COLS,
    _TREE_IDENTIFIER_COLS,
    _CANDIDATE_LIST_COLS,
)
from .utilities import TableBuilder


# -- Column naming
#
# _IDENTIFIER_COLS/_TREE_IDENTIFIER_COLS/_CANDIDATE_LIST_COLS/
# col_for_dtype() all now live in schema.py -- the single source of truth
# for column names/dtypes (including node_idx/parent_idx, which used to
# be hand-duplicated here). Imported, not redefined.

_PAR_IDX_DTYPE: pl.DataType = _cannonical_schema['par_idx']

# '_comment' is a free-form documentation key (module docstring's RESERVED
# KEY section) -- silently dropped wherever a dict's own keys are walked,
# never treated as a par_name.
_IGNORED_KEY = '_comment'

# Cached names for the six common types used in parsing dispatch.
_COL_STR    = col_for_dtype( pl.Utf8 )              # 'par_val_string'
_COL_INT    = col_for_dtype( pl.Int64 )              # 'par_val_int64'
_COL_FLOAT  = col_for_dtype( pl.Float64 )            # 'par_val_float64'
_COL_STRL   = col_for_dtype( pl.List( pl.Utf8 ) )    # 'par_val_list_string'
_COL_INTL   = col_for_dtype( pl.List( pl.Int64 ) )   # 'par_val_list_int64'
_COL_FLOATL = col_for_dtype( pl.List( pl.Float64 ) ) # 'par_val_list_float64'
_COL_BOOL   = col_for_dtype( pl.Boolean )            # 'par_val_boolean'


def _coerce_values_for_dtype(
    values: list, dtype: pl.DataType, *, par_name: str, spec_key: str,
) -> list:
    """Validate + cast a literal candidate list against a registered dtype,
    raising ValueError if any item isn't reasonably castable."""
    if not isinstance( values, list ) or len( values ) == 0:
        raise ValueError(
            f"{spec_key!r} for par_name={par_name!r} must be a non-empty "
            f"list, got {values!r}"
        )
    try:
        coerced = pl.Series( values, dtype=dtype, strict=True )
    except Exception as e:
        raise ValueError(
            f"{spec_key!r} values for par_name={par_name!r} are not "
            f"castable to registered dtype {dtype!r}: {values!r} ({e})"
        ) from e
    return coerced.to_list()
#/def _coerce_values_for_dtype


# -- ValueDict TypedDicts
# Describe the valid shapes of per-parameter spec dicts in JSON settings files.

PrimitiveValue: Type = str | int | float
RegularValue:   Type = PrimitiveValue | list[ PrimitiveValue ]


class ValueDict_Search_Values( TypedDict, closed = True ):
    values_search: list[ PrimitiveValue | list[ PrimitiveValue ] ]

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
    values_random: list[ PrimitiveValue | list[ PrimitiveValue ] ]

class ValueDict_Random_UniformInt( TypedDict, closed = True ):
    distribution: Literal[ "uniform_int" ]
    min: int
    max: int
    repeat: NotRequired[ Literal[ "constant", "independent" ] ]

class ValueDict_Random_LogUniformInt( TypedDict, closed = True ):
    distribution: Literal[ "log_uniform_int" ]
    min: int
    max: int
    repeat: NotRequired[ Literal[ "constant", "independent" ] ]

class ValueDict_Random_Uniform( TypedDict, closed = True ):
    distribution: Literal[ "uniform" ]
    min: float
    max: float
    repeat: NotRequired[ Literal[ "constant", "independent" ] ]

class ValueDict_Random_LogUniform( TypedDict, closed = True ):
    distribution: Literal[ "log_uniform" ]
    min: float
    max: float
    repeat: NotRequired[ Literal[ "constant", "independent" ] ]

class ValueDict_Random_LogitUniform( TypedDict, closed = True ):
    distribution: Literal[ "logit_uniform" ]
    min: float
    max: float
    repeat: NotRequired[ Literal[ "constant", "independent" ] ]

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
    # At most one of _or/_and is ever present -- see module docstring's
    # RESERVED KEYS section (enforced at runtime in ParameterParser.to_polars(),
    # not expressible as a TypedDict constraint).
    _or:  NotRequired[ list[ Self ] ]
    _and: NotRequired[ list[ Self ] ]
    __extra_items__: RegularValue | ValueDict


# -- Leaf path (tree-walk result)

@dataclass
class LeafPath:
    """One leaf of a tree-shaped parameter DataFrame (see ParameterParser.leaf_paths())."""
    content_node_idxs: list[ int ]
    choices:           list[ tuple[ int, int ] ]  # (junction_node_idx, chosen_child_node_idx), root -> leaf

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
        self._dtype_map:         dict[ str, pl.DataType ] = {}
        # Doubled-nesting candidate-list columns for pl.List(...)-registered
        # dtypes (e.g. pl.List(pl.Float64) -> pl.List(pl.List(pl.Float64))),
        # added lazily in register(), one per distinct inner dtype.
        self._list_wrapped_cols: dict[ str, pl.DataType ] = {}

    def register( self, name: str, dtype: pl.DataType ) -> None:
        self._dtype_map[ name ] = dtype
        if isinstance( dtype, pl.List ):
            wrapped_dtype = pl.List( dtype )
            wrapped_col   = col_for_dtype( wrapped_dtype )
            if wrapped_col not in _CANDIDATE_LIST_COLS and wrapped_col not in self._list_wrapped_cols:
                self._list_wrapped_cols[ wrapped_col ] = wrapped_dtype
        #/if isinstance( dtype, pl.List )
    #/def register

    @property
    def schema_flat( self ) -> dict[ str, pl.DataType ]:
        """Flat row schema: identifier columns (incl. par_idx, no node_idx/parent_idx)
        + one value column per unique registered dtype, + the fixed
        values_random/values_search candidate-list columns."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        val_cols |= _CANDIDATE_LIST_COLS
        val_cols |= self._list_wrapped_cols
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

    def _values_col_and_coerced(
        self, par_name: str, values: list, *, spec_key: str,
    ) -> tuple[ str, list ]:
        """Resolve the candidate-list column + coerced values for a
        values_random/values_search spec on a registered par_name."""
        dtype = self._dtype_map[ par_name ]
        if dtype not in ( pl.Utf8, pl.Int64, pl.Float64 ) and not isinstance( dtype, pl.List ):
            raise ValueError(
                f"{spec_key!r} is only supported for par_names registered as "
                f"Utf8/Int64/Float64 or pl.List(...), got par_name={par_name!r} dtype={dtype!r}"
            )
        coerced = _coerce_values_for_dtype( values, dtype, par_name=par_name, spec_key=spec_key )
        col     = col_for_dtype( pl.List( dtype ) )
        return col, coerced
    #/def _values_col_and_coerced

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
        + one value column per unique registered dtype, + the fixed
        values_random/values_search candidate-list columns. Junction rows
        (par_type in {'_or', '_and'}) carry no par_name/value, just identity."""
        val_cols: dict[ str, pl.DataType ] = {
            col_for_dtype( dtype ): dtype
            for dtype in dict.fromkeys( self._dtype_map.values() )
        }
        val_cols |= _CANDIDATE_LIST_COLS
        val_cols |= self._list_wrapped_cols
        return dict( _TREE_IDENTIFIER_COLS ) | val_cols
    #/def schema

    def _junction_row(
        self,
        kind:       Literal[ '_or', '_and' ],
        node_idx:   int,
        parent_idx: int | None,
    ) -> pl.DataFrame:
        row: dict[ str, Any ] = { 'par_type': kind, 'node_idx': node_idx, 'parent_idx': parent_idx }
        return pl.DataFrame( ( row, ), schema = self.schema )
    #/def _junction_row

    def _valueDict_to_tree(
        self,
        valueDict:  ValueDict,
        par_name:   str,
        node_idx:   int,
        parent_idx: int | None,
    ) -> pl.DataFrame:
        tb   = TableBuilder( schema = self.schema )
        base = {
            'par_name': par_name,
            'node_idx': node_idx, 'parent_idx': parent_idx,
        }

        def _app( par_type: str, par_feature: str, val: dict ) -> None:
            tb.append(
                base | { 'par_type': par_type, 'par_feature': par_feature } | val,
                allow_missing = True,
            )

        d = valueDict

        if 'repeat' in d and 'distribution' not in d:
            raise ValueError(
                "'repeat' is only supported on distribution-based random specs "
                "(with 'distribution'), got valueDict={!r}".format( valueDict )
            )

        if 'values_search' in d:
            col, values = self._values_col_and_coerced( par_name, d['values_search'], spec_key='values_search' )
            _app( 'search', 'value', { col: values } )

        elif 'values_random' in d:
            col, values = self._values_col_and_coerced( par_name, d['values_random'], spec_key='values_random' )
            _app( 'random', 'value', { col: values } )

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
            repeat_val = d.get( 'repeat', 'constant' )
            if repeat_val not in ( 'constant', 'independent' ):
                raise ValueError( f"repeat must be 'constant' or 'independent', got {repeat_val!r}" )
            _app( 'random', 'repeat', { _COL_STR: repeat_val } )
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

    def to_polars(
        self,
        param_dict:       ParamGroupDict,
        node_idx_start:   int = 0,
        parent_node_idx:  int | None = None,
    ) -> tuple[ pl.DataFrame, int ]:
        """
        Convert a parameter spec dict to a tree-shaped DataFrame: one row per
        node (no duplication of shared content), with node_idx/parent_idx
        reconstructing the original _or/_and nesting. No par_idx -- see
        to_polars_flat() for the derived, fully cross-producted view.

        Every level is parsed by the same rule (recursive: a `_or`/`_and`
        child is itself a ParamGroupDict, parsed by this same method) -- see
        module docstring's RESERVED KEYS section. A dict's own non-reserved
        keys are ordinary par_name -> value/ValueDict leaves, all sharing one
        content node_idx; `_or` and `_and` are the only structural devices,
        and a dict containing both raises ValueError (nest one inside the
        other instead -- there is no case that genuinely needs both as
        siblings).

        :param param_dict:      The spec dict for this call's own level.
        :param node_idx_start:  First node_idx value to assign.
        :param parent_node_idx: node_idx this call's own content is nested under;
            None at the root.
        :returns: (DataFrame with self.schema, next available node_idx)
        """
        if '_or' in param_dict and '_and' in param_dict:
            raise ValueError(
                "A dict may not contain both '_or' and '_and' -- nest one "
                "inside the other instead, e.g. {'_and': [{'_or': [...]}, {...}]}"
            )

        node_idx = node_idx_start
        rows: list[ pl.DataFrame ] = []

        own_keys = { k: v for k, v in param_dict.items() if k not in ( '_or', '_and', _IGNORED_KEY ) }

        if own_keys:
            this_node_idx = node_idx
            node_idx += 1
            for name, v in own_keys.items():
                if isinstance( v, dict ):
                    rows.append( self._valueDict_to_tree(
                        valueDict = v, par_name = name,
                        node_idx = this_node_idx, parent_idx = parent_node_idx,
                    ) )
                else:
                    col = self.col_for_name( name )
                    rows.append( pl.DataFrame(
                        ( { 'par_type': 'fixed', 'par_name': name,
                            'par_feature': 'value', col: v,
                            'node_idx': this_node_idx, 'parent_idx': parent_node_idx }, ),
                        schema = self.schema,
                    ) )
            attach_point = this_node_idx
        else:
            attach_point = parent_node_idx

        for key in ( '_or', '_and' ):
            if key not in param_dict:
                continue
            junction_idx = node_idx
            node_idx += 1
            rows.append( self._junction_row(
                kind = key, node_idx = junction_idx, parent_idx = attach_point,
            ) )
            for child in param_dict[ key ]:
                df_child, node_idx = self.to_polars(
                    param_dict = child,
                    parent_node_idx = junction_idx, node_idx_start = node_idx,
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

    def _expand_node_with_choices(
        self,
        node_idx: int | None,
        children: dict[ int | None, list[ int ] ],
        node_kind: dict[ int, str | None ],
    ) -> list[ tuple[ list[ int ], list[ tuple[ int, int ] ] ] ]:
        """Same recursion as _expand_node, but each combo also carries the
        ordered (junction_node_idx, chosen_child_node_idx) trail of every
        '_or' decision made to reach it. Children are visited in ascending
        node_idx order so the resulting leaf order is deterministic and
        matches declaration order in the source spec."""
        kids = sorted( children.get( node_idx, [] ) )
        kind = node_kind.get( node_idx ) if node_idx is not None else None

        if kind == '_or':
            combos: list[ tuple[ list[ int ], list[ tuple[ int, int ] ] ] ] = []
            for kid in kids:
                for content, choices in self._expand_node_with_choices( kid, children, node_kind ):
                    combos.append( ( content, [ ( node_idx, kid ) ] + choices ) )
            return combos

        combos = [ ( [], [] ) ]
        for kid in kids:
            kid_combos = self._expand_node_with_choices( kid, children, node_kind )
            combos = [
                ( a_content + b_content, a_choices + b_choices )
                for a_content, a_choices in combos
                for b_content, b_choices in kid_combos
            ]

        if node_idx is not None and kind is None:
            combos = [ ( [ node_idx ] + c, ch ) for c, ch in combos ]
        return combos
    #/def _expand_node_with_choices

    def leaf_paths( self, tree_df: pl.DataFrame ) -> list[ 'LeafPath' ]:
        """
        Enumerate leaf paths through a tree-shaped DataFrame (from to_polars()),
        walking the whole tree from its true root.

        Each leaf corresponds to one full combination -- same as one
        _expand_node() combo -- but also records the ordered trail of '_or'
        decisions (junction_node_idx, chosen_child_node_idx) made from the
        root down to that leaf, so callers can walk/report/resume a tree
        search without flattening away which choice was made where.
        """
        children, node_kind = self._tree_children_maps( tree_df )

        leaves: list[ LeafPath ] = []
        for content, choices in self._expand_node_with_choices( None, children, node_kind ):
            leaves.append( LeafPath( content_node_idxs=content, choices=choices ) )
        return leaves
    #/def leaf_paths

    def subtree( self, tree_df: pl.DataFrame, node_idx: int ) -> pl.DataFrame:
        """
        All rows in the subtree rooted at `node_idx` (inclusive) -- `node_idx`
        itself plus every descendant, found by walking parent_idx forward
        (via the same children map leaf_paths()/to_polars_flat() use) from
        `node_idx` down. The counterpart to leaf_paths()'s branch-to-root
        walk (which starts at a terminal leaf and walks up to the true
        root): this instead starts at any node and walks down.

        node_idx/parent_idx values are returned as-is (not renumbered), so
        the result composes with leaf_paths()/_expand_tree_to_flat()-style
        consumers unchanged as long as it's read starting from `node_idx`
        (its own parent_idx still points outside the returned rows). A
        caller wanting a fresh, self-contained, independently-renumbered
        tree (e.g. to feed back through to_polars_flat()'s own node_idx_start
        threading as an unrelated forest) renumbers separately -- not this
        method's concern, per parameterParse's own module docstring:
        applications get the subtrees/branches they want out of the general
        tree/leaf primitives, not a bespoke grouping concept baked in here.

        Raises KeyError if `node_idx` isn't present in `tree_df` at all.
        """
        if node_idx not in tree_df[ 'node_idx' ]:
            raise KeyError( f"node_idx={node_idx!r} not present in tree_df" )

        children, _ = self._tree_children_maps( tree_df )
        keep = self._subtree_node_idxs( node_idx, children )
        return tree_df.filter( pl.col( 'node_idx' ).is_in( list( keep ) ) )
    #/def subtree

    def _subtree_node_idxs(
        self, node_idx: int, children: dict[ int | None, list[ int ] ],
    ) -> set[ int ]:
        """node_idx plus every descendant's own node_idx (the set subtree()
        itself filters tree_df down to) -- split out so branch() can reuse
        the exact same downward walk without re-deriving `children` or
        re-filtering tree_df a second time."""
        keep: set[ int ] = set()
        frontier = [ node_idx ]
        while frontier:
            n = frontier.pop()
            if n in keep:
                continue
            keep.add( n )
            frontier.extend( children.get( n, [] ) )
        return keep
    #/def _subtree_node_idxs

    @staticmethod
    def _content_value( row: dict[ str, Any ] ) -> Any:
        """The single non-null par_val_* value on one to_polars() row, or
        None if it's a junction row ('_or'/'_and', no par_name/value at
        all) -- same "whichever par_val_* column is set" pattern
        silverflow.tableOps._search_scalar uses for a single reserved
        par_name, generalized here to an arbitrary row."""
        for k, v in row.items():
            if k.startswith( 'par_val_' ) and v is not None:
                return v
        return None
    #/def _content_value

    def branch( self, tree_df: pl.DataFrame, path: Sequence[ tuple[ str, Any ] ] ) -> pl.DataFrame:
        """
        Walk `path` -- an ordered sequence of (par_name, value) pairs --
        down from tree_df's true root, at each step descending into the
        nearest descendant node (crossing through any intervening
        '_or'/'_and' junction node transparently -- e.g. par_group=
        'actor' -> actor_type='plant' passes through an '_or' junction
        node with no par_name/value of its own) whose own content rows
        include that exact (par_name, value) pair, then narrowing the
        search to JUST that node's own children before matching the next
        path element (so a value re-used under a different branch, e.g.
        the same par_name appearing under both 'plant' and 'sentient',
        can never be matched from the wrong branch). Returns the UNION of:

          - every node's own content rows along that root-to-match path
            (so a value declared at an ANCESTOR level -- e.g. one shared
            across every species below it, like 'global_health_decay' one
            level above every species_idn -- is included alongside the
            specific match), and
          - subtree(tree_df, matched_node_idx) for the LAST path
            element's own match (so values nested further below the
            final match -- e.g. everything under one specific species_idn
            -- are included too).

        The third primitive alongside subtree() (walks down from an
        already-known node_idx) and leaf_paths() (enumerates every fully-
        expanded leaf's own root-to-leaf '_or' trail) -- this is for "give
        me the one branch matching this specific chain of labels" without
        already knowing its node_idx, e.g.
        `branch(tree_df, [('par_group','actor'), ('actor_type','plant'),
        ('species_idn','grass_0')])`.

        Idempotent: `branch(branch(tree_df, path), path) == branch(tree_df,
        path)` (identical rows, same node_idx/parent_idx) -- the result
        already contains every row `path` could ever match again (the
        full ancestor chain up to path[0]'s own match, plus everything
        `path`'s last match's own subtree holds), at the same node_idx
        values, so re-applying the identical path to its own output finds
        the same nodes and changes nothing. An empty `path` returns
        `tree_df` unchanged (there is nothing to narrow by).

        Raises KeyError, naming the failing (par_name, value) pair and
        how much of `path` matched before it, if any step matches no
        node reachable from the previous one (or from the true root, for
        path's own first element).
        """
        if not path:
            return tree_df

        children, _ = self._tree_children_maps( tree_df )
        parent_of: dict[ int, int | None ] = {
            row[ 'node_idx' ]: row[ 'parent_idx' ]
            for row in tree_df.select( [ 'node_idx', 'parent_idx' ] ).unique( subset = [ 'node_idx' ] ).iter_rows( named = True )
        }

        matched: int | None = None
        frontier = children.get( None, [] )
        for step_idx, ( par_name, value ) in enumerate( path ):
            found = self._find_descendant( tree_df, children, frontier, par_name, value )
            if found is None:
                raise KeyError(
                    f"branch(): no node found for path[{step_idx}]=({par_name!r}, {value!r}) "
                    f"-- matched {list( path[ :step_idx ] )!r} so far"
                )
            matched = found
            frontier = children.get( found, [] )
        #/for step_idx, ( par_name, value ) in enumerate( path )

        ancestor_idxs: set[ int ] = set()
        n: int | None = matched
        while n is not None:
            ancestor_idxs.add( n )
            n = parent_of.get( n )
        #/while n is not None

        keep = ancestor_idxs | self._subtree_node_idxs( matched, children )
        return tree_df.filter( pl.col( 'node_idx' ).is_in( list( keep ) ) )
    #/def branch

    def _find_descendant(
        self,
        tree_df:  pl.DataFrame,
        children: dict[ int | None, list[ int ] ],
        frontier: list[ int ],
        par_name: str,
        value:    Any,
    ) -> int | None:
        """Nearest node_idx reachable from `frontier` (breadth-first,
        descending through children -- including pure junction nodes,
        which carry no par_name/value of their own to match) that has a
        content row with par_name == `par_name` and that row's own
        (whichever par_val_* is set) value == `value`. None if no such
        node is reachable at all."""
        seen:  set[ int ] = set()
        queue: list[ int ] = list( frontier )
        while queue:
            n = queue.pop( 0 )
            if n in seen:
                continue
            seen.add( n )
            rows = tree_df.filter( ( pl.col( 'node_idx' ) == n ) & ( pl.col( 'par_name' ) == par_name ) )
            for row in rows.iter_rows( named = True ):
                if self._content_value( row ) == value:
                    return n
                #/if self._content_value( row ) == value
            #/for row in rows.iter_rows( named = True )
            queue.extend( children.get( n, [] ) )
        #/while queue
        return None
    #/def _find_descendant

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
        param_dict:     ParamGroupDict,
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
#/class ParameterParser


# -- TableOp construction kwargs (__table_ops_parameters__ forest, cell.py)
#
# Every parameterized op lands in the same forest, each as its own
# op_idn-rooted ParamGroupDict handed straight to to_polars() (no wrapper
# key -- see parameter_group_to_tree() below). Individual ops are told
# apart by node_idx: configTree.py threads a running node_idx_start/
# next_node_idx counter (the same dual-return pattern to_polars()/
# to_polars_flat() use) through every [[parameters]] entry it parses, so
# each op's root-level rows (parent_idx is null) land at their own
# node_idx, globally unique across one config_tree() call's whole forest.
# Each such root group carries one par_name == 'op_idn' row identifying
# which op its sibling rows are ctor kwargs for -- kept as an explicit,
# self-describing identity marker (consistent with the original design
# sketch, nested-gathering-russell.md) rather than inferring identity from
# node_idx position alone.
#
# Caveat: this relies on node_idx actually being unique forest-wide, which
# only configTree.py's own threading guarantees. A forest assembled some
# other way without sharing a counter -- e.g. Cell.merge() concatenating
# two independently cell_from_file()-built Cells' parameter forests -- can
# reintroduce colliding node_idx across different ops, silently merging
# two ops' kwargs into one entry. Not handled here; revisit if that merge
# path is ever exercised with real [[parameters]] data.

def _leaf_value( row: dict, val_cols: list[ str ] ) -> Any:
    """The single non-null par_val_* value on a tree row, or None if it
    carries none (shouldn't happen for a 'fixed' row, but content-node rows
    in general may be value-less)."""
    for col in val_cols:
        if row[ col ] is not None:
            return row[ col ]
    return None
#/def _leaf_value


def op_ctor_kwargs_forest( parameters: pl.DataFrame ) -> dict[ str, dict[ str, Any ] ]:
    """
    Extract TableOp constructor kwargs for every op declared in the
    __table_ops_parameters__ forest (cell.py), keyed by op_idn.

    Every op's own root sits at parent_idx is null (top of its own subtree)
    -- grouping by node_idx there, rather than scanning the whole forest's
    node_idx range, is what makes this correct: a nested _or/_and's
    junction/leaf rows live under DIFFERENT node_idx values (children of
    the root's), so they'd otherwise never be seen. Each root group
    includes one par_name == 'op_idn' row identifying the op, plus its
    sibling ctor-kwarg rows. Only par_type == 'fixed' rows are supported at
    the root: a random/search sibling raises, since construction-time
    kwargs need concrete values, not search specs (that machinery is for a
    different downstream consumer, e.g. hyperparameter search, not this
    one). Any non-root row in the node (parent_idx not null -- i.e. a
    `_or`/`_and` nested underneath) also raises, since there is no single
    unambiguous value to pick for a ctor kwarg in that case. A node_idx
    carrying more than one 'op_idn' leaf also raises -- this can only mean
    node_idx collided between two ops (e.g. built without threading
    node_idx_start), which would otherwise silently merge their kwargs
    into one entry.

    Returns {} for an empty forest. An op_idn with no declared parameter
    node simply has no entry, same as if the whole forest were empty;
    callers rely on the op's own constructor defaults (or a TypeError for a
    missing required kwarg) in that case.
    """
    if parameters.is_empty():
        return {}

    val_cols = [ c for c in parameters.columns if c.startswith( 'par_val_' ) ]
    roots_by_group = parameters.filter( pl.col( 'parent_idx' ).is_null() )
    nodes_with_children = set(
        parameters.filter( pl.col( 'parent_idx' ).is_not_null() )[ 'parent_idx' ].to_list()
    )
    result: dict[ str, dict[ str, Any ] ] = {}
    for node_idx in roots_by_group[ 'node_idx' ].drop_nulls().unique().to_list():
        roots = roots_by_group.filter( pl.col( 'node_idx' ) == node_idx ).to_dicts()
        id_rows = [ r for r in roots if r[ 'par_name' ] == 'op_idn' ]
        if not id_rows:
            continue
        if len( id_rows ) > 1:
            op_idns = [ _leaf_value( r, val_cols ) for r in id_rows ]
            raise ValueError(
                f"node_idx={ node_idx!r } carries multiple op_idn leaves "
                f"{ op_idns!r } -- node_idx must be unique per op (thread "
                f"node_idx_start/next_node_idx across calls that share it)"
            )
        op_idn = _leaf_value( id_rows[ 0 ], val_cols )
        if node_idx in nodes_with_children:
            raise ValueError(
                f"parameter node for op_idn={ op_idn!r } has nested "
                f"_or/_and structure -- not supported for TableOp "
                f"construction kwargs, which need one concrete value per "
                f"parameter"
            )
        kwargs: dict[ str, Any ] = {}
        for row in roots:
            if row[ 'par_name' ] in ( 'op_idn', None ):
                continue
            if row[ 'par_type' ] != 'fixed':
                raise ValueError(
                    f"parameter { row[ 'par_name' ]!r } for op_idn={ op_idn!r } has "
                    f"par_type={ row[ 'par_type' ]!r } -- only fixed values are "
                    f"supported for TableOp construction kwargs"
                )
            kwargs[ row[ 'par_name' ] ] = _leaf_value( row, val_cols )
        result[ op_idn ] = kwargs
    return result
#/def op_ctor_kwargs_forest


# -- Building a parameter group's tree from a raw dict (TOML/JSON, no
# advance dtype registration -- see configTree.py's [[parameters]] support)

def _dtype_for_value( value: Any ) -> pl.DataType:
    """Infer a registrable dtype from a literal Python value's runtime type
    -- same dispatch ParameterParser.col_for_value() uses, but returning the
    dtype (for .register()) rather than the (column, value) pair."""
    if isinstance( value, bool ):
        return pl.Boolean
    if isinstance( value, int ):
        return pl.Int64
    if isinstance( value, float ):
        return pl.Float64
    if isinstance( value, str ):
        return pl.Utf8
    if isinstance( value, list ) and value:
        return pl.List( _dtype_for_value( value[ 0 ] ) )
    return pl.Utf8
#/def _dtype_for_value


def _register_param_group_dtypes( parser: ParameterParser, group: dict ) -> None:
    """
    Auto-register every par_name this group needs a dtype for, inferred
    from the literal values present -- parser.to_polars() requires
    pre-registration for plain fixed values (via col_for_name) and for
    every ValueDict shape, since _valueDict_to_tree's own column choice
    (_COL_INT/_COL_FLOAT/etc.) only lands in the actual output if that
    dtype was registered somewhere first -- ParameterParser.schema's
    val_cols come from the registered-dtype set, not from which column a
    given row happens to target, so an appended value whose column isn't
    in schema is silently dropped by TableBuilder.append(allow_missing=
    True) (loops over self.columns, never sees unregistered keys).
    configTree.py has no advance knowledge of what dtype any given
    TOML/JSON-sourced parameter should be, hence inferring here rather
    than requiring a separate .register() pass. Recurses into _or/_and
    alternatives (same group). Ignores '_comment' (module docstring's
    RESERVED KEY section).

    Mirrors _valueDict_to_tree's own shape dispatch (values_search ->
    values_random -> logit_step -> log_step -> step -> distribution),
    registering whatever dtype that branch will actually write: the
    dtype of values[0] for values_search/values_random (list-of-
    candidates columns are unconditionally present regardless of
    registration, but registering the element dtype keeps coercion
    correct); Float64 for logit_step (always float, by its own 0<start<1
    domain constraint); the dtype of 'start' for log_step/step (both
    pick Int64 vs Float64 by isinstance(start, int), consistently across
    start/step/min/max); Int64 for a 'uniform_int'/'log_uniform_int'
    distribution, Float64 for any other distribution. An unrecognized
    shape (none of the above) is left unregistered, same as before --
    _valueDict_to_tree raises ValueError for it anyway once actually
    parsed, so there's nothing useful to register.
    """
    for key, value in group.items():
        if key == _IGNORED_KEY:
            continue
        if key in ( '_or', '_and' ):
            for alt in value:
                _register_param_group_dtypes( parser, alt )
            continue
        if isinstance( value, dict ):
            d = value
            if 'values_search' in d or 'values_random' in d:
                values = d.get( 'values_search', d.get( 'values_random' ) )
                if values:
                    parser.register( key, _dtype_for_value( values[ 0 ] ) )
            elif 'logit_step' in d:
                parser.register( key, pl.Float64 )
            elif 'log_step' in d or 'step' in d:
                parser.register( key, _dtype_for_value( d[ 'start' ] ) )
            elif 'distribution' in d:
                if d[ 'distribution' ] in ( 'uniform_int', 'log_uniform_int' ):
                    parser.register( key, pl.Int64 )
                else:
                    parser.register( key, pl.Float64 )
            continue
        parser.register( key, _dtype_for_value( value ) )
    #/for key, value in group.items()
#/def _register_param_group_dtypes


def parameter_group_to_tree(
    op_idn: str, group: dict, node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
    """
    Build one op's row-group for the __table_ops_parameters__ forest from a
    raw ParamGroupDict -- the same par_name -> scalar-or-valueDict (+ at
    most one of _or/_and) shape a hand-written pre-ParameterParser JSON/TOML
    spec already uses (parameterParse.py's module docstring). The 'op_idn'
    identity leaf op_ctor_kwargs_forest() reads back out is added here
    automatically, as an ordinary sibling par_name alongside the op's own
    kwargs -- no wrapper key needed (to_polars() has no concept of a
    top-level "group" any given dict lives under) -- ops are told apart by
    node_idx (see the "TableOp construction kwargs" comment above
    op_ctor_kwargs_forest) -- callers (e.g. configTree.py) supply just the
    op's own kwargs, keyed by name.

    Dtypes are inferred from the literal values present (see
    _register_param_group_dtypes) rather than requiring a separate
    .register() pass -- a TOML/JSON parameter file has no natural place to
    declare its own dtypes, and the eventual TableOp ctor kwarg types
    aren't known at parse time anyway (op_classes isn't available until
    Cell construction).
    """
    full_group = { 'op_idn': op_idn, **group }
    parser = ParameterParser()
    parser.register( 'op_idn', pl.Utf8 )
    _register_param_group_dtypes( parser, full_group )
    return parser.to_polars( full_group, node_idx_start = node_idx_start )
#/def parameter_group_to_tree
