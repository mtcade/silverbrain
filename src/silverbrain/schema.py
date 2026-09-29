#
#//  schema.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/26/26.
#//

from collections import UserDict
from dataclasses import dataclass, field
from typing import Literal, Self, Sequence
from uuid import uuid4

import polars as pl

from . import bdd
from .polarsDataTypeStrings import dtype_to_str


def col_for_dtype( dtype: pl.DataType ) -> str:
    """
    Derive a par_val_* column name from a polars DataType -- the single
    source of truth for this naming convention (parameterParse.py imports
    this rather than redefining it; silverflow.tableOps does too, via
    silverflow.schema's re-export).

    pl.Int64            → 'par_val_int64'
    pl.Float64          → 'par_val_float64'
    pl.Utf8             → 'par_val_string'
    pl.List(pl.Int64)   → 'par_val_list_int64'
    pl.List(pl.Float64) → 'par_val_list_float64'
    pl.List(pl.Utf8)    → 'par_val_list_string'
    """
    s = dtype_to_str( dtype ).lower().replace( '(', '_' ).replace( ')', '' )
    return 'par_val_' + s
#/def col_for_dtype


cannonical_schema: dict[ str, pl.DataType ] = {
    'op_idn':           pl.Utf8,
    'source':           pl.List( pl.Utf8 ),
    'target':           pl.List( pl.Utf8 ),
    # a Cell's own external binding (Cell.source/Cell.target) -- distinct
    # from a __nodes__/config_tree row's source/target, which are assigned
    # by whoever registers that node (see squishy-sleeping-snowflake.md).
    'cell_source':      pl.List( pl.Utf8 ),
    'cell_target':      pl.List( pl.Utf8 ),
    # __tables_schema__ (generic domain-table schema registry)
    'column_names':     pl.List( pl.Utf8 ),
    'column_types':     pl.List( pl.Utf8 ),
    # paths
    'key':              pl.Utf8,
    'isfile':           pl.Boolean,
    'isdir':            pl.Boolean,
    # tables schema
    'table_idn':        pl.Utf8,
    # effects df
    'effect_index':      pl.UInt32,
    'type':              pl.Utf8,
    'path_input':        pl.Int32,
    'path_column':       pl.Utf8,
    'output':            pl.Int32,
    'format':            pl.Utf8,
    'backend_input':     pl.Int32,
    'backend_id_column': pl.Utf8,
    # parameter spec rows (parameterParse.py's ParameterParser) -- par_idx
    # is the flat/cross-producted view's row-group id; node_idx/parent_idx
    # are the tree view's permanent-id/parent-link pair (both views share
    # everything else: par_type/par_name/par_feature + the par_val_* value
    # columns, added below via col_for_dtype). No par_group column -- which
    # rows belong together is fully captured by node_idx/parent_idx; a
    # consumer that needs to pick specific rows out of a mixed forest does
    # so via par_name (domain-specific, assumed unique within whatever
    # scope it's read back in), not a separate categorical label.
    'par_idx':          pl.UInt32,
    'par_type':         pl.Utf8,
    'par_name':         pl.Utf8,
    'par_feature':      pl.Utf8,
    'node_idx':         pl.UInt32,
    'parent_idx':       pl.UInt32,
    # _trigger (sunder.py): run_id is fresh per invocation (uuid7,
    # matching cell.py's own corr_id/uuid7 convention); triggered_at is
    # that run's wall-clock start time.
    'run_id':           pl.Utf8,
    'triggered_at':     pl.Datetime( time_zone = 'UTC' ),
} | {
    # __nodes__ serialization -- A Cell's
    # own local table only ever has parent_idn = null (every row is a
    # direct child of this Cell); the same shape is reused by configTree.py
    # for a real multi-level file-reference tree, parsed before any live
    # Cell exists.
    #
    # node_idn   — the name this child is registered under in self.tableOps;
    #              doubles as the lookup key (self.tableOps[node_idn])
    # parent_idn — parent node_idn in the same table; null = direct child
    #              of the owning Cell (always null in a live Cell's own
    #              table; may chain multiple levels in a configTree parse)
    # kind       — 'op' (leaf TableOp) or 'cell' (nested Cell) in a live
    #              Cell's own __nodes__ table -- both resolved the same way,
    #              via self.tableOps[node_idn]. configTree.py's parse-time
    #              tree additionally allows 'variable' (see
    #              variable_columns below); a variable builds into a
    #              PlaceholderOp, so it still shows up as kind='op' once a
    #              live Cell reads it back via register_node.
    # source     — names this node's positional dfs inputs are read from,
    #              assigned by whoever registers it (its parent Cell); need
    #              not be unique -- a name may feed more than one node
    #              (fan-out, each consumer keeping its own read position)
    #              and, since the wiring graph isn't required to be a DAG,
    #              more than one node may target the same name across
    #              different rounds (loops) -- each write just appends
    #              another token to that name's log
    # target     — names this node's positional outputs are written under,
    #              likewise parent-assigned; a null/None value for a
    #              target is an ordinary, logged token like any other
    #              (branching/loop termination -- see Token's/
    #              _WiringEngine's docstrings). A node fires only once
    #              every one of its declared source names has an unread,
    #              NON-NULL token -- a strict AND-join over presence and
    #              non-nullity, the only firing rule this engine has (no
    #              join modes, no discard-on-null opt-out).
    'node_idn':        pl.Utf8,
    'parent_idn':      pl.Utf8,
    'kind':            pl.Utf8,
    # configTree.py: which op_classes key to construct a kind='op' row with
    # -- distinct from node_idn (that row's own wiring/dispatch identity),
    # so the same class can be built by multiple anonymous nodes (node_idn
    # omitted, auto-generated) wired only by source/target. Defaults to the
    # row's own node_idn when omitted (today's behavior). Null on every
    # other kind -- kind='cell'/'variable' resolve their own "type" via
    # toml/cell_idn/columns instead.
    'op_type':      pl.Utf8,
    # configTree.py: file-reference metadata for kind='cell' rows only, not
    # part of a live Cell's own __nodes__ table.
    'path':         pl.Utf8,
    # table_index (below): identifies where one named table lives, across
    # any backend ReadWriteOps.py's ReadOp/WriteOp/ReadWriteOp know how to
    # resolve. 'kind' selects which of these remaining, mostly-nullable
    # columns are meaningful -- 'local' only uses 'path'; 'sqlite' uses
    # 'db_path'/'table_name'; 'postgres' uses 'connection_string'/
    # 'table_name'/'schema_name'; 'json_ptree' (silverflow's
    # ReadParametersOp only) uses 'path'. Moved here from silverflow's own
    # cannonical_schema once table_idn resolution became core
    # infrastructure (ReadWriteOps.py) rather than a silverflow-specific
    # convention.
    'db_path':           pl.Utf8,
    'table_name':        pl.Utf8,
    'connection_string': pl.Utf8,
    'schema_name':       pl.Utf8,
    # configTree.py: kind='variable' rows' declared interface -- a
    # bdd.Expr serialized via exprParse.expr_to_text (may reference
    # ColVar; every Atom always carries an explicit dtype in this stored
    # form, even though the TOML/JSON source may have omitted one for
    # inference -- see exprParse.py's module docstring), null on every
    # other kind. Used to construct a PlaceholderOp(expr=...) at build
    # time, standing in until a real TableOp/Cell is swapped into
    # self.tableOps[node_idn].
    'variable_columns': pl.Utf8,
    # configTree.py: variables.resources/variables.actors declaration
    # bookkeeping (see its module docstring). 'abs_path' is the dotted
    # node_idn chain from a config_tree() call's own root down to some
    # node -- both a config_tree row's own identity (added to every row,
    # any kind) and a variables.* declaration's resolved target. Two
    # different things share this one column name/dtype because they're
    # literally the same value once resolution matches a declaration to
    # the row it targets. 'declarer_depth' is how many kind='cell' hops
    # below config_tree()'s own root the DECLARING node sits at (root=0) --
    # resolve_resource_declarations()/resolve_actor_declarations() pick the
    # smallest depth per target (root-wins). 'declaring_path' is the
    # winning declaration's own abs_path, kept as provenance in a resolved
    # actor binding / __actor_bindings__ row. 'kwargs_json' is an
    # variables.actors entry's ctor kwargs, JSON-encoded rather than routed
    # through the __table_ops_parameters__ forest (that forest's op_idn
    # keying assumes local-file uniqueness, which a path-addressed
    # declaration from an arbitrary ancestor doesn't have).
    'abs_path':         pl.Utf8,
    'declarer_depth':   pl.Int64,
    'declaring_path':   pl.Utf8,
    'kwargs_json':      pl.Utf8,
    # configTree.resolve_deployment(): a caller-supplied deployment row's
    # own placement tag ('local'/'ray'/...) and its strategy-specific ctor
    # kwargs, JSON-encoded the same way variables.actors' own 'kwargs_json'
    # is (build_tableOps() decodes it right before use).
    'strategy':         pl.Utf8,
    'options_json':     pl.Utf8,
}

def cannonical_schema_for_keys(
    keys: Sequence[ str ],
) -> dict[ str, pl.DataType ]:
    return { key: cannonical_schema[key] for key in keys }
#/cannonical_schema_for_keys

# The standard par_val_* value columns (parameterParse.py's
# ParameterParser.schema/schema_flat always carry these three
# values_search/values_random candidate-list columns unconditionally,
# plus whichever scalar par_val_* columns its own registered dtypes
# need -- these four scalar ones cover every dtype PrimitiveValue/bool
# actually uses). Added to cannonical_schema via col_for_dtype so the
# column-naming convention has exactly one definition.
cannonical_schema |= {
    col_for_dtype( pl.Utf8 ):               pl.Utf8,
    col_for_dtype( pl.Int64 ):               pl.Int64,
    col_for_dtype( pl.Float64 ):             pl.Float64,
    col_for_dtype( pl.Boolean ):             pl.Boolean,
    col_for_dtype( pl.List( pl.Utf8 ) ):     pl.List( pl.Utf8 ),
    col_for_dtype( pl.List( pl.Int64 ) ):    pl.List( pl.Int64 ),
    col_for_dtype( pl.List( pl.Float64 ) ):  pl.List( pl.Float64 ),
}

# parameterParse.py's two row shapes -- both single source of truth here
# now, parameterParse.py imports rather than redefines them.
_IDENTIFIER_COLS: dict[ str, pl.DataType ] = cannonical_schema_for_keys( (
    'par_type', 'par_idx', 'par_name', 'par_feature',
) )
_TREE_IDENTIFIER_COLS: dict[ str, pl.DataType ] = cannonical_schema_for_keys( (
    'par_type', 'par_name', 'par_feature', 'node_idx', 'parent_idx',
) )
_CANDIDATE_LIST_COLS: dict[ str, pl.DataType ] = cannonical_schema_for_keys( (
    col_for_dtype( pl.List( pl.Utf8 ) ), col_for_dtype( pl.List( pl.Int64 ) ), col_for_dtype( pl.List( pl.Float64 ) ),
) )

def table_schemas_to_df(
    ts: dict[ str, dict[ str, pl.DataType ] ],
) -> pl.DataFrame:
    """Serialize a table_schemas dict to a DataFrame with schema tables_schema."""
    return pl.DataFrame(
        {
            'table_idn':    list( ts.keys() ),
            'column_names': [ list( schema.keys() ) for schema in ts.values() ],
            'column_types': [
                [ dtype_to_str( t ) for t in schema.values() ]
                for schema in ts.values()
            ],
        },
        schema = table_schemas['__tables_schema__'],
    )
#/table_schemas_to_df

table_schemas: dict[ str, dict[ str, pl.DataType ] ] = {
    # InputSchema/OutputSchema declare a bdd.Expr, not a flat column list --
    # __table_op_schema__ is exactly bdd's (op_idn, direction, index)-rooted
    # expression forest (bdd._SIGNATURE_TREE_COLS), not a cannonical_schema
    # subset. extra/passthrough_from no longer exist as separate columns --
    # see InputSchema/OutputSchema below for how those are recovered from
    # ColVar presence/sharing instead.
    '__table_op_schema__': bdd._SIGNATURE_TREE_COLS,
    '__table_op_effects__':  cannonical_schema_for_keys((
        'op_idn', 'effect_index', 'type',
        'path_input', 'path_column', 'output',
        'format', 'backend_input', 'backend_id_column',
    )),
    '__tables_schema__': cannonical_schema_for_keys((
        'table_idn', 'column_names', 'column_types',
    )),
    # See cannonical_schema's own 'db_path'/'table_name'/'connection_string'/
    # 'schema_name' comment above -- ReadWriteOps.py's TableIndexEntry
    # mirrors this exact column set with Python-native types.
    'table_index': cannonical_schema_for_keys((
        'table_idn', 'kind', 'path', 'db_path', 'table_name', 'connection_string', 'schema_name',
    )),
    '__nodes__': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
    )),

    # parameterParse.py's ParameterParser().schema, structural/identifier-
    # only (no scalar par_val_* columns -- those depend on whatever
    # dtypes get registered per settings file) -- e.g.
    # silverflow.tableOps.EmptyTableOp's table_schema="parameter_tree",
    # to bootstrap a loop needing an initial, always-valid 0-row token
    # shaped like a real parameter tree.
    'parameter_tree': _TREE_IDENTIFIER_COLS | _CANDIDATE_LIST_COLS,

    # -- TOML ↔ DataFrame schemas

    # configTree.py: a parsed, not-yet-constructed file-reference tree.
    # Same shape as __nodes__ plus 'path'/'cell_source'/'cell_target'/
    # 'variable_columns'. 'path' and 'cell_source'/'cell_target' are only
    # meaningful on kind='cell' rows: 'path' is which file this node came
    # from (informational -- construction no longer needs to reopen it);
    # 'cell_source'/'cell_target' are that cell's own external binding
    # ([cell].source/[cell].target in a TOML source), carried in the tree
    # alongside the row's own (parent-assigned) source/target so
    # construction never needs to open a file to learn either.
    # 'variable_columns' is only meaningful on kind='variable' rows --
    # see cannonical_schema above.
    'config_tree': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
        'path', 'abs_path', 'cell_source', 'cell_target', 'variable_columns', 'op_type',
    )),

    # cell.py: one row per variable node_idn that resolved to a real
    # variables.actors binding (see configTree.py's module docstring) --
    # append-only in spirit (a future live hot-swap just appends another
    # row and re-derives "current binding" via
    # .unique(subset=['node_idn'], keep='last'), same idiom
    # resolve_actor_declarations already uses). A variable with no
    # binding at all gets no row (stays a PlaceholderOp, same as today).
    '__actor_bindings__': cannonical_schema_for_keys((
        'node_idn', 'kind', 'op_type', 'declaring_path',
    )),

    # _trigger: the sunder table (see sunder.py) a Cell auto-supplies to
    # any node that declares it as a source without being seeded it
    # externally -- see cell.py's _WiringEngine.
    '_trigger': cannonical_schema_for_keys((
        'run_id', 'triggered_at',
    )),
}

# -- TableOpSchema

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
    """
    expr declares this input's set of columns -- literal Atoms, ColVars,
    or any combination via Union/Intersect/Difference/Cond. The old
    'forbidden'/'open'/'transparent' Extra enum is gone: those three
    behaviors are now consequences of whether expr references a ColVar at
    all (no ColVar == forbidden, extra columns disallowed) and, for
    'transparent', whether that same ColVar is ALSO referenced by one of
    the same TableOp's own outputs (see passthrough_inputs() below) --
    nothing to declare separately. See from_columns() for the common
    fully-literal case, expressed without hand-writing bdd.Expr directly.
    """
    expr: bdd.Expr

    def to_dict( self: Self ) -> dict:
        return { 'expr': self.expr.to_dict() }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls( expr = bdd.expr_from_dict( d['expr'] ) )

    @classmethod
    def from_columns(
        cls,
        columns: dict[ str, pl.DataType ],
        extra: "Literal['forbidden', 'open'] | bdd.ColVar" = 'forbidden',
        ) -> Self:
        """
        Build an InputSchema from a plain column dict, without hand-writing
        bdd.Expr -- the ergonomic bridge for the common fully-literal case.
        extra='forbidden' (default): no ColVar, nothing but `columns`
        allowed. extra='open': an anonymous ColVar is added (extra columns
        allowed, dropped -- nothing references this ColVar elsewhere, so
        it can never become a passthrough). extra=<a bdd.ColVar instance>:
        that ColVar is added instead of an anonymous one -- pass the same
        ColVar into the corresponding OutputSchema.from_columns(...,
        passthrough=...) call to declare it flows through.
        """
        atoms = [ bdd.Atom( name, dtype ) for name, dtype in columns.items() ]
        if extra == 'forbidden':
            return cls( expr = bdd.Union( *atoms ) )
        if extra == 'open':
            return cls( expr = bdd.Union( *atoms, bdd.ColVar( f'_extra_{uuid4().hex}' ) ) )
        if isinstance( extra, bdd.ColVar ):
            return cls( expr = bdd.Union( *atoms, extra ) )
        raise ValueError( f"extra must be 'forbidden', 'open', or a ColVar instance, got {extra!r}" )
#/class InputSchema

@dataclass
class OutputSchema():
    """
    expr declares this output's set of columns, same as InputSchema.expr.
    The old passthrough_from: list[int] is gone -- "which input's extras
    flow to this output" is now derived by checking which ColVar names
    this output's expr shares with each input's expr (passthrough_inputs()
    below), not declared as an index list.
    """
    expr: bdd.Expr

    def to_dict( self: Self ) -> dict:
        return { 'expr': self.expr.to_dict() }

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls( expr = bdd.expr_from_dict( d['expr'] ) )

    @classmethod
    def from_columns(
        cls,
        columns: dict[ str, pl.DataType ],
        passthrough: 'bdd.ColVar | None' = None,
        ) -> Self:
        """
        Build an OutputSchema from a plain column dict. Pass the same
        ColVar instance given to some InputSchema.from_columns(...,
        extra=that_colvar) call to declare that input's extras flow
        through to this output.
        """
        atoms = [ bdd.Atom( name, dtype ) for name, dtype in columns.items() ]
        if passthrough is not None:
            atoms = [ *atoms, passthrough ]
        return cls( expr = bdd.Union( *atoms ) )
#/class OutputSchema

@dataclass
class TableOpSchema():
    inputs: list[ InputSchema ]
    outputs: list[ OutputSchema ]
    effects: list[ Effect ] = field( default_factory = list )

    def to_polars_schema( self: Self, op_idn: str, node_idx_start: int = 0 ) -> tuple[ pl.DataFrame, int ]:
        signatures = {
            ( op_idn, 'input', i ): inp.expr for i, inp in enumerate( self.inputs )
        } | {
            ( op_idn, 'output', i ): out.expr for i, out in enumerate( self.outputs )
        }
        return bdd.signatures_to_polars( signatures, node_idx_start = node_idx_start )

    def to_polars_effects( self: Self, op_idn: str ) -> pl.DataFrame:
        frames = [ eff.to_polars( op_idn, i ) for i, eff in enumerate( self.effects ) ]
        return (
            pl.concat( frames, how = 'vertical' ) if frames
            else pl.DataFrame( schema = table_schemas['__table_op_effects__'] )
        )

    def to_polars( self: Self, op_idn: str, node_idx_start: int = 0 ) -> tuple[ pl.DataFrame, pl.DataFrame, int ]:
        schema_df, next_idx = self.to_polars_schema( op_idn, node_idx_start )
        return ( schema_df, self.to_polars_effects( op_idn ), next_idx )

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

def passthrough_inputs( schema: TableOpSchema, output_index: int ) -> list[ int ]:
    """
    Which input indices' extra columns flow to schema.outputs[output_index]
    -- an input and the given output "share a passthrough" iff their
    expressions reference at least one common ColVar name. Replaces the
    old OutputSchema.passthrough_from: list[int] -- computed on demand,
    never stored, so it can't drift from the expressions themselves.
    """
    output_colvars = bdd.colvar_names( schema.outputs[ output_index ].expr )
    return [
        i for i, inp in enumerate( schema.inputs )
        if bdd.colvar_names( inp.expr ) & output_colvars
    ]
#/def passthrough_inputs

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
        signatures = {}
        for op_idn, op in self.data.items():
            signatures |= { ( op_idn, 'input', i ): inp.expr for i, inp in enumerate( op.inputs ) }
            signatures |= { ( op_idn, 'output', i ): out.expr for i, out in enumerate( op.outputs ) }
        df, _ = bdd.signatures_to_polars( signatures )
        return df

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
