#
#//  schema.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/26/26.
#//

from collections import UserDict
from dataclasses import dataclass, field
from typing import Literal, Self, Sequence

import polars as pl

from .polarsDataTypeStrings import dtype_to_str, str_to_dtype

cannonical_schema: dict[ str, pl.DataType ] = {
    'op_idn':           pl.Utf8,
    'source':           pl.List( pl.Utf8 ),
    'target':           pl.List( pl.Utf8 ),
    # a Cell's own external binding (Cell.source/Cell.target) -- distinct
    # from a __nodes__/config_tree row's source/target, which are assigned
    # by whoever registers that node (see squishy-sleeping-snowflake.md).
    'cell_source':      pl.List( pl.Utf8 ),
    'cell_target':      pl.List( pl.Utf8 ),
    # schemas df
    'direction':        pl.Utf8,
    'index':            pl.UInt32,
    'column_names':     pl.List( pl.Utf8 ),
    'column_types':     pl.List( pl.Utf8 ),
    'extra':            pl.Utf8,
    'passthrough_from': pl.List( pl.Int32 ),
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
    # parameter spec rows
    'par_idx':          pl.UInt32,
} | {
    # __nodes__ serialization -- see squishy-sleeping-snowflake.md. A Cell's
    # own local table only ever has parent_idn = null (every row is a
    # direct child of this Cell); the same shape is reused by configTree.py
    # for a real multi-level file-reference tree, parsed before any live
    # Cell exists.
    # node_idn   — the name this child is registered under in self.tableOps;
    #              doubles as the lookup key (self.tableOps[node_idn])
    # parent_idn — parent node_idn in the same table; null = direct child
    #              of the owning Cell (always null in a live Cell's own
    #              table; may chain multiple levels in a configTree parse)
    # kind       — 'op' (leaf TableOp) or 'cell' (nested Cell) in a live
    #              Cell's own __nodes__ table -- both resolved the same way,
    #              via self.tableOps[node_idn]. configTree.py's parse-time
    #              tree additionally allows 'placeholder' (see
    #              placeholder_columns below); a placeholder builds into a
    #              PlaceholderOp, so it still shows up as kind='op' once a
    #              live Cell reads it back via register_node.
    # source     — names this node's positional dfs inputs are read from,
    #              assigned by whoever registers it (its parent Cell); need
    #              not be unique -- a name may feed more than one node
    #              (fan-out) and, since the wiring graph isn't required to
    #              be a DAG, more than one node may target the same name
    #              across different rounds (loops)
    # target     — names this node's positional outputs are written under,
    #              likewise parent-assigned; a null/None value for a target
    #              is a legitimate "no data this round" signal (loop
    #              termination), not an error
    'node_idn':     pl.Utf8,
    'parent_idn':   pl.Utf8,
    'kind':         pl.Utf8,
    # configTree.py: file-reference metadata for kind='cell' rows only, not
    # part of a live Cell's own __nodes__ table.
    'path':         pl.Utf8,
    # configTree.py: kind='placeholder' rows' declared interface -- a
    # JSON-encoded {col_name: dtype_str} object (dtype_str via
    # polarsDataTypeStrings.dtype_to_str), null on every other kind. Used to
    # construct a PlaceholderOp(columns=...) at build time, standing in
    # until a real TableOp/Cell is swapped into self.tableOps[node_idn].
    'placeholder_columns': pl.Utf8,
}

def cannonical_schema_for_keys(
    keys: Sequence[ str ],
) -> dict[ str, pl.DataType ]:
    return { key: cannonical_schema[key] for key in keys }
#/cannonical_schema_for_keys

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
    '__table_op_schema__':  cannonical_schema_for_keys((
        'op_idn', 'direction', 'index',
        'column_names', 'column_types',
        'extra', 'passthrough_from',
    )),
    '__table_op_effects__':  cannonical_schema_for_keys((
        'op_idn', 'effect_index', 'type',
        'path_input', 'path_column', 'output',
        'format', 'backend_input', 'backend_id_column',
    )),
    '__tables_schema__': cannonical_schema_for_keys((
        'table_idn', 'column_names', 'column_types',
    )),
    # Backend storage table schemas (__backend_index__, __parquet_backend__,
    # __sqlite_backend__, __postgres_backend__) live in silverflow.schema
    # alongside the backend-specific TableOps that read/write them.
    '__nodes__': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
    )),

    # -- TOML ↔ DataFrame schemas

    # configTree.py: a parsed, not-yet-constructed file-reference tree.
    # Same shape as __nodes__ plus 'path'/'cell_source'/'cell_target'/
    # 'placeholder_columns'. 'path' and 'cell_source'/'cell_target' are only
    # meaningful on kind='cell' rows: 'path' is which file this node came
    # from (informational -- construction no longer needs to reopen it);
    # 'cell_source'/'cell_target' are that cell's own external binding
    # ([cell].source/[cell].target in a TOML source), carried in the tree
    # alongside the row's own (parent-assigned) source/target so
    # construction never needs to open a file to learn either.
    # 'placeholder_columns' is only meaningful on kind='placeholder' rows --
    # see cannonical_schema above.
    'config_tree': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
        'path', 'cell_source', 'cell_target', 'placeholder_columns',
    )),
}

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
