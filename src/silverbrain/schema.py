#
#//  schema.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/26/26.
#//

from typing import Sequence

import polars as pl

from .polarsDataTypeStrings import dtype_to_str

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
    'fp':               pl.Utf8,
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
    # backend storage
    'backend_id':        pl.Utf8,
    'backend_table':     pl.Utf8,
    'db_path':           pl.Utf8,
    'table_name':        pl.Utf8,
    'if_exists':         pl.Utf8,
    'connection_string': pl.Utf8,
    'schema_name':       pl.Utf8,
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
    # kind       — 'op' (leaf TableOp) or 'cell' (nested Cell) -- both
    #              resolved the same way, via self.tableOps[node_idn]
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
    '__backend_index__': cannonical_schema_for_keys((
        'backend_id', 'backend_table',
    )),
    '__parquet_backend__': cannonical_schema_for_keys((
        'backend_id', 'fp', 'if_exists',
    )),
    '__sqlite_backend__': cannonical_schema_for_keys((
        'backend_id', 'db_path', 'table_name', 'if_exists',
    )),
    '__postgres_backend__': cannonical_schema_for_keys((
        'backend_id', 'connection_string', 'table_name', 'schema_name', 'if_exists',
    )),
    '__nodes__': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
    )),

    # -- TOML ↔ DataFrame schemas

    # configTree.py: a parsed, not-yet-constructed file-reference tree.
    # Same shape as __nodes__ plus 'path'/'cell_source'/'cell_target' --
    # only meaningful on kind='cell' rows: 'path' is which file this node
    # came from (informational -- construction no longer needs to reopen
    # it); 'cell_source'/'cell_target' are that cell's own external binding
    # ([cell].source/[cell].target in a TOML source), carried in the tree
    # alongside the row's own (parent-assigned) source/target so
    # construction never needs to open a file to learn either.
    'config_tree': cannonical_schema_for_keys((
        'node_idn', 'parent_idn', 'kind', 'source', 'target',
        'path', 'cell_source', 'cell_target',
    )),
}
