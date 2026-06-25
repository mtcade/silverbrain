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
    # op bindings
    'root_op_idn':      pl.Utf8,            # op_idn of the root process node (parent_id = null) that owns this op
    'op_idn':           pl.Utf8,
    'source':           pl.List( pl.Utf8 ),
    'target':           pl.List( pl.Utf8 ),
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
    # routes / inter-web transport
    'web_idn':          pl.Utf8,
    'address':          pl.Utf8,
    'topic':            pl.Utf8,
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
    # table_init / process_init serialization
    'written_by':       pl.List( pl.Utf8 ),
    'factory_idn':      pl.Utf8,
    'always_run':       pl.Boolean,
    # routing — cross-node data edges
    'source_node_idn':  pl.Utf8,
    'trigger_op_idn':   pl.Utf8,
    'source_table_idn': pl.Utf8,
    'target_node_idn':  pl.Utf8,
    'target_table_idn': pl.Utf8,
} | {
    # table_processes serialization
    # op_idn     — primary key; unique across all registered processes
    # type       — dispatch tag: TableProcessRef | TableCheckRef | TableProcessSequence |
    #                TableProcessWhile | TableProcessCount | TableProcessBranch
    # source     — ordered input table keys read from local tables dict before this node runs
    # target     — ordered output table keys written to local tables dict after this node runs
    # term_idns  — for Ref/Check: [op_idn of the tableOp to call]; for Sequence: op_idns of child
    #              processes in order; for While/Count: [op_idn of the inner process];
    #              for Branch: [op_idn of the fallback process], or [] if no fallback
    # condition  — (While) op_idn of the condition TableCheckRef
    # count      — (While) max iteration cap; (Count) exact iteration count
    # ifs        — (Branch) op_idns of condition check processes, parallel with thens
    # thens      — (Branch) op_idns of branch-arm processes
    #'source':       pl.List( pl.Utf8 ), from op bindings
    #'target':       pl.List( pl.Utf8 ), from op bindings
    'term_idns':    pl.List( pl.Utf8 ),
    'condition':    pl.Utf8,
    'count':        pl.Int64,
    'ifs':          pl.List( pl.Utf8 ),
    'thens':        pl.List( pl.Utf8 ),
    # CompositeNodeRef: child node to dispatch to (null for all other types)
    'node_idn':     pl.Utf8,
} | {
    # network / distributed routing
    # mode              — 'terminal' (reply to origin) or 'relay' (forward to next hop)
    # origin_identity   — raw ZMQ ROUTER routing frame bytes for the request origin
    # received_at       — epoch milliseconds when the request arrived
    # request_id        — correlation key linking a relay request to its origin
    # status            — connection lifecycle: 'connecting' | 'connected' | 'disconnected'
    '__system_op__':    pl.Utf8,
    'mode':             pl.Utf8,
    'origin_identity':  pl.Binary,
    'received_at':      pl.Int64,
    'request_id':       pl.Utf8,
    'status':           pl.Utf8,
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
    'op_bindings': cannonical_schema_for_keys((
        'root_op_idn', 'op_idn', 'source',
        'target',
    )),
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
    'paths': cannonical_schema_for_keys((
        'key', 'fp', 'isfile', 'isdir',
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
    'routes': cannonical_schema_for_keys((
        'web_idn', 'op_idn', 'address', 'topic',
    )),
    '__table_init__': cannonical_schema_for_keys((
        'table_idn', 'source', 'op_idn', 'written_by', 'always_run',
    )),
    '__process_init__': cannonical_schema_for_keys((
        'op_idn', 'source', 'factory_idn', 'always_run',
    )),
    '__table_processes__': cannonical_schema_for_keys((
        'op_idn', 'type',
        'source', 'target',
        'term_idns', 'condition', 'count',
        'ifs', 'thens',
        'node_idn',
    )),
    '__routing__': cannonical_schema_for_keys((
        'source_node_idn', 'trigger_op_idn', 'source_table_idn',
        'target_node_idn', 'target_table_idn',
    )),

    # -- Network system tables (present in every network-capable Cell)

    # Current routing configuration: for each pipeline (op_idn), how to handle
    # completed results — reply to origin ('terminal') or forward to another node
    # ('relay').  address is null when mode == 'terminal'.
    '__node_routing_table__': cannonical_schema_for_keys((
        'op_idn', 'mode', 'address',
    )),

    # In-flight relay tracking: while waiting for a downstream node (C) to reply,
    # B stores the original requester's identity here keyed by request_id.
    '__node_correlation_table__': cannonical_schema_for_keys((
        'request_id', 'origin_identity', 'received_at',
    )),

    # Live ZMQ DEALER connections this node has opened to peers.
    '__node_connections__': cannonical_schema_for_keys((
        'address', 'status',
    )),

    # -- Incoming system message schemas (tables that arrive on the data socket
    #    but carry routing control commands rather than domain data).
    #    Every system message table carries a '__system_op__' discriminator column.

    # Update one pipeline's routing entry.
    # mode in ('terminal', 'relay'); address is null when mode == 'terminal'.
    '__routing_update__': cannonical_schema_for_keys((
        '__system_op__', 'op_idn', 'mode', 'address',
    )),

    # Open a new DEALER connection to address.
    '__connect__': cannonical_schema_for_keys((
        '__system_op__', 'address',
    )),

    # Close an existing DEALER connection.
    '__disconnect__': cannonical_schema_for_keys((
        '__system_op__', 'address',
    )),
}
