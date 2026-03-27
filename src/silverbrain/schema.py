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
    'process_id':       pl.Utf8,
    'op_id':            pl.Utf8,
    'input_tables':     pl.List( pl.Utf8 ),
    'output_tables':    pl.List( pl.Utf8 ),
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
    'table_id':         pl.Utf8,
    # effects df
    'effect_index':     pl.UInt32,
    'type':             pl.Utf8,
    'path_input':       pl.Int32,
    'path_column':      pl.Utf8,
    'output':           pl.Int32,
    'format':           pl.Utf8,
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
            'table_id':     list( ts.keys() ),
            'column_names': [ list( schema.keys() ) for schema in ts.values() ],
            'column_types': [
                [ dtype_to_str( t ) for t in schema.values() ]
                for schema in ts.values()
            ],
        },
        schema = table_schemas['tables_schema'],
    )
#/table_schemas_to_df

table_schemas: dict[ str, dict[ str, pl.DataType ] ] = {
    'op_bindings': cannonical_schema_for_keys((
        'process_id', 'op_id', 'input_tables',
        'output_tables',
    )),
    'tableOp_schema':  cannonical_schema_for_keys((
        'op_id', 'direction', 'index',
        'column_names', 'column_types',
        'extra', 'passthrough_from',
    )),
    'tableOp_effects':  cannonical_schema_for_keys((
        'op_id', 'effect_index', 'type',
        'path_input', 'path_column', 'output',
        'format',
    )),
    'tables_schema': cannonical_schema_for_keys((
        'table_id', 'column_names', 'column_types',
    )),
    'paths': cannonical_schema_for_keys((
        'key', 'fp', 'isfile', 'isdir',
    )),
}
