#
#//  polarsDataTypeStrings.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/25/26.
#//

import base64
import polars as pl

# Finite set of non-parametric dtypes that round-trip via getattr(pl, str(dtype)).
# str(dtype) gives e.g. "Int32", "Boolean", "Categorical", etc.
SIMPLE_DTYPE_NAMES: frozenset[ str ] = frozenset({
    'Boolean',
    'Int8', 'Int16', 'Int32', 'Int64',
    'UInt8', 'UInt16', 'UInt32', 'UInt64',
    'Float32', 'Float64',
    'String', 'Utf8',    # aliases — both serialize to "String" in polars 1.x
    'Binary',
    'Date', 'Time',
    'Categorical',
    'Object',
    'Null', 'Unknown',
})

def dtype_to_str( dtype: pl.DataType ) -> str:
    """
    Serialize a polars DataType to a string.

    - Simple types      → polars canonical string, e.g. "Int32", "Boolean"
    - List(simple)      → e.g. "List(String)"  (recursive)
    - Other parametric  → "ipc:<base64>" via Arrow IPC (requires pyarrow)
    """
    dtype_name = str( dtype )

    if dtype_name in SIMPLE_DTYPE_NAMES:
        return dtype_name
    #

    if isinstance( dtype, pl.List ):
        return 'List({})'.format( dtype_to_str( dtype.inner ) )
    #

    # Fallback: Arrow IPC base64
    import pyarrow as pa
    arrow_type = pl.Series( [], dtype = dtype ).to_arrow().type
    buf = pa.schema( [ pa.field( '', arrow_type ) ] ).serialize()
    return 'ipc:' + base64.b64encode( bytes( buf ) ).decode()
#/def dtype_to_str

def str_to_dtype( s: str ) -> pl.DataType:
    """
    Deserialize a polars DataType from a string produced by dtype_to_str.
    """
    if s.startswith( 'ipc:' ):
        import pyarrow as pa
        arrow_type = pa.ipc.read_schema(
            pa.BufferReader( base64.b64decode( s[ 4: ] ) )
        ).field( '' ).type
        return pl.from_arrow( pa.array( [], type = arrow_type ) ).dtype
    #

    if s.startswith( 'List(' ) and s.endswith( ')' ):
        return pl.List( str_to_dtype( s[ 5:-1 ] ) )
    #

    return getattr( pl, s )
#/def str_to_dtype
