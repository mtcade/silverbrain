#
#//  tableProcesses.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

"""
Node-row helpers for the __nodes__ schema (see schema.py and
nested-gathering-russell.md §1.6 / the squishy-sleeping-snowflake plan).

A Cell's __call__ is a declarative wiring engine (cell.py's _WiringEngine),
not hand-written per Cell: it fires whichever registered node's declared
source names have all received a value, feeding results forward by target
name, until the Cell's own declared target is resolved. __nodes__ rows are
what drive this -- which named children exist, whether each is a leaf
TableOp or a nested Cell, and each node's own parent-assigned source/target
names -- consumed both at construction time and at every __call__.
"""

from .schema import table_schemas

import polars as pl

from typing import Sequence


def make_node_row(
    node_idn:   str,
    kind:       str,
    source:     Sequence[ str ] = (),
    target:     Sequence[ str ] = (),
    parent_idn: str | None      = None,
    ) -> pl.DataFrame:
    """Build a single-row DataFrame with the '__nodes__' schema."""
    if kind not in ( 'op', 'cell' ):
        raise ValueError( f"kind must be 'op' or 'cell', got {kind!r}" )
    return pl.DataFrame(
        {
            'node_idn':   [ node_idn ],
            'parent_idn': [ parent_idn ],
            'kind':       [ kind ],
            'source':     [ list( source ) ],
            'target':     [ list( target ) ],
        },
        schema = table_schemas[ '__nodes__' ],
    )
#/def make_node_row


# -- Backend Storage Process Factories
#
# BackendReadOp/BackendWriteOp already dispatch parquet/sqlite/postgres as
# plain Python if/elif inside their own __call__ (tableOps.py) -- these
# factories just build the __nodes__ row so callers don't need to list all
# five backend source tables by hand. node_idn doubles as the tableOps
# lookup key (Cell.__nodes__'s convention -- see schema.py), so register a
# BackendReadOp/BackendWriteOp instance under the *same* node_idn you pass
# here for each named call site (multiple named reads/writes can all share
# one instance, same object under multiple keys):
#
#   tableOps = {
#       'read_it':  BackendReadOp(),
#       'write_it': BackendWriteOp(),
#   }

def backend_read_process(
    node_idn: str,
    source_backend_ref: str,
    target_data: str,
    source_backend_index:   str = '__backend_index__',
    source_parquet_backend: str = '__parquet_backend__',
    source_sqlite_backend:  str = '__sqlite_backend__',
    source_postgres_backend: str = '__postgres_backend__',
    ) -> pl.DataFrame:
    """__nodes__ row that reads from the backend identified by source_backend_ref."""
    return make_node_row(
        node_idn = node_idn,
        kind     = 'op',
        source   = [
            source_backend_ref,
            source_backend_index,
            source_parquet_backend,
            source_sqlite_backend,
            source_postgres_backend,
        ],
        target   = [ target_data ],
    )
#/def backend_read_process

def backend_write_process(
    node_idn: str,
    source_data: str,
    source_backend_ref: str,
    target_status: str,
    source_backend_index:    str = '__backend_index__',
    source_parquet_backend:  str = '__parquet_backend__',
    source_sqlite_backend:   str = '__sqlite_backend__',
    source_postgres_backend: str = '__postgres_backend__',
    ) -> pl.DataFrame:
    """
    __nodes__ row that writes source_data to the backend identified by
    source_backend_ref. source_backend_ref may include an optional
    'if_exists' column to override the backend default. Returns
    source_backend_ref as target_status unchanged.
    """
    return make_node_row(
        node_idn = node_idn,
        kind     = 'op',
        source   = [
            source_data,
            source_backend_ref,
            source_backend_index,
            source_parquet_backend,
            source_sqlite_backend,
            source_postgres_backend,
        ],
        target   = [ target_status ],
    )
#/def backend_write_process
