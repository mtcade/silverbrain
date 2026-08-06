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

# Backend storage process factories (backend_read_process/backend_write_process,
# for wiring BackendReadOp/BackendWriteOp) live in silverflow.tableProcesses
# alongside the backend-specific TableOps they support.
