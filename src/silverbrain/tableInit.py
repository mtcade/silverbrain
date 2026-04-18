#
#//  tableInit.py
#//  silverbrain
#//

from . import types
from .schema import table_schemas

import polars as pl
from dataclasses import dataclass
from typing import Self


@dataclass
class TableInitRef():
    """
    A named table initializer. References a TaggedTableProcess by op_id string
    for topological ordering and registration into tableOps.

    table_id   — the primary table being initialized
    source     — pure prerequisites used only for topological ordering;
                 deps absent from the registry (e.g. 'paths', 'self.index')
                 are treated as already satisfied
    op_id      — op_id of the TaggedTableProcess in __table_processes__ that
                 initializes this table
    written_by — opIds of post-init tableProcesses that update this table
    """
    table_id:   str
    source:     tuple[str, ...]
    op_id:      str
    written_by: tuple[str, ...]
    always_run: bool = False

    def as_polars(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'table_id':   [ self.table_id ],
                'source':     [ list( self.source ) ],
                'op_id':      [ self.op_id ],
                'written_by': [ list( self.written_by ) ],
                'always_run': [ self.always_run ],
            },
            schema = table_schemas[ '__table_init__' ],
        )
    #/def as_polars
#


@dataclass
class ProcessInitRef():
    """
    A named process initializer. Declares prerequisites for topological ordering
    alongside TableInitRef entries.

    op_id      — the op_id under which the process will be registered; used to
                 check whether it is already present in __table_processes__
    source     — tables or other process op_ids that must be ready first
    factory_id — op_id of the factory TaggedTableProcess in __table_processes__;
                 when run it produces and registers the target process
    always_run — reserved for symmetry with TableInitRef; currently unused
    """
    op_id:      str
    source:     tuple[ str, ... ]
    factory_id: str
    always_run: bool = False

    def as_polars(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_id':      [ self.op_id ],
                'source':     [ list( self.source ) ],
                'factory_id': [ self.factory_id ],
                'always_run': [ self.always_run ],
            },
            schema = table_schemas[ '__process_init__' ],
        )
    #/def as_polars
#/class ProcessInitRef


def table_init_df_from_dict(
    refs: dict[ str, TableInitRef ],
) -> pl.DataFrame:
    if not refs:
        return pl.DataFrame( schema = table_schemas[ '__table_init__' ] )
    return pl.concat( [ r.as_polars() for r in refs.values() ], how = 'vertical' )
#/def table_init_df_from_dict


def process_init_df_from_dict(
    refs: dict[ str, ProcessInitRef ],
) -> pl.DataFrame:
    if not refs:
        return pl.DataFrame( schema = table_schemas[ '__process_init__' ] )
    return pl.concat( [ r.as_polars() for r in refs.values() ], how = 'vertical' )
#/def process_init_df_from_dict


def topological_init_order_from_df(
    table_init_df:   pl.DataFrame,
    process_init_df: pl.DataFrame,
) -> list[ tuple[ str, str ] ]:
    """
    Returns (type, init_id) pairs in valid initialization order via Kahn's
    algorithm.  type is 'table' or 'process'; init_id is table_id or op_id.
    Source entries absent from both DataFrames are treated as already satisfied.
    Raises ValueError on cycle.
    """
    # Build unified registry: init_id -> (type, source_list)
    registry: dict[ str, tuple[ str, list[ str ] ] ] = {}
    for row in table_init_df.to_dicts():
        registry[ row[ 'table_id' ] ] = ( 'table',   list( row[ 'source' ] ) )
    for row in process_init_df.to_dicts():
        registry[ row[ 'op_id'    ] ] = ( 'process', list( row[ 'source' ] ) )

    in_degree:  dict[ str, int ]        = { k: 0  for k in registry }
    dependents: dict[ str, list[ str ] ] = { k: [] for k in registry }

    for init_id, ( _, sources ) in registry.items():
        for dep in sources:
            if dep in registry:
                in_degree[ init_id ] += 1
                dependents[ dep ].append( init_id )
            #
        #
    #

    queue = [ k for k, d in in_degree.items() if d == 0 ]
    order: list[ tuple[ str, str ] ] = []

    while queue:
        k = queue.pop( 0 )
        order.append( ( registry[ k ][ 0 ], k ) )
        for dep in dependents[ k ]:
            in_degree[ dep ] -= 1
            if in_degree[ dep ] == 0:
                queue.append( dep )
            #
        #
    #

    if len( order ) != len( registry ):
        seen  = { o[ 1 ] for o in order }
        cycle = [ k for k in registry if k not in seen ]
        raise ValueError( f'Cycle detected in init_deps: {cycle}' )
    #

    return order
#/def topological_init_order_from_df


def topological_init_order(
    tables: dict[ str, TableInitRef | ProcessInitRef ],
) -> list[str]:
    """
    Returns table IDs in a valid initialization order via Kahn's algorithm
    on init_deps. Deps absent from `tables` are treated as already satisfied.
    Raises ValueError on cycle.
    """
    in_degree:  dict[str, int]       = {tid: 0  for tid in tables}
    dependents: dict[str, list[str]] = {tid: [] for tid in tables}

    for tid, spec in tables.items():
        for dep in spec.source:
            if dep in tables:
                in_degree[tid] += 1
                dependents[dep].append(tid)
            #/if dep in tables
        #/for dep in spec.init_deps
    #/for tid, spec in tables.items()

    queue = [
        tid for tid, deg in in_degree.items()
        if deg == 0
    ]
    order: list[ str ] = []

    while queue:
        tid = queue.pop(0)
        order.append(tid)
        for dependent in dependents[tid]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
            #
        #/for dependent in dependents[tid]
    #/while queue

    if len(order) != len(tables):
        cycle = [tid for tid in tables if tid not in order]
        raise ValueError(f'Cycle detected in init_deps: {cycle}')
    #/if len(order) != len(tables)

    return order
#/def topological_init_order
