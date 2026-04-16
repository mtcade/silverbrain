#
#//  tableInit.py
#//  silverbrain
#//

from . import types

import polars as pl
from dataclasses import dataclass
from typing import Callable, Self


@dataclass
class TableInitRef():
    """
    A named table initializer. References a TaggedTableProcess by op_id string
    for topological ordering and registration into tableOps.

    table_id   — the primary table being initialized
    source     — pure prerequisites used only for topological ordering;
                 deps absent from the registry (e.g. 'paths', 'self.index')
                 are treated as already satisfied
    op_id      — opId of the TaggedTableProcess in tableOps that initializes this table
    written_by — opIds of post-init tableProcesses that update this table
    """
    table_id:   str
    source:     tuple[str, ...]
    op_id:      str
    written_by: tuple[str, ...]
    always_run: bool = False

    @property
    def opId(self) -> str:
        return self.op_id
#

@dataclass
class ProcessInitRef():
    """
    A named process initializer. Declares prerequisites for topological ordering
    alongside TableInitRef entries; initialized by calling factory(tables) and
    registering the returned TaggedTableProcess.

    op_id   — the opId under which the process will be registered
    init_deps — tables or other process op_ids that must be ready first
    factory   — called at init time with the current tables dict; returns the
                TaggedTableProcess to register under op_id
    always_run — reserved for symmetry with TableInitRef; currently unused
    """
    op_id:     str
    source:    tuple[ str, ... ]
    factory:   Callable[ [ dict[ str, pl.DataFrame ] ], types.TaggedTableProcess ]
    always_run: bool = False
#/class ProcessInitRef


def topological_init_order(
    tables: dict[ str, 'TableInitRef | ProcessInitRef' ],
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
