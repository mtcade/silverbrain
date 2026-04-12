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
    A named table initializer. Wraps a TaggedTableProcess with metadata for
    topological ordering and update tracking.

    table_id   — the primary table being initialized
    init_deps  — pure prerequisites used only for topological ordering;
                 deps absent from the registry (e.g. 'paths', 'self.index')
                 are treated as already satisfied
    op         — the TaggedTableProcess that produces this table; its
                 source/target may be broader than init_deps
    written_by — opIds of post-init tableProcesses that write this table

    Satisfies TaggedTableProcess: source / target / __call__ all delegate to op.
    """
    table_id:   str
    init_deps:  tuple[str, ...]
    op:         types.TaggedTableProcess
    written_by: tuple[str, ...]
    always_run: bool = False

    @property
    def opId(self) -> str:
        return f'init_{self.table_id}'

    @property
    def source(self) -> tuple[str, ...]:
        return tuple(self.op.source)

    @property
    def target(self) -> tuple[str, ...]:
        return tuple(self.op.target)

    def __call__(
        self: Self,
        dfs: tuple[pl.DataFrame, ...],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[pl.DataFrame, ...]:
        return self.op(dfs, tableOps, verbose, verbose_prefix)

    def get_signatureCollector(self: Self) -> types.SignatureCollector:
        return self.op.get_signatureCollector()

    def get_op_bindings(self: Self, process_id: str) -> pl.DataFrame:
        return self.op.get_op_bindings(process_id)


@dataclass
class ProcessInitRef():
    """
    A named process initializer. Declares prerequisites for topological ordering
    alongside TableInitRef entries; initialized by calling factory(tables) and
    registering the returned TaggedTableProcess.

    node_id   — the opId under which the process will be registered
    init_deps — tables or other process node_ids that must be ready first
    factory   — called at init time with the current tables dict; returns the
                TaggedTableProcess to register under node_id
    always_run — reserved for symmetry with TableInitRef; currently unused
    """
    node_id:    str
    init_deps:  tuple[ str, ... ]
    factory:    Callable[ [ dict[ str, pl.DataFrame ] ], types.TaggedTableProcess ]
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
        for dep in spec.init_deps:
            if dep in tables:
                in_degree[tid] += 1
                dependents[dep].append(tid)

    queue = [tid for tid, deg in in_degree.items() if deg == 0]
    order: list[str] = []

    while queue:
        tid = queue.pop(0)
        order.append(tid)
        for dependent in dependents[tid]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(tables):
        cycle = [tid for tid in tables if tid not in order]
        raise ValueError(f'Cycle detected in init_deps: {cycle}')

    return order
