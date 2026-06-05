#
#//  execution.py
#//  silverbrain
#//
#//  Created by Evan Mason on 5/16/26.
#

import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Self
import threading

import polars as pl

from . import tableProcesses, types


# -- Shared helpers

def _lookup_process_row(
    tables:             dict[ str, pl.DataFrame ],
    op_idn:             str,
    processes_table_id: str,
) -> dict:
    df   = tables[ processes_table_id ]
    rows = { r[ 'op_idn' ]: r for r in df.to_dicts() }
    return rows[ op_idn ]


# -- Strategies

@dataclass
class SyncLocalStrategy:
    """
    Synchronous, in-process execution strategy.

    Holds references to the shared `tables` and `tableOps` dicts.
    Both `execute()` and `put()` mutate `tables` in place via `|=` and return
    the named outputs, so callers see results through the same dict reference.
    """

    tables:   dict[ str, pl.DataFrame ]
    tableOps: types.TableProcessDict

    def _lookup_row(
        self,
        op_idn:             str,
        processes_table_id: str,
    ) -> dict:
        return _lookup_process_row( self.tables, op_idn, processes_table_id )

    def execute(
        self,
        op_idn:             str,
        inputs:             dict[ str, pl.DataFrame ],
        processes_table_id: str = '__table_processes__',
        verbose:            int = 0,
        verbose_prefix:     str = '',
    ) -> dict[ str, pl.DataFrame ]:
        """Execute op_idn with named inputs, write outputs to self.tables, return outputs."""
        row    = self._lookup_row( op_idn, processes_table_id )
        df     = self.tables[ processes_table_id ]
        dfs    = tuple( inputs[ tid ] for tid in row[ 'source' ] )
        result = tableProcesses.run_from_df(
            df             = df,
            root_op_idn    = op_idn,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
        outputs = dict( zip( row[ 'target' ], result ) )
        self.tables |= outputs
        return outputs


@dataclass
class NodeNuStrategy:
    """
    Strategy backed by any Web/WebNode/AsyncWebNode.

    Generates a UUID process_id per execute() call and blocks on the returned Future.
    """
    _node: Any  # Web/WebNode/AsyncWebNode; untyped to avoid circular import

    def execute(
        self,
        op_idn:             str,
        inputs:             dict[ str, pl.DataFrame ],
        processes_table_id: str = '__table_processes__',
        verbose:            int = 0,
        verbose_prefix:     str = '',
    ) -> dict[ str, pl.DataFrame ]:
        process_id = str( uuid.uuid4() )
        return self._node.run( op_idn, process_id, inputs ).result()


# -- Context

@dataclass
class ExecutionContext:
    """
    Topology-transparent runner: same run() call regardless of whether the
    underlying strategy is a local Web, a LocalWebNode, or any future strategy.

    run(wait=True)  — blocks and returns dict[str, DataFrame] (default)
    run(wait=False) — fires in background thread; returns Future[dict[str, DataFrame]]
    """
    _strategy: types.ExecutionStrategy

    def run(
        self,
        op_idn:             str,
        inputs:             dict[ str, pl.DataFrame ] | None = None,
        processes_table_id: str  = '__table_processes__',
        verbose:            int  = 0,
        verbose_prefix:     str  = '',
        wait:               bool = True,
    ) -> dict[ str, pl.DataFrame ] | Future:
        def _call() -> dict[ str, pl.DataFrame ]:
            return self._strategy.execute(
                op_idn             = op_idn,
                inputs             = inputs or {},
                processes_table_id = processes_table_id,
                verbose            = verbose,
                verbose_prefix     = verbose_prefix,
            )
        if wait:
            return _call()
        fut: Future = Future()
        def _worker() -> None:
            try:
                fut.set_result( _call() )
            except Exception as exc:
                fut.set_exception( exc )
        threading.Thread( target=_worker, daemon=True ).start()
        return fut

    @classmethod
    def for_cell( cls, cell ) -> Self:
        """Sync-local context backed by a Cell instance."""
        return cls( _strategy = SyncLocalStrategy( tables=cell.tables, tableOps=cell.tableOps ) )

    @classmethod
    def for_web( cls, node ) -> Self:
        """Context backed by any Web/WebNode/AsyncWebNode."""
        return cls( _strategy = NodeNuStrategy( _node = node ) )
