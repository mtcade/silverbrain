#
#//  web.py
#//  silverbrain
#//
#//  Created by Evan Mason on 5/29/26.
#//
"""
The `Web` is the main holder and orchestrator of computational units, referencing each in `._nodes`, each of which declares operations through:
    - op_idn (str): Identifier of an operation in that node
    - source ( tuple[ str,... ] ): Identifiers for the dataframe input
    - target (tuple[ str,...] ): Identifiers for the dataframe output

The results from each node will be async from their parent; they can be asyncio for calling external sources, or multiprocessed with shared memory for the source and target.

One Web can be responsible for multiple computation paths, reusing the same nodes and even the same op_idn for different paths. To keep track of this, when a `Web` provides an `op_idn` and dataframes, it also gives an identifier for the process which uses it. The node will then keep the `op_idn` and process identifier with the result, so the `Web` can route the result appropriately, even with multiple nodes used async.

To decide:
    - How do we handle the same node with the same op_idn used in the same parent process in different places? Do we have to require the same op_idn not be reused, and thus have aliasing within the node? OLD: Inconsistent handling, sometimes using imperative methods and sometimes declarative methods without clear separation and interaction.
    - Do we need to wrap each node in a `WebNode` or can we put them directly on `._nodes`? OLD METHOD: Wrap `cell.Cell` but child `CompositeWebNode` goes straight in.
    - How do we handle computations which cannot be represented as a DAG? Making it an actor model rather than DAG might be preferable, as long as we can represent all the same processes. OLD: Confused, overlapping, unergonomic combination of `__routing__` and `__table_processes__` without well defined async mechanics
    - How can we serialize processes effectively as a polars dataframe? OLD: '__table_processes__` which might be sufficient but might not
    - How do we handle spawning new threads or processes? A `Web` can have its own since it only handles routing; can we have the same async call to each child node, and have them decide on the implementation opaquely before giving their ref to their parent? OLD: Unclear, inconsistent handling of different nodes
"""

import asyncio
import contextvars
import json
import threading
import tomllib
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Coroutine

import polars as pl

from . import types
from .cell import Cell, _domain_process_df, cell_from_toml


# -- Helpers

def _decls_from_cell( cell: Cell ) -> list[ types.NodeDecl ]:
    """Read non-builtin, non-check process rows that have both source and target."""
    domain_df = _domain_process_df( cell.tables[ '__table_processes__' ] )
    decls = []
    for row in domain_df.to_dicts():
        src = row.get( 'source' ) or []
        tgt = row.get( 'target' ) or []
        if src and tgt:
            decls.append( types.NodeDecl(
                op_idn = row[ 'op_idn' ],
                source = tuple( src ),
                target = tuple( tgt ),
            ) )
    return decls
#/def _decls_from_cell


def _make_asyncio_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    t = threading.Thread( target=loop.run_forever, daemon=True )
    t.start()
    return loop
#/def _make_asyncio_loop


# -- Leaf nodes

class WebNode:
    """
    Leaf node wrapping a cell.Cell.

    Sequential mailbox: a single-worker ThreadPoolExecutor ensures Cell.run()
    calls are serialised, preserving cell.tables consistency across concurrent
    run() callers.

    _declarations: when provided (e.g. loaded from a composite TOML), these
    are returned directly by declare() instead of reading __table_processes__.
    """

    def __init__(
        self,
        _cell:         Cell,
        _declarations: list[ types.NodeDecl ] | None = None,
    ) -> None:
        self._cell         = _cell
        self._declarations = _declarations
        self._executor     = ThreadPoolExecutor( max_workers=1 )
    #/def __init__

    def declare( self ) -> list[ types.NodeDecl ]:
        if self._declarations is not None:
            return list( self._declarations )
        return _decls_from_cell( self._cell )
    #/def declare

    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]:
        def _call() -> dict[ str, pl.DataFrame ]:
            self._cell.run( op_idn, extra=inputs )
            tp_rows = {
                r[ 'op_idn' ]: r
                for r in self._cell.tables[ '__table_processes__' ].to_dicts()
            }
            targets = tp_rows[ op_idn ].get( 'target' ) or []
            return { t: self._cell.tables[ t ] for t in targets }
        ctx = contextvars.copy_context()
        return self._executor.submit( ctx.run, _call )
    #/def run
#/class WebNode


class AsyncWebNode:
    """
    Leaf node for IO-bound external calls (network, ZMQ, database, etc.).

    coro_factory(op_idn, process_id, inputs) must return a coroutine that
    resolves to dict[str, pl.DataFrame]. Runs on a dedicated asyncio event
    loop thread so callers receive a concurrent.futures.Future.
    """

    def __init__(
        self,
        _declarations: list[ types.NodeDecl ],
        _coro_factory: Callable[
            [ str, str, dict[ str, pl.DataFrame ] ],
            Coroutine,
        ],
        _loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._declarations = _declarations
        self._coro_factory = _coro_factory
        self._loop         = _loop if _loop is not None else _make_asyncio_loop()
    #/def __init__

    def declare( self ) -> list[ types.NodeDecl ]:
        return list( self._declarations )
    #/def declare

    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]:
        coro = self._coro_factory( op_idn, process_id, inputs )
        return asyncio.run_coroutine_threadsafe( coro, self._loop )
    #/def run
#/class AsyncWebNode


# -- Composite node

class Web:
    """
    Pure router: dispatches op_idn to child WebNodeProtocol instances and reactively
    chains outputs when a completed step's targets satisfy another op's sources.

    Supports cycles — termination is controlled by child process logic
    (TableProcessBranch / TableProcessCount / etc.) returning empty outputs,
    which prevents further chaining.

    _nodes maps a node identifier to any object implementing the WebNodeProtocol.

    declare() exposes only entry-point ops — ops whose sources are all external
    (not produced by any sibling op). Internal chain ops are implementation details.
    This makes Web nesting work correctly: the parent Web only routes to entry
    points and never double-fires internal continuations.
    """

    def __init__( self, _nodes: dict[ str, Any ] ) -> None:
        self._nodes   = _nodes
        self._routing: dict[ str, tuple[ str, types.NodeDecl ] ] = {}
        for node_id, node in self._nodes.items():
            for decl in node.declare():
                if decl.op_idn in self._routing:
                    raise ValueError(
                        f"Duplicate op_idn {decl.op_idn!r} declared by node {node_id!r}"
                        f" (already claimed by {self._routing[ decl.op_idn ][ 0 ]!r})"
                    )
                self._routing[ decl.op_idn ] = ( node_id, decl )
        self._declared_targets: frozenset[ str ] = frozenset(
            t for _, d in self._routing.values() for t in d.target
        )
    #/def __init__

    def _reachable_targets( self, entry_op: str ) -> tuple[ str, ... ]:
        """BFS: all target tables reachable from entry_op through internal chaining."""
        visited: set[ str ] = set()
        result:  set[ str ] = set()
        queue = [ entry_op ]
        while queue:
            op = queue.pop( 0 )
            if op in visited:
                continue
            visited.add( op )
            _, decl = self._routing[ op ]
            result.update( decl.target )
            for next_op, ( _, next_decl ) in self._routing.items():
                if next_op not in visited and any( t in next_decl.source for t in decl.target ):
                    queue.append( next_op )
        return tuple( sorted( result ) )
    #/def _reachable_targets

    def declare( self ) -> list[ types.NodeDecl ]:
        """
        Entry-point ops only: ops whose every source is NOT produced by any sibling.
        Target is the full set of tables reachable from that entry point (via BFS).
        Internal chain ops are not re-exported.
        """
        return [
            types.NodeDecl(
                op_idn = decl.op_idn,
                source = decl.source,
                target = self._reachable_targets( decl.op_idn ),
            )
            for _, decl in self._routing.values()
            if all( s not in self._declared_targets for s in decl.source )
        ]
    #/def declare

    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]:
        if op_idn not in self._routing:
            raise KeyError( f"Unknown op_idn {op_idn!r}" )

        outer:       Future                     = Future()
        accumulated: dict[ str, pl.DataFrame ]  = dict( inputs )
        pending:     set[ str ]                 = set()
        lock                                    = threading.Lock()

        def _on_done( fut: Future, completed_op: str ) -> None:
            to_fire: list[ tuple[ str, dict[ str, pl.DataFrame ] ] ] = []
            with lock:
                pending.discard( completed_op )
                exc = fut.exception()
                if exc is not None:
                    if not outer.done():
                        outer.set_exception( exc )
                    return
                outputs: dict[ str, pl.DataFrame ] = fut.result()
                newly_produced = set( k for k, v in outputs.items() if v is not None )
                for k, v in outputs.items():
                    if v is not None:
                        accumulated[ k ] = v
                    else:
                        accumulated.pop( k, None )
                for op, ( _, decl ) in self._routing.items():
                    if (
                        op not in pending
                        and any( s in newly_produced for s in decl.source )
                        and all( s in accumulated for s in decl.source )
                    ):
                        pending.add( op )
                        to_fire.append( ( op, { s: accumulated[ s ] for s in decl.source } ) )
                if not to_fire and not pending and not outer.done():
                    # Return only tables produced by ops (not initial pass-through inputs).
                    # Tables that are both input and target (e.g. updated in a cycle) are
                    # included because they appear in _declared_targets.
                    outer.set_result( {
                        k: v for k, v in accumulated.items()
                        if k in self._declared_targets
                    } )

            for op, op_inputs in to_fire:
                node_id, _ = self._routing[ op ]
                f = self._nodes[ node_id ].run( op, process_id, op_inputs )
                f.add_done_callback( lambda fut, _op=op: _on_done( fut, _op ) )
        #/def _on_done

        node_id, _ = self._routing[ op_idn ]
        pending.add( op_idn )
        f = self._nodes[ node_id ].run( op_idn, process_id, inputs )
        f.add_done_callback( lambda fut: _on_done( fut, op_idn ) )

        return outer
    #/def run
#/class Web


# -- TOML loading

def _apply_template_vars( s: str, vars: dict[ str, str ] ) -> str:
    for k, v in vars.items():
        s = s.replace( f'{{{k}}}', v )
    return s


def web_from_toml(
    path:           str | Path,
    ops_module,
    rng                         = None,
    verbose:        int         = 0,
    template_vars:  dict[ str, str ] | None = None,
) -> Web:
    """
    Load a Web from a {name}.toml composite map file.

    Schema
    ------
    [composite]
    main_id   = "..."          # identifier for this composite
    main_root = "../../"       # optional; relative to this file; used to resolve paths values

    [[nodes]]
    node_id = "..."            # identifier passed to cell_from_toml as main_id
    toml    = "path/to/brain.toml"   # relative to this file
    paths   = "path/to/paths.json"   # optional; JSON {key: path} injected as Cell paths table

    [[nodes.ops]]
    op_idn = "..."
    source = [...]
    target = [...]
    # Repeat for each exposed op. If [[nodes.ops]] is absent, declarations are
    # inferred from __table_processes__ via WebNode.declare().
    """
    path     = Path( path ).resolve()
    base_dir = path.parent

    with open( path, 'rb' ) as f:
        data = tomllib.load( f )

    comp      = data.get( 'composite', {} )
    main_root_raw = comp.get( 'main_root' )
    main_root = ( base_dir / main_root_raw ).resolve() if main_root_raw else base_dir

    tvars = template_vars or {}

    nodes: dict[ str, WebNode ] = {}
    for node_desc in data.get( 'nodes', [] ):
        node_id   = node_desc[ 'node_id' ]
        toml_path = ( base_dir / _apply_template_vars( node_desc[ 'toml' ], tvars ) ).resolve()

        paths_dict: dict[ str, str ] | None = None
        if 'paths' in node_desc:
            paths_json = ( base_dir / _apply_template_vars( node_desc[ 'paths' ], tvars ) ).resolve()
            with open( paths_json ) as f:
                raw = json.load( f )
            paths_dict = {
                '__main_root__': str( main_root ),
                **{
                    k: str( ( main_root / v ).resolve() )
                    for k, v in raw.items()
                },
            }

        cell = cell_from_toml(
            path       = toml_path,
            ops_module = ops_module,
            main_id    = node_id,
            rng        = rng,
            verbose    = verbose,
            paths_dict = paths_dict,
        )

        declarations: list[ types.NodeDecl ] | None = None
        if 'ops' in node_desc:
            declarations = [
                types.NodeDecl(
                    op_idn = op[ 'op_idn' ],
                    source = tuple( op[ 'source' ] ),
                    target = tuple( op[ 'target' ] ),
                )
                for op in node_desc[ 'ops' ]
            ]

        nodes[ node_id ] = WebNode( _cell=cell, _declarations=declarations )

    return Web( _nodes=nodes )
#/def web_from_toml
