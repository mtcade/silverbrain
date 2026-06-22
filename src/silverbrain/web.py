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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Coroutine

import polars as pl

from . import types
from .cell import Cell, _domain_process_df, cell_from_toml


# -- Helpers

def _decls_from_cell( cell: Cell ) -> list[ types.NodeDecl ]:
    """Read non-builtin, non-check process rows."""
    domain_df = _domain_process_df( cell.tables[ '__table_processes__' ] )
    decls = []
    for row in domain_df.to_dicts():
        decls.append( types.NodeDecl(
            op_idn = row[ 'op_idn' ],
            source = tuple( row.get( 'source' ) or [] ),
            target = tuple( row.get( 'target' ) or [] ),
        ) )
    return decls
#/def _decls_from_cell


def _make_asyncio_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    t = threading.Thread( target=loop.run_forever, daemon=True )
    t.start()
    return loop
#/def _make_asyncio_loop


def _apply_template_vars( s: str, vars: dict[ str, str ] ) -> str:
    for k, v in vars.items():
        s = s.replace( f'{{{k}}}', v )
    return s


# -- Web-level process descriptor

_DISALLOWED_WEB_PROC_TYPES = frozenset({ 'TableProcessWhile', 'TableProcessSleep' })

@dataclass
class _WebProc:
    """
    Descriptor for a web-level composite process, parsed from [[processes]] in a
    web TOML.  Terms reference node op_idns or other web process op_idns.
    While and Sleep types are disallowed — web processes must be DAG-shaped.
    """
    op_idn:    str
    ptype:     str                  # TableProcessSequence | TableProcessCount | TableProcessBranch | TableProcessRef
    source:    tuple[ str, ... ]
    target:    tuple[ str, ... ]
    terms:     list[ str ]          # sequence/count body; ref body; branch otherwise
    ifs:       list[ str ]          # branch: check terms
    thens:     list[ str ]          # branch: then terms
    otherwise: str | None           # branch: fallthrough term
    count:     int | None           # count: repetitions
#/class _WebProc


def _parse_web_processes( entries: list[ dict ] ) -> dict[ str, _WebProc ]:
    result: dict[ str, _WebProc ] = {}
    for e in entries:
        t = e[ 'type' ]
        if t in _DISALLOWED_WEB_PROC_TYPES:
            raise ValueError(
                f"Web-level [[processes]] may not use {t!r}; "
                f"only DAG-shaped process types are allowed (no While or Sleep)."
            )
        op = e[ 'op_idn' ]
        result[ op ] = _WebProc(
            op_idn    = op,
            ptype     = t,
            source    = tuple( e.get( 'source', [] ) ),
            target    = tuple( e.get( 'target', [] ) ),
            terms     = list( e.get( 'terms', [] ) ),
            ifs       = list( e.get( 'ifs', [] ) ),
            thens     = list( e.get( 'thens', [] ) ),
            otherwise = e.get( 'otherwise' ),
            count     = e.get( 'count' ),
        )
    return result
#/def _parse_web_processes


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
        return []
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

    def init_data( self, **kwargs ) -> None:
        self._cell.init_data( **kwargs )
    #/def init_data
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


# -- Declaration override wrapper

class _AliasedNode:
    """Wraps any node, presenting a fixed aliased declaration list to the parent Web."""

    def __init__(
        self,
        _inner:        Any,
        _declarations: list[ types.NodeDecl ],
    ) -> None:
        self._inner        = _inner
        self._declarations = _declarations

    def declare( self ) -> list[ types.NodeDecl ]:
        return list( self._declarations )

    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]:
        return self._inner.run( op_idn, process_id, inputs )

    def init_data( self, **kwargs ) -> None:
        if hasattr( self._inner, 'init_data' ):
            self._inner.init_data( **kwargs )
#/class _AliasedNode


# -- Composite node

class Web:
    """
    Pure router: dispatches op_idn to child WebNodeProtocol instances and reactively
    chains outputs when a completed step's targets satisfy another op's sources.

    Supports cycles — termination is controlled by child process logic
    (TableProcessBranch / TableProcessCount / etc.) returning empty outputs,
    which prevents further chaining.

    _nodes maps a node identifier to any object implementing the WebNodeProtocol.

    _web_processes: optional dict of composite-level DAG processes declared via
    [[processes]] in the web TOML.  These may reference node op_idns or other
    web processes.  While and Sleep types are disallowed (non-DAG).

    declare() behaviour:
      - If _web_processes is non-empty: exposes only the web processes.
        Node ops are internal implementation details hidden from parent webs.
      - If _web_processes is empty: exposes entry-point node ops (ops whose
        sources are not produced by any sibling), with BFS-expanded targets.
        This preserves backward-compatible transparent-passthrough behaviour.

    Web itself satisfies WebNodeProtocol so it can be used as a node inside
    another Web — enabling recursive composite loading.
    """

    def __init__(
        self,
        _nodes:         dict[ str, Any ],
        _web_processes: dict[ str, _WebProc ] | None = None,
    ) -> None:
        self._nodes          = _nodes
        self._web_processes  = _web_processes or {}
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
        If [[processes]] are defined, expose only those as the interface.
        Otherwise expose entry-point node ops (backward-compatible).
        """
        if self._web_processes:
            return [
                types.NodeDecl(
                    op_idn = proc.op_idn,
                    source = proc.source,
                    target = proc.target,
                )
                for proc in self._web_processes.values()
            ]
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

    # -- Web process execution

    def _run_term_sync(
        self,
        term:        str,
        process_id:  str,
        accumulated: dict[ str, pl.DataFrame ],
    ) -> dict[ str, pl.DataFrame ]:
        """Run one term (web process or node op) and return its outputs.

        Search order:
        1. Web processes ([[processes]] in TOML) — sequential, no reactive cascade.
        2. Node routing ([[nodes.ops]] in TOML) — leaf WebNode cells only.
           Nested Web nodes are never called synchronously; they must be reached
           through the reactive chain via Web.run().
        """
        if term in self._web_processes:
            return self._exec_web_proc( self._web_processes[ term ], process_id, accumulated )
        if term in self._routing:
            node_id, decl = self._routing[ term ]
            node = self._nodes[ node_id ]
            if isinstance( node, Web ):
                raise TypeError(
                    f"Web process term {term!r} resolves to a nested composite Web — "
                    f"nested webs must not be called synchronously via [[processes]]."
                )
            term_inputs = { s: accumulated[ s ] for s in decl.source if s in accumulated }
            actual_op = decl.node_op_idn or term
            return node.run( actual_op, process_id, term_inputs ).result()
        raise KeyError( f"Unknown term {term!r} — not in [[processes]] or [[nodes.ops]]" )
    #/def _run_term_sync

    def _check_term_sync(
        self,
        term:        str,
        process_id:  str,
        accumulated: dict[ str, pl.DataFrame ],
    ) -> bool:
        """
        Evaluate a branch check term.  If the result contains '__check__', reads its
        first cell as a bool.  Otherwise truthy if any returned DataFrame has rows.
        """
        result = self._run_term_sync( term, process_id, accumulated )
        if '__check__' in result:
            df = result[ '__check__' ]
            return bool( df.shape[ 0 ] > 0 and df[ df.columns[ 0 ] ][ 0 ] )
        return any( v is not None and v.shape[ 0 ] > 0 for v in result.values() )
    #/def _check_term_sync

    def _exec_web_proc(
        self,
        proc:        _WebProc,
        process_id:  str,
        inputs:      dict[ str, pl.DataFrame ],
    ) -> dict[ str, pl.DataFrame ]:
        """Execute a web process, returning only its declared target tables."""
        accumulated = dict( inputs )

        if proc.ptype in ( 'TableProcessRef', 'TableProcessSequence' ):
            for term in proc.terms:
                result = self._run_term_sync( term, process_id, accumulated )
                accumulated.update( { k: v for k, v in result.items() if v is not None } )

        elif proc.ptype == 'TableProcessCount':
            if proc.count is None:
                raise ValueError( f"Web process {proc.op_idn!r} has type=TableProcessCount but no count" )
            for _ in range( proc.count ):
                for term in proc.terms:
                    result = self._run_term_sync( term, process_id, accumulated )
                    accumulated.update( { k: v for k, v in result.items() if v is not None } )

        elif proc.ptype == 'TableProcessBranch':
            for if_term, then_term in zip( proc.ifs, proc.thens ):
                if self._check_term_sync( if_term, process_id, accumulated ):
                    result = self._run_term_sync( then_term, process_id, accumulated )
                    accumulated.update( { k: v for k, v in result.items() if v is not None } )
                    break
            else:
                if proc.otherwise:
                    result = self._run_term_sync( proc.otherwise, process_id, accumulated )
                    accumulated.update( { k: v for k, v in result.items() if v is not None } )

        else:
            raise ValueError( f"Unknown web process type {proc.ptype!r}" )

        return { k: v for k, v in accumulated.items() if k in proc.target }
    #/def _exec_web_proc

    # -- Main dispatch

    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]:
        # Web process path: execute synchronously in a daemon thread, return Future.
        if op_idn in self._web_processes:
            proc  = self._web_processes[ op_idn ]
            outer: Future = Future()
            def _exec_proc() -> None:
                try:
                    outer.set_result( self._exec_web_proc( proc, process_id, inputs ) )
                except Exception as exc:
                    outer.set_exception( exc )
            threading.Thread( target=_exec_proc, daemon=True ).start()
            return outer

        # Node routing path: reactive chaining over node ops.
        if op_idn not in self._routing:
            raise KeyError( f"Unknown op_idn {op_idn!r}" )

        outer:       Future                    = Future()
        accumulated: dict[ str, pl.DataFrame ] = dict( inputs )
        pending:     set[ str ]                = set()
        completed:   set[ str ]                = set()
        lock                                   = threading.Lock()

        def _on_done( fut: Future, completed_op: str ) -> None:
            to_fire: list[ tuple[ str, dict[ str, pl.DataFrame ] ] ] = []
            with lock:
                pending.discard( completed_op )
                completed.add( completed_op )
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
                        and op not in completed
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
                node_id, op_decl = self._routing[ op ]
                actual_op = op_decl.node_op_idn or op
                f = self._nodes[ node_id ].run( actual_op, process_id, op_inputs )
                f.add_done_callback( lambda fut, _op=op: _on_done( fut, _op ) )
        #/def _on_done

        node_id, decl = self._routing[ op_idn ]
        actual_op = decl.node_op_idn or op_idn
        pending.add( op_idn )
        f = self._nodes[ node_id ].run( actual_op, process_id, inputs )
        f.add_done_callback( lambda fut: _on_done( fut, op_idn ) )

        return outer
    #/def run

    def init_data( self, **kwargs ) -> None:
        """Cascade init_data to all child nodes that support it."""
        for node in self._nodes.values():
            if hasattr( node, 'init_data' ):
                node.init_data( **kwargs )
    #/def init_data
#/class Web


# -- TOML loading

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
    toml    = "path/to/cell_or_web.toml"   # relative to this file
                                             # If the TOML has a [composite] header it is
                                             # loaded recursively as a child Web; if it has
                                             # [cell] it is loaded as a Cell via WebNode.
    paths   = "path/to/paths.json"   # optional; JSON {key: path}; only used for cell nodes

    [[nodes.ops]]
    op_idn = "..."
    source = [...]
    target = [...]
    # Repeat for each exposed op on a brain node.  Absent → inferred from
    # __table_processes__.  Ignored for composite (web) nodes.

    [[processes]]
    op_idn = "..."
    type   = "TableProcessSequence" | "TableProcessBranch" | "TableProcessCount"
           | "TableProcessRef"
           # TableProcessWhile and TableProcessSleep are disallowed.
    source = [...]
    target = [...]
    terms  = [...]   # op_idns of [[nodes.ops]] entries or other [[processes]]
    # TableProcessBranch extras:
    ifs       = [...]      # check terms
    thens     = [...]      # then terms
    otherwise = "..."      # optional fallthrough term
    # TableProcessCount extras:
    count = N
    """
    path     = Path( path ).resolve()
    base_dir = path.parent

    with open( path, 'rb' ) as f:
        data = tomllib.load( f )

    comp          = data.get( 'composite', {} )
    main_root_raw = comp.get( 'main_root' )
    main_root     = ( base_dir / main_root_raw ).resolve() if main_root_raw else base_dir

    tvars = template_vars or {}

    nodes: dict[ str, Any ] = {}
    for node_desc in data.get( 'nodes', [] ):
        node_id   = node_desc[ 'node_id' ]
        toml_path = ( base_dir / _apply_template_vars( node_desc[ 'toml' ], tvars ) ).resolve()

        # Detect composite vs brain by inspecting the TOML header.
        with open( toml_path, 'rb' ) as f:
            node_header = tomllib.load( f )
        is_composite = 'composite' in node_header

        if is_composite:
            child_web = web_from_toml(
                path          = toml_path,
                ops_module    = ops_module,
                rng           = rng,
                verbose       = verbose,
                template_vars = tvars,
            )
            if 'ops' in node_desc:
                child_declared = { d.op_idn: d for d in child_web.declare() }
                sub_declarations: list[ types.NodeDecl ] = []
                for op in node_desc[ 'ops' ]:
                    child_op = op[ 'op_idn' ]
                    alias    = op.get( 'alias_idn' )
                    exposed  = alias if alias else child_op
                    if child_op not in child_declared:
                        raise ValueError(
                            f"op_idn {child_op!r} not found in subweb {node_id!r}; "
                            f"available: {sorted( child_declared )}"
                        )
                    child_d = child_declared[ child_op ]
                    sub_declarations.append( types.NodeDecl(
                        op_idn      = exposed,
                        source      = child_d.source,
                        target      = child_d.target,
                        node_op_idn = child_op if alias else None,
                    ) )
                nodes[ node_id ] = _AliasedNode( child_web, sub_declarations )
            else:
                nodes[ node_id ] = child_web
        else:
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

            declarations: list[ types.NodeDecl ] = []
            if 'ops' in node_desc:
                cell_op_idns = { d.op_idn for d in _decls_from_cell( cell ) }
                for op in node_desc[ 'ops' ]:
                    cell_op = op[ 'op_idn' ]
                    alias   = op.get( 'alias_idn' )
                    exposed = alias if alias else cell_op
                    if cell_op not in cell_op_idns:
                        raise ValueError(
                            f"op_idn {cell_op!r} not found among ops declared by cell {node_id!r}"
                        )
                    declarations.append( types.NodeDecl(
                        op_idn      = exposed,
                        source      = tuple( op[ 'source' ] ),
                        target      = tuple( op[ 'target' ] ),
                        node_op_idn = cell_op if alias else None,
                    ) )

            nodes[ node_id ] = WebNode( _cell=cell, _declarations=declarations )

    seen_exposed: dict[ str, str ] = {}
    for nid, node in nodes.items():
        for decl in node.declare():
            if decl.op_idn in seen_exposed:
                raise ValueError(
                    f"Duplicate exposed op name {decl.op_idn!r}: "
                    f"node {seen_exposed[ decl.op_idn ]!r} and {nid!r} both expose this name "
                    f"(use alias_idn to disambiguate)"
                )
            seen_exposed[ decl.op_idn ] = nid

    web_processes = _parse_web_processes( data.get( 'processes', [] ) )

    return Web( _nodes=nodes, _web_processes=web_processes )
#/def web_from_toml
