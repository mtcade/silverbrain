#
#//  cell.py
#//  silverbrain
#//

from . import configTree, exprParse, tableOps, tableProcesses, types
from .types import Actor
from .tableOps import PlaceholderOp, TableOp, TableOpSchema
from .parameterParse import ParameterParser, op_ctor_kwargs_forest
from .ReadWriteOps import merge_resources
from .schema import table_schemas, table_schemas_to_df
from .sunder import sunder_table_factories

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Self, Sequence

import inspect
import threading

import polars as pl
import numpy as np

from uuid_extensions import uuid7

# -- System table schemas
_SYSTEM_SCHEMAS: dict[ str, dict ] = {
    '__tables_schema__':          table_schemas[ '__tables_schema__'   ],
    '__nodes__':                  table_schemas[ '__nodes__'           ],
    '__table_op_schema__':        table_schemas[ '__table_op_schema__'  ],
    '__table_op_effects__':       table_schemas[ '__table_op_effects__' ],
    # TableOp construction-parameter forest -- see configTree.py's module
    # docstring and squishy-sleeping-snowflake.md. No parameter sources
    # exist yet, so this is always empty for now; the schema is
    # ParameterParser's own (with zero dtypes registered, just its
    # identifier columns), reused directly rather than duplicated.
    '__table_ops_parameters__':   ParameterParser().schema,
}

# -- Async dispatch: a shared thread pool every Cell's wiring engine and
# bind() submit calls to, so a synchronous TableOp gets adapted into a
# non-blocking Future uniformly with a nested Cell (which already returns
# one natively) or a Ray remote handle. See squishy-sleeping-snowflake.md.
_EXECUTOR = ThreadPoolExecutor( thread_name_prefix = 'silverbrain-actor' )


# Sentinel distinguishing "no non-null token yet" from a genuine None
# result value in _WiringEngine._scan's target resolution.
_UNRESOLVED = object()


@dataclass( frozen = True )
class Token:
    """
    One value appended to a named port's log (_WiringEngine.logs) -- an
    entry in that port's FIFO queue, never overwritten by a later write
    (see _WiringEngine's docstring: this is a point-to-point/Kahn Process
    Network-style wiring model, not a shared overwritable blackboard).
    corr_id is carried along purely for tracing/correlation -- relayed
    unchanged from whichever token(s) triggered the firing that produced
    this one (see _WiringEngine._dispatch); it plays no role in which
    tokens get paired together, that's decided entirely by each port's own
    queue position. value is the dataframe itself (or None -- a legitimate
    token, not a missing one; see _WiringEngine._on_done).
    """
    corr_id: tuple[ str, ... ]
    value:   pl.DataFrame | None
#/class Token


def _call_actor(
    actor:          Actor,
    dfs:            tuple[ pl.DataFrame, ... ],
    corr_id:        tuple[ str, ... ],
    verbose:        int,
    verbose_prefix: str,
    ) -> tuple[ pl.DataFrame, ... ]:
    """
    Invoke a bound Actor. Always runs on a thread-pool worker (see
    _EXECUTOR), never the original caller's thread -- this is what makes
    __call__/bind() non-blocking from the caller's perspective. Every Actor
    takes corr_id, so it's always passed. If the target itself returns a
    Future (a nested Cell, or a Ray remote call already wrapped), blocks
    this worker thread on it rather than the caller -- a known
    simplification; see squishy-sleeping-snowflake.md.
    """
    if hasattr( actor, 'remote' ):
        import ray
        result = ray.get( actor.__call__.remote( dfs, corr_id, verbose, verbose_prefix ) )
    else:
        result = actor( dfs, corr_id = corr_id, verbose = verbose, verbose_prefix = verbose_prefix )
    if isinstance( result, Future ):
        result = result.result()
    return result
#/def _call_actor


class _WiringEngine:
    """
    Runs one Cell.__call__ invocation's wiring graph as a point-to-point,
    Kahn Process Network-style network: each named port is an append-only
    FIFO log of Tokens, never an overwritable "latest value" slot -- a
    token is never silently lost to a later write. Each registered node
    keeps its own read cursor per source name (self.cursor, keyed by
    (node_idn, source_name)), so fan-out (several nodes reading the same
    name) works for free -- every consumer sees every token, in order, at
    its own pace, independent of any other consumer's pace.

    A node's `join` firing rule (__nodes__ row) decides when it's ready,
    checking each declared source's presence (does it have an unread
    token at all) -- there is no cross-source identity/round check:
      - 'all' (default): fires once every declared source has an unread
        token (an AND-join).
      - 'any': fires once at least one declared source has an unread
        token; sources with nothing unread contribute None this firing.
      - 'at_least_k': fires once join_k declared sources have an unread
        token; the rest contribute None.
    Every source that had an unread token when the node fired has its
    cursor advanced by one (its oldest unread token consumed); a source
    with nothing unread is simply left for a future scan.

    Correctness of pairing across a node's several source ports rests on
    Kahn Process Network determinism, not on any explicit correlation id:
    since a node is never dispatched again while a prior firing of itself
    is still in flight (self.in_flight), and each port's queue is strictly
    FIFO, "the i-th token on port A" and "the i-th token on port B" are
    whatever the graph's own causal/topological structure produced them
    to be -- consuming the oldest unread token from each required port,
    every firing, correctly reconstructs that pairing with no separate
    round-tracking machinery required. This is what replaced an earlier,
    more complicated round-id-matching design (see git history) once it
    became clear the round-matching problem it solved was itself only a
    symptom of routing through shared, overwritable named slots -- with
    genuine per-port FIFO queues the disambiguation need mostly dissolves.

    Not restricted to a DAG. The same node may fire more than once (a
    loop). corr_id is carried purely for tracing/correlation, not
    correctness -- see Token's docstring and Cell.__call__'s corr_id
    propagation notes; a firing's output tokens are tagged with the
    corr_id of whichever source port (first in declared order) actually
    participated, relayed unchanged, with no minting/extension performed
    here.

    Deadlock is detected, not silently hung forever: if a scan finds
    nothing newly fireable and nothing still in flight, but the Cell's own
    target isn't fully resolved, `future` gets a RuntimeError naming the
    unresolved names.

    A Cell's own target resolves on each name's first NON-null token, not
    merely its first token -- an internal null token (e.g. a loop's
    in-progress signal on one branch while the real result is still being
    computed on another) is never mistaken for "the Cell is done." A node
    that reads its own prior output as a source (a self-referencing loop)
    must itself guard against being re-invoked with a null input once it
    has emitted its own termination signal -- nothing here silently stops
    dispatching it just because a value happens to be null (see Token's
    docstring: null is an ordinary value, not a special engine-level case).

    "_" is a reserved discard target: a node's target list may name a
    result slot "_" to mean "run this (side effects included) but never
    store or wire the result anywhere" -- e.g. a write op that always
    echoes the table it wrote, when the caller doesn't want that echo
    consumed by anything. Discarded results never enter `logs`, so nothing
    can depend on them and they never contribute to resolving a target.
    This is node-level only (register_node's target=) -- a Cell's own
    external source/target contract is unaffected.

    Sunder tables (see sunder.py) are auto-supplied at construction, before
    any node ever fires -- but ONLY for a name nothing else could ever
    supply: a declared source name that (a) names a registered sunder
    factory, (b) is absent from this Cell's own self.source (so no caller
    could ever seed it), AND (c) is never the target of any node in this
    same Cell (so no sibling node could ever produce it either) gets one
    freshly-built token appended to its log up front. This is what lets
    e.g. `_trigger` reach a deeply-nested node without every enclosing Cell
    re-declaring it in its own `[cell] source = [...]` just to relay it
    downward -- while still always preferring an explicit value over the
    synthetic one: if a parent DOES pass a real value for that name (via
    self.source, or by wiring some node's target to it), auto-supply never
    fires for it at all, so there's no synthetic token racing a real one
    into the same FIFO log and no risk of the real one being shadowed.
    """

    def __init__(
        self:           Self,
        cell:           'Cell',
        root_id:        tuple[ str, ... ],
        verbose:        int,
        verbose_prefix: str,
        ) -> None:
        self.cell           = cell
        self.root_id        = root_id
        self.verbose        = verbose
        self.verbose_prefix = verbose_prefix
        self.future: Future = Future()
        self.lock           = threading.Lock()
        self.logs:   dict[ str, list[ Token ] ]        = {}
        self.cursor: dict[ tuple[ str, str ], int ]    = {}
        self.in_flight:  set[ str ]                    = set()
        self._nodes = cell.tables[ '__nodes__' ].to_dicts()

        # -- Sunder table auto-supply (see sunder.py's module docstring).
        # Explicit always wins over magic: auto-supply only ever fires for
        # a name NOTHING in this Cell could ever explicitly supply --
        # neither this Cell's own external contract (self.source, seeded
        # by Cell.__call__'s seed(), which zips dfs against exactly that
        # tuple) nor any sibling node's target within this same Cell
        # (an explicit intra-cell pass-through). If either route could
        # produce the name, auto-supply skips it entirely, so a genuine
        # parent-supplied value is never raced against -- let alone
        # shadowed by -- a synthetic one landing first in the same FIFO
        # log. Only once both routes are ruled out does this Cell build
        # and seed the sunder table itself, once, right here -- before
        # seed() or _scan() ever runs -- so every node (and every fan-out
        # consumer of the same name within this Cell) reads the same
        # single token, exactly as if a parent had passed it in.
        needed_sources = {
            s for row in self._nodes for s in ( row.get( 'source' ) or [] )
        }
        explicitly_supplied = set( cell.source ) | {
            t for row in self._nodes for t in ( row.get( 'target' ) or [] )
        }
        for name in needed_sources:
            if name in sunder_table_factories and name not in explicitly_supplied:
                self.logs.setdefault( name, [] ).append(
                    Token( corr_id = root_id, value = sunder_table_factories[ name ]() )
                )
            #/if name in sunder_table_factories and name not in explicitly_supplied
        #/for name in needed_sources
    #/def __init__

    def seed( self: Self, values: dict[ str, pl.DataFrame ] ) -> None:
        with self.lock:
            for k, v in values.items():
                self.logs.setdefault( k, [] ).append( Token( corr_id = self.root_id, value = v ) )
        self._scan()
    #/def seed

    def _scan( self: Self ) -> None:
        to_fire: list[ tuple[ str, dict, tuple, tuple ] ] = []
        with self.lock:
            if self.future.done():
                return
            # Resolve on each target's first NON-null token, not merely its
            # first token -- a null token is a legitimate in-flight signal
            # (e.g. loop-in-progress) an internal node may pass through
            # before the real result arrives; the Future this Cell itself
            # returns should never resolve to "nothing yet" just because a
            # transient null happened to reach a target name first.
            resolved = {
                t: next( ( tok.value for tok in self.logs.get( t, [] ) if tok.value is not None ), _UNRESOLVED )
                for t in self.cell.target
            }
            if all( v is not _UNRESOLVED for v in resolved.values() ):
                self.future.set_result( tuple( resolved[ t ] for t in self.cell.target ) )
                return
            for row in self._nodes:
                node_idn = row[ 'node_idn' ]
                if node_idn in self.in_flight:
                    continue
                src = row[ 'source' ] or []
                if not src:
                    continue

                unread: dict[ str, list[ Token ] ] = {
                    s: self.logs.get( s, [] )[ self.cursor.get( ( node_idn, s ), 0 ): ]
                    for s in src
                }
                present = [ s for s in src if unread[ s ] ]
                if not present:
                    continue

                join = row.get( 'join' ) or 'all'
                if join == 'all':
                    if len( present ) != len( src ):
                        continue
                    participants = src
                elif join == 'any':
                    participants = present
                elif join == 'at_least_k':
                    if len( present ) < row[ 'join_k' ]:
                        continue
                    participants = present
                else:
                    raise ValueError( f"__nodes__ row {node_idn!r} has unrecognized join={join!r}" )
                participants = set( participants )

                for s in participants:
                    self.cursor[ ( node_idn, s ) ] = self.cursor.get( ( node_idn, s ), 0 ) + 1
                inputs = tuple(
                    unread[ s ][ 0 ].value if s in participants else None
                    for s in src
                )
                # corr_id relayed from whichever source port (first in
                # declared order) actually participated -- purely for
                # tracing, see Token's docstring; never used for matching.
                dispatch_corr_id = next( unread[ s ][ 0 ].corr_id for s in src if s in participants )

                self.in_flight.add( node_idn )
                to_fire.append( ( node_idn, row, inputs, dispatch_corr_id ) )
            #/for row in self._nodes

            if not to_fire and not self.in_flight:
                missing = [ t for t in self.cell.target if resolved[ t ] is _UNRESOLVED ]
                self.future.set_exception( RuntimeError(
                    f"Cell { self.cell.main_id!r } cannot resolve target(s) { missing!r } "
                    f"-- no registered node is ready to produce them"
                ) )
                return
        #/with self.lock

        for node_idn, row, inputs, dispatch_corr_id in to_fire:
            self._dispatch( node_idn, row, inputs, dispatch_corr_id )
    #/def _scan

    def _dispatch(
        self:               Self,
        node_idn:           str,
        row:                dict,
        inputs:             tuple[ pl.DataFrame | None, ... ],
        dispatch_corr_id:  tuple[ str, ... ],
        ) -> None:
        actor = self.cell.tableOps[ node_idn ]
        # verbose >= 1, leaf TableOp children only ('op', not 'cell') -- a
        # nested Cell already prints its own enter/exit trace from within
        # its own __call__ (self.verbose_prefix, passed to it unmodified
        # below, already carries this Cell's own main_id segment -- see
        # Cell.__call__'s child_prefix), so printing here too would double
        # up. A leaf op's own verbose_prefix DOES get node_idn appended
        # (op_prefix, not self.verbose_prefix) -- unlike a nested Cell, a
        # TableOp has no main_id of its own to append it, so this is the
        # only place its op_idn ever enters the printed path; any message
        # the op itself prints (via its own verbose_prefix) is then
        # already correctly scoped to "this specific op", not just "this
        # Cell".
        is_leaf_op = row[ 'kind' ] == 'op'
        op_prefix  = f"{ self.verbose_prefix }/{ node_idn }" if is_leaf_op else self.verbose_prefix
        if self.verbose >= 1 and is_leaf_op:
            print( f"{ op_prefix } { tuple( row[ 'source' ] or () ) }" )
        #
        fut = _EXECUTOR.submit(
            _call_actor, actor, inputs, dispatch_corr_id, self.verbose, op_prefix,
        )
        if self.verbose >= 1 and is_leaf_op:
            fut.add_done_callback(
                lambda f, op_prefix = op_prefix, row = row: print(
                    f"/{ op_prefix } { tuple( row[ 'target' ] or () ) }"
                )
            )
        #
        fut.add_done_callback(
            lambda f, node_idn = node_idn, row = row, dispatch_corr_id = dispatch_corr_id:
                self._on_done( f, node_idn, row, dispatch_corr_id )
        )
    #/def _dispatch

    def _on_done( self: Self, fut: Future, node_idn: str, row: dict, dispatch_corr_id: tuple[ str, ... ] ) -> None:
        with self.lock:
            self.in_flight.discard( node_idn )
        try:
            result = fut.result()
        except Exception as exc:
            with self.lock:
                if not self.future.done():
                    self.future.set_exception( exc )
            return
        targets = row[ 'target' ] or []
        with self.lock:
            for name, value in zip( targets, result ):
                if name == '_':
                    # Reserved discard target: the op still ran (side
                    # effects, if any, already happened) but this result is
                    # never stored, so nothing can ever depend on it and it
                    # never contributes to resolving anything.
                    continue
                # Every value is appended, including None -- a null token
                # is an ordinary result a downstream consumer can inspect
                # (e.g. for loop termination), not a special engine-level
                # case.
                self.logs.setdefault( name, [] ).append(
                    Token( corr_id = dispatch_corr_id, value = value )
                )
            #/for name, value
        self._scan()
    #/def _on_done
#/class _WiringEngine


class Cell:
    """
    A stateful compute unit with a fixed set of system tables that every
    instance carries. Conforms to the Actor protocol (actor.py) -- a Cell may
    nest other Cells (and raw TableOps) as entries in its own tableOps, so
    composition doesn't need a separate Web class (nested-gathering-
    russell.md §1.6).

    __call__ is a declarative wiring engine, not hand-written per Cell (see
    _WiringEngine): source/target names, assigned when each node is
    registered, determine routing -- including fan-out (one target feeding
    several nodes' source) and fan-in (a node whose source lists several
    names, each independently produced) -- and the graph need not be a DAG;
    loops are expressible (see _WiringEngine's docstring). A Cell's own
    external contract is self.source/self.target: names its incoming dfs
    are seeded under, and which resolved names (in order) it returns. These
    are private to this Cell -- a parent registering this Cell as one of
    its own nodes assigns independent node-level source/target names for
    it; only arity need match across that boundary, never the names
    themselves. Cell no longer manages any domain data of its own -- every
    table originates from some Actor's __call__, produced by execution, not
    pulled from a persistent store Cell maintains (squishy-sleeping-
    snowflake.md).

    System tables (always present, named with dunder convention):
        __tables_schema__         — schema registry; one row per table
                                    (table_id, column_names, column_types)
        __nodes__                 — declarative wiring: which named
                                    children exist, whether each is a leaf
                                    op or a nested Cell, and their
                                    parent-assigned source/target names
        __table_op_schema__       — per-column schema declarations for registered tableOps
        __table_op_effects__      — side-effect declarations for registered tableOps
        __table_ops_parameters__  — TableOp construction-parameter forest (always
                                    empty for now; see configTree.py)

    All system-table schemas are themselves rows in __tables_schema__, so the
    table is self-describing from construction.

    Extension:
        register_tableOps(ops)  — merge ops into self.tableOps and update
                                   __table_op_schema__ / __table_op_effects__.
        register_node(node_idn, source, target) — declare a __nodes__ row for
                                   an already-registered tableOps entry.
    """

    def __init__(
        self:       Self,
        main_id:    str,
        ops:        types.TableProcessDict | None = None,
        verbose:    int                        = 0,
        source:     Sequence[ str ]             = (),
        target:     Sequence[ str ]             = (),
        input:      list[ tableOps.InputSchema ]  | None = None,
        output:     list[ tableOps.OutputSchema ] | None = None,
        parameters: pl.DataFrame | None            = None,
        resources:  pl.DataFrame | None            = None,
        ) -> None:
        self.main_id:   str                   = main_id
        self.verbose:   int                   = verbose
        self.tableOps:  types.TableProcessDict = {}
        # A resource manifest (table_index-shaped, see ReadWriteOps.py) --
        # held as a top-level attribute of every Cell. This is already the
        # fully-resolved OUTPUT of this Cell's own construction-time
        # caller-wins merge of its declared [resources] defaults and
        # whatever override its instantiating parent explicitly supplied
        # (see _cell_from_tree/configTree.py's [resources]/[nodes.resources]
        # and ReadWriteOps.merge_resources) -- NOT itself forwarded to any
        # nested child Cell automatically. Opaque to core Cell itself
        # (which schema/columns it has is a caller concern, e.g.
        # ReadWriteOps.py's table_index) -- just carried, not interpreted.
        self.resources: pl.DataFrame | None = resources
        # This Cell's own external binding -- private to this Cell, not the
        # same as the source/target a parent assigns when registering this
        # Cell as one of ITS nodes. Replaces the old single op_idn (Static
        # Execution Binding is still upheld: this binding is fixed at
        # construction, never re-decided per call -- it's just no longer
        # restricted to "exactly one child, called directly").
        self.source: tuple[ str, ... ] = tuple( source )
        self.target: tuple[ str, ... ] = tuple( target )
        # Declared Actor interface (actor.py) -- what __call__ consumes/
        # produces, same params/semantics as TableOp.__init__ already has.
        # This is the "genuine interface, not routing" that replaced Web's
        # declare()/NodeDecl exposure machinery (nested-gathering-
        # russell.md §1.6); may be left None, same as an undeclared TableOp.
        self.input:  list[ tableOps.InputSchema ]  | None = input
        self.output: list[ tableOps.OutputSchema ] | None = output

        # -- Initialise system tables
        self.tables: dict[ str, pl.DataFrame ] = {
            '__nodes__':                 pl.DataFrame( schema = table_schemas[ '__nodes__'           ] ),
            '__table_op_schema__':       pl.DataFrame( schema = table_schemas[ '__table_op_schema__'  ] ),
            '__table_op_effects__':      pl.DataFrame( schema = table_schemas[ '__table_op_effects__' ] ),
            # TableOp construction-parameter forest -- see configTree.py's
            # module docstring. Always empty for now; no parameter sources
            # exist yet.
            '__table_ops_parameters__':  (
                parameters if parameters is not None
                else pl.DataFrame( schema = _SYSTEM_SCHEMAS[ '__table_ops_parameters__' ] )
            ),
            # __tables_schema__ bootstraps itself — its own schema is one of the rows
            '__tables_schema__':         table_schemas_to_df( _SYSTEM_SCHEMAS ),
        }
        # __table_op_schema__ is now bdd's (op_idn, direction, index)-rooted
        # expression forest -- node_idx must stay globally unique across
        # every register_tableOps() call this Cell instance ever makes, so
        # the counter lives here, not reset per call (same threading
        # convention as configTree.py/ParameterParser.to_polars()).
        self._next_schema_node_idx: int = 0

        if ops:
            self.register_tableOps( ops )
        #/if ops
    #/def __init__


    # -- Actor protocol

    def __call__(
        self:           Self,
        dfs:            tuple[ pl.DataFrame, ... ],
        corr_id:        tuple[ str, ... ] = (),
        verbose:        int = 0,
        verbose_prefix: str = '',
        ) -> Future[ tuple[ pl.DataFrame, ... ] ]:
        """
        Actor interface: run this Cell's declarative wiring graph against
        dfs, seeded under self.source's names, returning a Future that
        resolves to the values collected under self.target's names, in
        order. Never blocks -- see _WiringEngine.

        corr_id is what every externally-seeded input is tagged with (see
        _WiringEngine.seed) -- if not given (the top-level/external-call
        case), a fresh id is minted here. Otherwise it's used AS GIVEN,
        unchanged -- a nested Cell relays whatever corr_id its parent's
        _dispatch gave it, rather than unconditionally extending it with a
        further segment of its own. corr_id is purely a tracing/
        correlation aid, not load-bearing for correctness -- see
        _WiringEngine's docstring for why pairing across a node's several
        source ports doesn't need one. Concurrency isolation between
        invocations of the same Cell doesn't depend on corr_id's
        uniqueness either -- a fresh _WiringEngine (its own logs/cursor
        dicts) is constructed every call regardless.
        """
        if len( dfs ) != len( self.source ):
            raise ValueError(
                f"Cell { self.main_id!r } expects { len( self.source ) } input(s) "
                f"{ self.source!r }, got { len( dfs ) }"
            )
        #
        if not corr_id:
            corr_id = ( str( uuid7() ), )
        #

        my_id = corr_id

        # verbose >= 1: enter/exit trace, path-style (verbose_prefix built
        # up one '/main_id' segment per nesting level) -- child_prefix is
        # what every node this Cell dispatches (leaf TableOp or nested
        # Cell) receives as ITS OWN verbose_prefix, so the path keeps
        # growing one level per Cell/op boundary crossed. No leading '/'
        # at the root (verbose_prefix == '') -- only a '/' *joining* two
        # segments, or the single '/' prefixed onto the whole path to mark
        # an exit (see below), ever appears. Bare tuples, no 'source:'/
        # 'target:' labels -- the enter/exit shape (no leading '/' vs
        # leading '/') already says which one it is.
        child_prefix = self.main_id if not verbose_prefix else f"{ verbose_prefix }/{ self.main_id }"
        if verbose >= 1:
            print( f"{ child_prefix } { self.source }" )
        #

        engine = _WiringEngine( self, my_id, verbose, child_prefix )
        if verbose >= 1:
            engine.future.add_done_callback(
                lambda fut: print( f"/{ child_prefix } { self.target }" )
            )
        #
        engine.seed( dict( zip( self.source, dfs ) ) )
        return engine.future
    #/def __call__

    def bind( self: Self, op_idn: str ) -> Actor:
        """
        Resolve op_idn to a callable Actor, fixed once per the Static
        Execution Binding invariant (nested-gathering-russell.md §1.5): a
        dict lookup on self.tableOps, nothing more. A registered entry
        (TableOp, nested Cell, or Ray actor handle) is a child this Cell was
        constructed with. The returned callable always returns a Future,
        uniformly, whether the underlying target is a synchronous TableOp
        (adapted via thread-pool submission), a nested Cell (already
        native), or a Ray actor handle -- callers can't tell the difference
        and never block waiting for it.
        """
        if op_idn not in self.tableOps:
            raise KeyError( f"{ op_idn!r } is not a registered tableOp on this Cell" )

        target = self.tableOps[ op_idn ]

        def _bound(
            dfs:            tuple[ pl.DataFrame, ... ],
            corr_id:        tuple[ str, ... ] = (),
            verbose:        int = 0,
            verbose_prefix: str = '',
            ) -> Future[ tuple[ pl.DataFrame, ... ] ]:
            return _EXECUTOR.submit( _call_actor, target, dfs, corr_id, verbose, verbose_prefix )
        #/def _bound

        return _bound
    #/def bind

    def to_dataframes( self: Self ) -> dict[ str, object ]:
        """
        Serialize this Cell's node structure and domain table data.

        tableOps are intentionally excluded: the destination reconstructing
        a Cell is assumed to already have the necessary op library and
        supplies it directly via Cell(..., ops=...), same as __init__
        already requires -- no content-addressed reconstruction.
        """
        domain_tables = {
            k: v for k, v in self.tables.items() if k not in _SYSTEM_SCHEMAS
        }
        return {
            'main_id':    self.main_id,
            'source':     list( self.source ),
            'target':     list( self.target ),
            'nodes':      self.tables[ '__nodes__' ],
            'parameters': self.tables[ '__table_ops_parameters__' ],
            'tables':     domain_tables,
            'verbose':    self.verbose,
        }
    #/def to_dataframes


    # -- Registration

    def register_tableOps(
        self: Self,
        ops: types.TableProcessDict,
        ) -> None:
        """
        Merge `ops` into self.tableOps and update __table_op_schema__ /
        __table_op_effects__ for ops that expose input / output / effects.
        """
        self.tableOps |= ops

        # Check for schema
        _schema_rows:  list[ pl.DataFrame ] = []
        _effects_rows: list[ pl.DataFrame ] = []
        for op_id, op in ops.items():
            _has_schema  = getattr( op, 'input', None ) is not None or getattr( op, 'output',  None ) is not None
            _has_effects = getattr( op, 'effects', None ) is not None
            if _has_schema:
                _schema_df, _effects_df, self._next_schema_node_idx = TableOpSchema(
                    inputs  = op.input  or [],
                    outputs = op.output or [],
                    effects = getattr( op, 'effects', None ) or [],
                ).to_polars( op_id, node_idx_start = self._next_schema_node_idx )
                _schema_rows.append( _schema_df )
                if not _effects_df.is_empty():
                    _effects_rows.append( _effects_df )
            elif _has_effects and op.effects:
                _, _effects_df, self._next_schema_node_idx = TableOpSchema(
                    inputs = [], outputs = [], effects = op.effects,
                ).to_polars( op_id, node_idx_start = self._next_schema_node_idx )
                _effects_rows.append( _effects_df )
            #/if _has_schema/elif _has_effects and op.effects
        #/for op_id, op in ops.items()

        _new_schema  = (
            pl.concat( _schema_rows,  how = 'vertical' )
            if _schema_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_schema__' ] )
        )
        _new_effects = (
            pl.concat( _effects_rows, how = 'vertical' )
            if _effects_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_effects__' ] )
        )

        self.tables[ '__table_op_schema__'  ] = pl.concat(
            [ self.tables[ '__table_op_schema__'  ], _new_schema  ], how = 'vertical',
        )
        self.tables[ '__table_op_effects__' ] = pl.concat(
            [ self.tables[ '__table_op_effects__' ], _new_effects ], how = 'vertical',
        )
    #/def register_tableOps

    def _replace_actor( self: Self, node_idn: str, actor: Actor ) -> None:
        """
        Swap the actor bound to an already-registered node_idn for another
        one, in place -- unlike register_tableOps (an additive merge meant
        for first-time registration), this also drops node_idn's existing
        __table_op_schema__/__table_op_effects__ rows before re-deriving
        them from the new actor's own input/output/effects, so no stale
        rows from the old actor linger alongside the new ones.

        Sunder for now -- no public replace-actor API decided yet (dict
        swap + wiring-row survival semantics still need a caller-facing
        contract); the wiring row itself (__nodes__: source/target/kind)
        is untouched, since only which actor answers node_idn is changing,
        not how it's wired.

        _WiringEngine._dispatch re-reads self.cell.tableOps[node_idn] fresh
        on every firing (no cached bind()), so this takes effect on
        node_idn's next dispatch -- it is NOT synchronized against a
        firing already in flight for node_idn.
        """
        if node_idn not in self.tableOps:
            raise KeyError( f"{ node_idn!r } is not a registered tableOp on this Cell" )

        self.tableOps[ node_idn ] = actor

        self.tables[ '__table_op_schema__'  ] = self.tables[ '__table_op_schema__'  ].filter(
            pl.col( 'op_idn' ) != node_idn
        )
        self.tables[ '__table_op_effects__' ] = self.tables[ '__table_op_effects__' ].filter(
            pl.col( 'op_idn' ) != node_idn
        )

        _has_schema  = getattr( actor, 'input', None ) is not None or getattr( actor, 'output',  None ) is not None
        _has_effects = getattr( actor, 'effects', None ) is not None
        if _has_schema:
            _schema_df, _effects_df, self._next_schema_node_idx = TableOpSchema(
                inputs  = actor.input  or [],
                outputs = actor.output or [],
                effects = getattr( actor, 'effects', None ) or [],
            ).to_polars( node_idn, node_idx_start = self._next_schema_node_idx )
            self.tables[ '__table_op_schema__'  ] = pl.concat(
                [ self.tables[ '__table_op_schema__'  ], _schema_df  ], how = 'vertical',
            )
            if not _effects_df.is_empty():
                self.tables[ '__table_op_effects__' ] = pl.concat(
                    [ self.tables[ '__table_op_effects__' ], _effects_df ], how = 'vertical',
                )
            #/if not _effects_df.is_empty()
        elif _has_effects and actor.effects:
            _, _effects_df, self._next_schema_node_idx = TableOpSchema(
                inputs = [], outputs = [], effects = actor.effects,
            ).to_polars( node_idn, node_idx_start = self._next_schema_node_idx )
            self.tables[ '__table_op_effects__' ] = pl.concat(
                [ self.tables[ '__table_op_effects__' ], _effects_df ], how = 'vertical',
            )
        #/if _has_schema/elif _has_effects and actor.effects
    #/def _replace_actor

    def _rewire( self: Self, node_idn: str, target: Sequence[ str ] ) -> Future[ None ]:
        """
        FUTURE -- not implemented yet; always raises NotImplementedError.

        Change node_idn's own __nodes__ row so its RESULTS get sent to a
        new constant set of destination names, live, without reconstructing
        the Cell -- the "except when rewiring to another set of constant
        destinations" carve-out on top of this engine's usual invariant
        that an autonomous actor's output destinations are fixed (see
        _WiringEngine's docstring: point-to-point ports, not a shared
        blackboard). Only ever changes WHERE node_idn's results are wired
        to (its `target`); which actor answers node_idn is a separate,
        already-implemented concern (_replace_actor), and node_idn's own
        `source` ports are untouched -- a rewire is a producer-side event.

        Returns a Future, not None synchronously, because rewiring an
        already-running node is not an instantaneous swap: the intended
        (not yet built) sequence is (1) pause node_idn's own future
        dispatches, so no new firing starts mid-rewire (2) let any firing
        already in flight, plus anything already queued for node_idn's own
        source ports, finish flushing through under the OLD `target` (3)
        atomically replace node_idn's __nodes__ row's `target` with the new
        destination names (4) resume dispatch. The returned Future resolves
        once step 4 completes -- callers that need the swap to have fully
        taken effect await it; callers that don't can fire-and-forget, same
        as any other Actor call in this codebase.

        No node_idn-in-self.tableOps check yet either -- deferred along
        with everything else here until the real implementation lands.
        """
        raise NotImplementedError(
            "FUTURE: Cell._rewire is a stub -- live rewiring of a node's "
            "constant output destinations (pause -> drain -> swap -> "
            "resume) is not implemented yet"
        )
    #/def _rewire

    def register_node(
        self:         Self,
        node_idn:     str,
        source:       Sequence[ str ] = (),
        target:       Sequence[ str ] = (),
        parent_idn:   str | None      = None,
        join:         str             = 'all',
        join_k:       int | None      = None,
        ) -> None:
        """
        Declare a __nodes__ row for node_idn, an entry already registered in
        self.tableOps (via register_tableOps or the ops= constructor param).
        kind ('op' vs 'cell') is inferred from the tableOps entry's type, not
        passed explicitly -- there's nothing to get wrong.

        target names need not be unique across nodes -- the wiring graph
        isn't required to be a DAG (see _WiringEngine); more than one node
        may legitimately declare the same target, each write simply
        appending another token to that name's queue. This also means "_"
        (the reserved discard target -- see _WiringEngine) needs no
        special-casing here: any number of nodes may independently declare
        target=["_"] with no conflict, same as any other repeated name.

        join/join_k: this node's firing rule over its declared source
        ports -- see _WiringEngine._scan/tableProcesses.make_node_row for
        the exact semantics/validation. Default ('all', None) reproduces a
        strict AND-join -- today's only behavior before join/join_k were
        added.
        """
        if node_idn not in self.tableOps:
            raise KeyError(
                f"{ node_idn!r } must be registered via register_tableOps "
                f"(or Cell(..., ops=...)) before register_node"
            )
        kind = 'cell' if isinstance( self.tableOps[ node_idn ], Cell ) else 'op'
        self.tables[ '__nodes__' ] = pl.concat(
            [
                self.tables[ '__nodes__' ],
                tableProcesses.make_node_row(
                    node_idn   = node_idn,
                    kind       = kind,
                    source     = source,
                    target     = target,
                    parent_idn = parent_idn,
                    join       = join,
                    join_k     = join_k,
                ),
            ],
            how = 'vertical',
        )
    #/def register_node

    def validate_nodes( self: Self ) -> None:
        """Validate every __nodes__ row's node_idn is a registered tableOps entry."""
        for row in self.tables[ '__nodes__' ].to_dicts():
            if row[ 'node_idn' ] not in self.tableOps:
                raise KeyError( f"__nodes__ row {row[ 'node_idn' ]!r} has no matching tableOps entry" )
    #/def validate_nodes

    @classmethod
    def merge(
        cls,
        a:       Self,
        b:       Self,
        main_id: str                 = '',
        source:  Sequence[ str ] | None = None,
        target:  Sequence[ str ] | None = None,
        ) -> Self:
        """
        Produce a new Cell combining a and b.
        main_id is '{a.main_id}_{b.main_id}'.
        source/target: the merged Cell's own binding -- a's and b's aren't
            inherently compatible to combine automatically, so this
            defaults to a's if not given explicitly.
        tableOps: from both (b overwrites a on conflict).
        __nodes__: rows from both; on node_idn conflict b wins.
        __table_ops_parameters__: rows from both, concatenated (no dedup --
            a forest, not keyed by any single column shared across
            independently-sourced trees; see configTree.py's module
            docstring).
        resources: a's if present, else b's -- a simple presence-wins pick,
            not another caller-wins override pass. a/b are already
            fully-constructed Cells, so each .resources is already the
            resolved OUTPUT of its own construction-time merge (see
            Cell.__init__) -- there's no "instantiating parent" relationship
            between merge's a/b the way there is between a Cell and
            whoever built it, so a second override pass wouldn't have a
            principled winner to pick beyond this.
        Domain tables (non-system keys): a's, then b's (b overwrites a).
        """
        merged = cls(
            main_id   = main_id or f"{ a.main_id }_{ b.main_id }",
            source    = source if source is not None else a.source,
            target    = target if target is not None else a.target,
            resources = a.resources if a.resources is not None else b.resources,
        )

        ops: types.TableProcessDict = dict( a.tableOps ) | dict( b.tableOps )
        if ops:
            merged.register_tableOps( ops )
        #

        combined = pl.concat( [ a.tables[ '__nodes__' ], b.tables[ '__nodes__' ] ], how = 'vertical' )
        if not combined.is_empty():
            deduped = combined.unique( subset = [ 'node_idn' ], keep = 'last' )
            merged.tables[ '__nodes__' ] = deduped
        #

        merged.tables[ '__table_ops_parameters__' ] = pl.concat(
            [ a.tables[ '__table_ops_parameters__' ], b.tables[ '__table_ops_parameters__' ] ],
            # diagonal_relaxed: a's and b's parameter forests may have been
            # built with independently-registered dtypes (configTree.py's
            # [[parameters]] entries each infer their own), so their
            # value-column sets can differ.
            how = 'diagonal_relaxed',
        )

        _system_keys = set( _SYSTEM_SCHEMAS )
        for k, v in a.tables.items():
            if k not in _system_keys:
                merged.tables[ k ] = v
            #/if k not in _system_keys
        #/for k, v in a.tables.items()

        for k, v in b.tables.items():
            if k not in _system_keys:
                merged.tables[ k ] = v
            #/if k not in _system_keys
        #/for k, v in b.tables.items()

        return merged
    #/def merge

    def print_nodes( self: Self, verbose_prefix: str = '' ) -> None:
        """Print every registered __nodes__ row: its kind, source, and target."""
        for row in self.tables[ '__nodes__' ].to_dicts():
            print( f"{ verbose_prefix }[{ row[ 'node_idn' ] }] { row[ 'kind' ] }" )
            if row[ 'source' ]:
                print( f"{ verbose_prefix }  source: { tuple( row[ 'source' ] ) }" )
            if row[ 'target' ]:
                print( f"{ verbose_prefix }  target: { tuple( row[ 'target' ] ) }" )
    #/def print_nodes
#/class Cell


def build_tableOps(
    op_classes: dict[ str, type[ TableOp ] ],
    op_keys:    Sequence[ str ],
    rng:        np.random.Generator | None,
    parameters: pl.DataFrame,
    op_types:   dict[ str, str ] | None = None,
    resources:  pl.DataFrame | None     = None,
    ) -> types.TableProcessDict:
    """
    Construct one TableOp instance per op_keys entry:
    op_classes[class_key](rng=rng, **kwargs), where kwargs comes from
    op_ctor_kwargs_forest(parameters).get(op_idn, {}) -- {} for any op with
    no declared parameter node, relying entirely on the class's own
    defaults (or a TypeError for a missing required kwarg, same as any
    ordinary Python call -- no separate validation here). class_key is
    op_types.get(op_idn, op_idn) -- op_idn itself (today's exact behavior)
    unless op_types (configTree.py's per-node op_type column, decoupling
    "which class to build" from op_idn's own wiring/dispatch identity) maps
    it elsewhere. kwargs stay keyed by op_idn (each node's own distinct
    ctor kwargs), never by class_key -- two nodes sharing an op_type have
    no reason to share ctor kwargs too.

    resources (this Cell's own resolved resource manifest -- see
    Cell.resources) is passed as a ctor kwarg ONLY to classes whose own __init__ declares a
    `resources` parameter (checked via inspect.signature) -- unlike rng,
    which every TableOp already accepts unconditionally, most existing ops
    have no use for resources at all, so this stays opt-in rather than
    forcing every TableOp subclass in existence to grow an unused
    parameter (see silverbrain.ReadWriteOps.ReadOp/WriteOp/ReadWriteOp for
    the ops that do want it).

    Replaces the old "a series of lambdas" pattern (an application
    hand-writing a get_ops(keys, rng) function with one branch per op_idn)
    with a plain class registry + this one generic instantiation rule.
    op_classes[class_key] not existing raises KeyError, same failure mode
    as the old hook silently producing an incomplete ops dict.
    """
    kwargs_by_op = op_ctor_kwargs_forest( parameters )
    op_types     = op_types or {}
    ops: types.TableProcessDict = {}
    for op_idn in op_keys:
        cls    = op_classes[ op_types.get( op_idn, op_idn ) ]
        kwargs = dict( kwargs_by_op.get( op_idn, {} ) )
        if 'resources' in inspect.signature( cls.__init__ ).parameters:
            kwargs.setdefault( 'resources', resources )
        ops[ op_idn ] = cls( rng = rng, **kwargs )
    return ops
#/def build_tableOps


def _cell_from_tree(
    tree:       pl.DataFrame,
    node_idn:   str,
    op_classes: dict[ str, type[ TableOp ] ],
    rng:        np.random.Generator | None,
    verbose:    int,
    parameters: pl.DataFrame,
    declared_resources: dict[ str, dict[ str, pl.DataFrame | None ] ] | None = None,
    ) -> Cell:
    """
    Construct one Cell (and, recursively, its nested Cell children) from a
    flat config_tree dataframe (configTree.py) -- the topology (who's a
    child of whom, op vs cell, each node's own parent-assigned source/
    target, each cell's own external source/target binding) comes entirely
    from the tree, already fully parsed by configTree. No file access
    happens here at all -- Cell no longer manages any domain data of its
    own (squishy-sleeping-snowflake.md), so there's no remaining
    non-topology metadata to read.

    parameters is the whole TableOp construction-parameter forest parsed by
    configTree alongside the tree, threaded unchanged to every level -- it's
    a forest (independent per-op trees, no cross-tree resolution needed), so
    every Cell can safely hold the same shared frame rather than needing a
    per-Cell slice. Currently always empty (no TOML syntax populates it
    yet); build_tableOps()/op_ctor_kwargs_forest() already handle a
    non-empty one, ahead of that syntax landing.

    declared_resources (configTree.py's resources_map -- see its
    [resources]/[nodes.resources] docs) is threaded UNCHANGED to every
    level, exactly like parameters -- but, unlike parameters, each level
    only ever reads its OWN node_idn's two slots ('own': this Cell's own
    declared [resources] defaults; 'override': whatever its instantiating
    parent explicitly bound for it via [nodes.resources]) and resolves them
    via merge_resources (caller/override wins on a clashing table_idn).
    This Cell's OWN resolved Cell.resources is never itself forwarded to
    any child -- a nested Cell only ever sees resources through its own
    declared defaults plus its own parent's explicit override slot in
    declared_resources; there is no automatic inheritance beyond that.
    """
    own_row = tree.filter(
        ( pl.col( 'node_idn' ) == node_idn ) & ( pl.col( 'kind' ) == 'cell' )
    ).row( 0, named = True )
    children             = tree.filter( pl.col( 'parent_idn' ) == node_idn )
    op_children          = children.filter( pl.col( 'kind' ) == 'op'          ).to_dicts()
    placeholder_children = children.filter( pl.col( 'kind' ) == 'placeholder' ).to_dicts()
    cell_children        = children.filter( pl.col( 'kind' ) == 'cell'        ).to_dicts()

    _decl = ( declared_resources or {} ).get( node_idn, {} )
    effective_resources = merge_resources( _decl.get( 'own' ), _decl.get( 'override' ) )

    op_keys: list[ str ] = [ row[ 'node_idn' ] for row in op_children ]
    op_types: dict[ str, str ] = {
        row[ 'node_idn' ]: row[ 'op_type' ] or row[ 'node_idn' ]
        for row in op_children
    }
    ops: types.TableProcessDict = build_tableOps( op_classes, op_keys, rng, parameters, op_types, effective_resources )

    for row in placeholder_children:
        # configTree.py already fully resolved every atom's dtype before
        # storing (exprParse.expr_to_text always emits them explicitly),
        # so this parse is pure deserialization -- never expected to hit
        # the inference-error path.
        expr = exprParse.parse_expr( row[ 'placeholder_columns' ] )
        ops[ row[ 'node_idn' ] ] = PlaceholderOp( expr = expr, rng = rng )

    for row in cell_children:
        ops[ row[ 'node_idn' ] ] = _cell_from_tree(
            tree               = tree,
            node_idn           = row[ 'node_idn' ],
            op_classes         = op_classes,
            rng                = rng,
            verbose            = verbose,
            parameters         = parameters,
            # Unchanged, not effective_resources -- a child only ever sees
            # resources through its OWN 'own'/'override' slots in this same
            # dict, never through what this level resolved for itself. This
            # is what enforces "no automatic inheritance" (see docstring).
            declared_resources = declared_resources,
        )

    cell = Cell(
        main_id    = node_idn,
        ops        = ops,
        verbose    = verbose,
        source     = own_row[ 'cell_source' ] or [],
        target     = own_row[ 'cell_target' ] or [],
        parameters = parameters,
        resources  = effective_resources,
    )

    for row in op_children + placeholder_children:
        cell.register_node(
            node_idn = row[ 'node_idn' ], source = row[ 'source' ], target = row[ 'target' ],
            join     = row[ 'join' ] or 'all',
            join_k   = row[ 'join_k' ],
        )
    for row in cell_children:
        if row[ 'source' ] or row[ 'target' ]:
            cell.register_node(
                node_idn = row[ 'node_idn' ], source = row[ 'source' ], target = row[ 'target' ],
                join     = row[ 'join' ] or 'all',
                join_k   = row[ 'join_k' ],
            )

    return cell
#/def _cell_from_tree


def cell_from_file(
    path: str | Path,
    op_classes: dict[ str, type[ TableOp ] ],
    main_id: str,
    rng: np.random.Generator | None = None,
    verbose: int = 0,
    template_vars: dict[ str, str ] | None = None,
    app_name: str | None = None,
    fallback_data_roots: dict[ str, Path ] | None = None,
    cells_index_file: Path = configTree.CELLS_INDEX_FILE_DEFAULT,
    resources: pl.DataFrame | None = None,
) -> Cell:
    """
    Construct a Cell from a config file, delegating to configTree.config_tree
    to recursively parse the file and its transitive kind='cell' [[nodes]]
    references into a flat initialization table (topology + each cell's own
    binding), then building nested Cells from that table bottom-up. File
    format is inferred from suffix at every node in the walk
    (configTree.config_tree); both '.toml' and '.json' cell maps are
    accepted, and a config's children may mix formats freely.

    Parameters
    ----------
    path        : path to the config file
    op_classes  : dict[str, type[TableOp]] mapping each kind='op' [[nodes]]
                  node_idn to the TableOp subclass to construct for it. Each
                  instance is built via build_tableOps() as
                  cls(rng=rng, **kwargs) -- kwargs from the op's own node in
                  the __table_ops_parameters__ forest (empty for now; no
                  TOML syntax populates it yet), so an op with no declared
                  parameters just gets `cls(rng=rng)`, relying on its own
                  defaults (or a TypeError for a missing required kwarg).
                  kind='placeholder' nodes don't consult op_classes at all --
                  they're built directly as tableOps.PlaceholderOp(columns=...).
    main_id     : passed to ``Cell.__init__`` and used as this file's own
                  node_idn in the parsed tree
    rng         : forwarded to every constructed TableOp, at every level
    verbose     : forwarded to ``Cell.__init__`` at every level
    template_vars : optional ``{key}`` substitutions applied to kind='cell'
                  ``toml`` strings before resolving (e.g. ``{cells_root}``)
    app_name/fallback_data_roots/cells_index_file : only needed if some
                  kind='cell' [[nodes]] entry (anywhere in the transitive
                  walk) uses ``cell_idn`` instead of ``toml`` -- resolved via
                  bundle.resolve_map_fp()'s maps_index.toml search. Omit for
                  a config that only ever uses ``toml``.
    resources   : a table_index-shaped resource manifest (see
                  ReadWriteOps.py) acting as the ROOT Cell's own
                  caller-override -- merged (caller-wins) against the root
                  cell's own declared [resources] TOML defaults, exactly
                  the same role a parent's [nodes.resources] block plays
                  for a nested kind='cell' instantiation (this Python
                  kwarg IS that override, for the outermost/no-parent
                  case). NOT forwarded to any nested child automatically --
                  a child only ever sees resources via its own [resources]
                  defaults plus its own parent's explicit
                  [nodes.resources] override.

    TOML schema expected
    --------------------
    [cell]
        class  = "..."       # informational only
        source = [...]       # this Cell's own external input binding --
                              # names its incoming dfs are seeded under
        target = [...]       # this Cell's own external output binding --
                              # which resolved names (in order) it returns.
                              # Both optional; a Cell with neither declared
                              # only reaches its children via bind(), never
                              # through its own __call__'s wiring engine.

    [[nodes]]                    # one block per node this file directly owns
        node_idn = "..."          # unique among this file's own nodes
        kind     = "op"           # "op" | "cell" | "placeholder"
        source   = [...]          # names this node's positional inputs are
                                   # read from (this Cell's own namespace)
        target   = [...]          # names this node's positional outputs are
                                   # written under (ditto) -- need not be
                                   # unique across [[nodes]] entries; see
                                   # squishy-sleeping-snowflake.md

        # kind="op": node_idn must be a key op_classes resolves; no other
        #   fields required.

        # kind="cell": one block per nested Cell -- exactly one of:
        #   toml     = "path/to/child.toml"   # relative to this file
        #   cell_idn = "some-cell-idn"        # resolved via bundle.
        #                                      # resolve_map_fp() (requires
        #                                      # app_name to be supplied)
        #   node_idn is the key this child is registered under in the
        #   parent's tableOps; source/target are optional -- omit both to
        #   leave this child unregistered as a __nodes__ row (still
        #   reachable via bind()).

        # kind="placeholder": interface known, implementation not bound yet --
        #   columns = { col_name = "DtypeStr", ... }   # e.g. { a = "Int64" }
        #   Builds a tableOps.PlaceholderOp(columns=...) (arity-1
        #   input/output, raises if actually called); swap
        #   cell.tableOps[node_idn] for a real TableOp/Cell before this node
        #   is reached on a live execution path.

    [resources]                  # this cell's own declared resources
        _json = "path/to/resources.json"   # relative to this file -- or,
                                            # instead (never both):
    [[resources.items]]
        table_idn = "..."
        kind      = "local"      # optional, defaults "local"
        path      = "..."        # or db_path/table_name/connection_string/
                                  # schema_name, whichever 'kind' needs

        # A kind='cell' [[nodes]] entry may carry its own [nodes.resources]
        # (same _json/items grammar) -- the caller-override this specific
        # instantiation explicitly binds for that child; wins over the
        # child's own declared defaults on a clashing table_idn. See
        # configTree.py's module docstring for the full semantics.
    """
    tree, parameters, _, declared_resources = configTree.config_tree(
        path                = path,
        node_idn            = main_id,
        template_vars       = template_vars,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file     = cells_index_file,
    )
    if resources is not None:
        declared_resources = dict( declared_resources )
        declared_resources[ main_id ] = {
            **declared_resources.get( main_id, {} ),
            'override': resources,
        }
    return _cell_from_tree(
        tree               = tree,
        node_idn           = main_id,
        op_classes         = op_classes,
        rng                = rng,
        verbose            = verbose,
        parameters         = parameters,
        declared_resources = declared_resources,
    )
#/def cell_from_file
