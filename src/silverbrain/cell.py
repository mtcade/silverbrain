#
#//  cell.py
#//  silverbrain
#//

from . import configTree, tableOps, tableProcesses, types
from .actor import Actor
from .tableOps import TableOp, TableOpSchema
from .parameterParse import ParameterParser, op_ctor_kwargs_forest
from .schema import table_schemas, table_schemas_to_df

from concurrent.futures import Future, ThreadPoolExecutor
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


def _accepts_corr_id( actor ) -> bool:
    """Whether `actor`'s __call__ structurally accepts a corr_id kwarg.
    TableOp.__call__ doesn't (left unchanged, out of scope for this
    rearchitecture); Cell and any corr_id-aware Actor does."""
    try:
        return 'corr_id' in inspect.signature( actor ).parameters
    except ( TypeError, ValueError ):
        return False
#/def _accepts_corr_id


def _call_actor(
    actor,
    dfs:            tuple[ pl.DataFrame, ... ],
    corr_id:        tuple[ str, ... ],
    verbose:        int,
    verbose_prefix: str,
    ) -> tuple[ pl.DataFrame, ... ]:
    """
    Invoke a bound Actor. Always runs on a thread-pool worker (see
    _EXECUTOR), never the original caller's thread -- this is what makes
    __call__/bind() non-blocking from the caller's perspective. Passes
    corr_id only if the target structurally accepts it. If the target
    itself returns a Future (a nested Cell, or a Ray remote call already
    wrapped), blocks this worker thread on it rather than the caller -- a
    known simplification; see squishy-sleeping-snowflake.md.
    """
    if hasattr( actor, 'remote' ):
        import ray
        result = ray.get( actor.__call__.remote( dfs, corr_id, verbose, verbose_prefix ) )
    elif _accepts_corr_id( actor ):
        result = actor( dfs, corr_id = corr_id, verbose = verbose, verbose_prefix = verbose_prefix )
    else:
        result = actor( dfs, verbose = verbose, verbose_prefix = verbose_prefix )
    if isinstance( result, Future ):
        result = result.result()
    return result
#/def _call_actor


class _WiringEngine:
    """
    Runs one Cell.__call__ invocation's wiring graph: fires any registered
    node whose declared source names have all received a value since it
    last fired, feeding results forward by target name, until every name in
    the Cell's own declared target is available -- then resolves `future`
    with those values, in order.

    Not restricted to a DAG. The same node may fire more than once (a
    loop); each firing gets its own corr_id segment
    (root_id + "node_idn:n"), derived from a simple per-node fire counter,
    not from graph analysis. A null/None value written to a target does
    NOT count as a "new" round for downstream consumers -- their
    generation-tracking per source name only advances on non-null writes.
    This is how loop termination propagates: a controller node emitting
    null to one target and real data to another lets the null branch's
    consumers stop advancing while the real branch's consumers fire; the
    controller itself needs no internal state; "which iteration am I on"
    is carried entirely by the corr_id its own trigger arrived under and
    the fire-count-derived id of whatever it emits next.

    Deadlock is detected, not silently hung forever: if a scan finds
    nothing newly fireable and nothing still in flight, but the Cell's own
    target isn't fully resolved, `future` gets a RuntimeError naming the
    unresolved names.
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
        self.values: dict[ str, pl.DataFrame | None ] = {}
        self.gen:    dict[ str, int ]                  = {}
        self.seen:   dict[ str, dict[ str, int ] ]     = {}
        self.fire_count: dict[ str, int ]              = {}
        self.in_flight:  set[ str ]                    = set()
        self._nodes = cell.tables[ '__nodes__' ].to_dicts()
    #/def __init__

    def seed( self: Self, values: dict[ str, pl.DataFrame ] ) -> None:
        with self.lock:
            for k, v in values.items():
                self.values[ k ] = v
                self.gen[ k ] = self.gen.get( k, 0 ) + 1
        self._scan()
    #/def seed

    def _scan( self: Self ) -> None:
        to_fire: list[ tuple[ str, dict, tuple, tuple ] ] = []
        with self.lock:
            if self.future.done():
                return
            if all( t in self.values for t in self.cell.target ):
                self.future.set_result( tuple( self.values[ t ] for t in self.cell.target ) )
                return
            for row in self._nodes:
                node_idn = row[ 'node_idn' ]
                if node_idn in self.in_flight:
                    continue
                src = row[ 'source' ] or []
                if not src or not all( s in self.values for s in src ):
                    continue
                seen = self.seen.setdefault( node_idn, {} )
                if all( seen.get( s, 0 ) >= self.gen.get( s, 0 ) for s in src ):
                    continue
                for s in src:
                    seen[ s ] = self.gen.get( s, 0 )
                self.in_flight.add( node_idn )
                fire_no = self.fire_count.get( node_idn, 0 )
                self.fire_count[ node_idn ] = fire_no + 1
                node_corr_id = self.root_id + ( f'{ node_idn }:{ fire_no }', )
                to_fire.append( (
                    node_idn, row,
                    tuple( self.values[ s ] for s in src ),
                    node_corr_id,
                ) )
            #/for row in self._nodes

            if not to_fire and not self.in_flight:
                missing = [ t for t in self.cell.target if t not in self.values ]
                self.future.set_exception( RuntimeError(
                    f"Cell { self.cell.main_id!r } cannot resolve target(s) { missing!r } "
                    f"-- no registered node is ready to produce them"
                ) )
                return
        #/with self.lock

        for node_idn, row, inputs, node_corr_id in to_fire:
            self._dispatch( node_idn, row, inputs, node_corr_id )
    #/def _scan

    def _dispatch(
        self:         Self,
        node_idn:     str,
        row:          dict,
        inputs:       tuple[ pl.DataFrame, ... ],
        node_corr_id: tuple[ str, ... ],
        ) -> None:
        actor = self.cell.tableOps[ node_idn ]
        fut = _EXECUTOR.submit(
            _call_actor, actor, inputs, node_corr_id, self.verbose, self.verbose_prefix,
        )
        fut.add_done_callback(
            lambda f, node_idn = node_idn, row = row: self._on_done( f, node_idn, row )
        )
    #/def _dispatch

    def _on_done( self: Self, fut: Future, node_idn: str, row: dict ) -> None:
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
                self.values[ name ] = value
                if value is not None:
                    self.gen[ name ] = self.gen.get( name, 0 ) + 1
                #/if value is not None
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
        ) -> None:
        self.main_id:   str                   = main_id
        self.verbose:   int                   = verbose
        self.tableOps:  types.TableProcessDict = {}
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
        ) -> 'Future[ tuple[ pl.DataFrame, ... ] ]':
        """
        Actor interface: run this Cell's declarative wiring graph against
        dfs, seeded under self.source's names, returning a Future that
        resolves to the values collected under self.target's names, in
        order. Never blocks -- see _WiringEngine.

        corr_id, if not given (the top-level/external-call case), gets a
        fresh root segment minted here. Every Cell invocation -- this one
        included -- extends whatever corr_id it receives with its own
        further segment, so this Cell's internal joins can't cross-talk
        with a concurrent invocation of itself.
        """
        if len( dfs ) != len( self.source ):
            raise ValueError(
                f"Cell { self.main_id!r } expects { len( self.source ) } input(s) "
                f"{ self.source!r }, got { len( dfs ) }"
            )
        if not corr_id:
            corr_id = ( str( uuid7() ), )
        my_id = corr_id + ( str( uuid7() ), )

        engine = _WiringEngine( self, my_id, verbose, verbose_prefix )
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
            ) -> 'Future[ tuple[ pl.DataFrame, ... ] ]':
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
                _schema_df, _effects_df = TableOpSchema(
                    inputs  = op.input  or [],
                    outputs = op.output or [],
                    effects = getattr( op, 'effects', None ) or [],
                ).to_polars( op_id )
                _schema_rows.append( _schema_df )
                if not _effects_df.is_empty():
                    _effects_rows.append( _effects_df )
            elif _has_effects and op.effects:
                _, _effects_df = TableOpSchema(
                    inputs = [], outputs = [], effects = op.effects,
                ).to_polars( op_id )
                _effects_rows.append( _effects_df )
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

    def register_node(
        self:       Self,
        node_idn:   str,
        source:     Sequence[ str ] = (),
        target:     Sequence[ str ] = (),
        parent_idn: str | None      = None,
        ) -> None:
        """
        Declare a __nodes__ row for node_idn, an entry already registered in
        self.tableOps (via register_tableOps or the ops= constructor param).
        kind ('op' vs 'cell') is inferred from the tableOps entry's type, not
        passed explicitly -- there's nothing to get wrong.

        target names need not be unique across nodes -- the wiring graph
        isn't required to be a DAG (see _WiringEngine); more than one node
        may legitimately claim the same target across different rounds.
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
        Domain tables (non-system keys): a's, then b's (b overwrites a).
        """
        merged = cls(
            main_id = main_id or f"{ a.main_id }_{ b.main_id }",
            source  = source if source is not None else a.source,
            target  = target if target is not None else a.target,
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
    ) -> types.TableProcessDict:
    """
    Construct one TableOp instance per op_keys entry:
    op_classes[op_idn](rng=rng, **kwargs), where kwargs comes from
    op_ctor_kwargs_forest(parameters).get(op_idn, {}) -- {} for any op with
    no declared parameter node, relying entirely on the class's own
    defaults (or a TypeError for a missing required kwarg, same as any
    ordinary Python call -- no separate validation here).

    Replaces the old "a series of lambdas" pattern (an application
    hand-writing a get_ops(keys, rng) function with one branch per op_idn)
    with a plain class registry + this one generic instantiation rule.
    op_classes[op_idn] not existing raises KeyError, same failure mode as
    the old hook silently producing an incomplete ops dict.
    """
    kwargs_by_op = op_ctor_kwargs_forest( parameters )
    return {
        op_idn: op_classes[ op_idn ]( rng = rng, **kwargs_by_op.get( op_idn, {} ) )
        for op_idn in op_keys
    }
#/def build_tableOps


def _cell_from_tree(
    tree:       pl.DataFrame,
    node_idn:   str,
    op_classes: dict[ str, type[ TableOp ] ],
    rng:        np.random.Generator | None,
    verbose:    int,
    parameters: pl.DataFrame,
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
    """
    own_row = tree.filter(
        ( pl.col( 'node_idn' ) == node_idn ) & ( pl.col( 'kind' ) == 'cell' )
    ).row( 0, named = True )
    children      = tree.filter( pl.col( 'parent_idn' ) == node_idn )
    op_children   = children.filter( pl.col( 'kind' ) == 'op'   ).to_dicts()
    cell_children = children.filter( pl.col( 'kind' ) == 'cell' ).to_dicts()

    op_keys: list[ str ] = [ row[ 'node_idn' ] for row in op_children ]
    ops: types.TableProcessDict = build_tableOps( op_classes, op_keys, rng, parameters )

    for row in cell_children:
        ops[ row[ 'node_idn' ] ] = _cell_from_tree(
            tree       = tree,
            node_idn   = row[ 'node_idn' ],
            op_classes = op_classes,
            rng        = rng,
            verbose    = verbose,
            parameters = parameters,
        )

    cell = Cell(
        main_id    = node_idn,
        ops        = ops,
        verbose    = verbose,
        source     = own_row[ 'cell_source' ] or [],
        target     = own_row[ 'cell_target' ] or [],
        parameters = parameters,
    )

    for row in op_children:
        cell.register_node( node_idn=row[ 'node_idn' ], source=row[ 'source' ], target=row[ 'target' ] )
    for row in cell_children:
        if row[ 'source' ] or row[ 'target' ]:
            cell.register_node( node_idn=row[ 'node_idn' ], source=row[ 'source' ], target=row[ 'target' ] )

    return cell
#/def _cell_from_tree


def cell_from_file(
    path: str | Path,
    op_classes: dict[ str, type[ TableOp ] ],
    main_id: str,
    rng: np.random.Generator | None = None,
    verbose: int = 0,
    template_vars: dict[ str, str ] | None = None,
) -> Cell:
    """
    Construct a Cell from a config file, delegating to configTree.config_tree
    to recursively parse the file and its transitive [[children]] references
    into a flat initialization table (topology + each cell's own binding),
    then building nested Cells from that table bottom-up. File format is
    inferred from suffix at every node in the walk (configTree.config_tree);
    only '.toml' is implemented today.

    Parameters
    ----------
    path        : path to the config file
    op_classes  : dict[str, type[TableOp]] mapping each [[processes]]
                  node_idn to the TableOp subclass to construct for it. Each
                  instance is built via build_tableOps() as
                  cls(rng=rng, **kwargs) -- kwargs from the op's own node in
                  the __table_ops_parameters__ forest (empty for now; no
                  TOML syntax populates it yet), so an op with no declared
                  parameters just gets `cls(rng=rng)`, relying on its own
                  defaults (or a TypeError for a missing required kwarg).
    main_id     : passed to ``Cell.__init__`` and used as this file's own
                  node_idn in the parsed tree
    rng         : forwarded to every constructed TableOp, at every level
    verbose     : forwarded to ``Cell.__init__`` at every level
    template_vars : optional ``{key}`` substitutions applied to [[children]]
                  ``toml`` strings before resolving (e.g. ``{cells_root}``)

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

    [[processes]]                # one block per leaf op reference
        node_idn = "..."          # must be a key op_classes resolves
        source   = [...]          # names this op's positional inputs are
                                   # read from (this Cell's own namespace)
        target   = [...]          # names this op's positional outputs are
                                   # written under (ditto) -- need not be
                                   # unique across [[processes]]/[[children]]
                                   # entries; see squishy-sleeping-
                                   # snowflake.md

    [[children]]                 # one block per nested Cell
        node_idn = "..."          # key this child is registered under in
                                   # the parent's tableOps
        toml     = "path/to/child.toml"   # relative to this file
        source   = [...]                  # optional -- omit to leave this
        target   = [...]                  # child unregistered as a __nodes__
                                           # row (still reachable via bind())
    """
    tree, parameters, _ = configTree.config_tree(
        path          = path,
        node_idn      = main_id,
        template_vars = template_vars,
    )
    return _cell_from_tree(
        tree       = tree,
        node_idn   = main_id,
        op_classes = op_classes,
        rng        = rng,
        verbose    = verbose,
        parameters = parameters,
    )
#/def cell_from_file
