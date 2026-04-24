#
#//  webNetwork.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/21/26.

import io
import json
import threading
from queue import Empty, Queue
from threading import Lock
from typing import Callable, Self

import polars as pl
import zmq

from . import tableProcesses, types
from .tableProcesses import _make_process_row
from .web import Web, _extract_op_subtree


# -- ZMQ serialization helpers

def _pack_envelope( web_id: str, op_id: str ) -> bytes:
    return json.dumps({
        'web_id': web_id,
        'op_id' : op_id,
    }).encode()
#/def _pack_envelope

def _unpack_envelope(
    data: bytes,
    ) -> tuple[
        str,
        str,
    ]:
    d = json.loads( data )
    return d[ 'web_id' ], d[ 'op_id' ]
#/def _unpack_envelope

def _pack_dataframes( dfs: tuple[ pl.DataFrame, ... ] ) -> list[ bytes ]:
    frames = []
    for df in dfs:
        buf = io.BytesIO()
        df.write_ipc( buf )
        frames.append( buf.getvalue() )
    return frames
#/def _pack_dataframes

def _unpack_dataframes( frames: list[ bytes ] ) -> tuple[ pl.DataFrame, ... ]:
    return tuple( pl.read_ipc( io.BytesIO( f ) ) for f in frames )
#/def _unpack_dataframes


# -- Process-tree helpers

def _collect_subtree_ids( rows: dict, root_nid: int ) -> set[ int ]:
    """All node_ids in the subtree rooted at root_nid (inclusive)."""
    result: set[ int ] = set()
    stack = [ root_nid ]
    while stack:
        nid = stack.pop()
        if nid in result:
            continue
        result.add( nid )
        row = rows[ nid ]
        for child in ( row.get( 'term_ids' ) or [] ):
            stack.append( child )
        if row.get( 'condition' ) is not None:
            stack.append( row[ 'condition' ] )
        for child in ( row.get( 'ifs' ) or [] ):
            stack.append( child )
        for child in ( row.get( 'thens' ) or [] ):
            stack.append( child )
    return result
#/def _collect_subtree_ids


def _ordered_deps( term_nids: list[ int ], all_rows: dict ) -> list[ str ]:
    """
    Tables consumed by the given term subtrees but not produced within them,
    in first-occurrence order across the term list.
    """
    produced:        set[ str ]  = set()
    consumed_seen:   set[ str ]  = set()
    consumed_ordered: list[ str ] = []
    for root_nid in term_nids:
        for nid in _collect_subtree_ids( all_rows, root_nid ):
            for tid in ( all_rows[ nid ].get( 'target' ) or [] ):
                produced.add( tid )
    for root_nid in term_nids:
        for nid in _collect_subtree_ids( all_rows, root_nid ):
            for tid in ( all_rows[ nid ].get( 'source' ) or [] ):
                if tid not in consumed_seen:
                    consumed_seen.add( tid )
                    consumed_ordered.append( tid )
    return [ t for t in consumed_ordered if t not in produced ]
#/def _ordered_deps


class LocalWebNode:
    """
    Leaf node: wraps a single web.Web.
    put enqueues work and returns immediately; a daemon worker thread
    calls self._web.put on each item.  Only op_ids in input_ids are accepted.
    """

    def __init__(
        self:      Self,
        web:       types.WebNode,
        input_ids: list[ str ],
    ) -> None:
        self._web          = web
        self.input_ids     = input_ids
        self._inbox:        Queue[ tuple[ tuple[ pl.DataFrame, ... ], str, str, int | None, str ] ] = Queue()
        self._stop          = threading.Event()
        self._thread:       threading.Thread | None      = None
        self._bindings:     list[ tuple[ str, zmq.Context, bool ] ] = []
        self._zmq_threads:  list[ threading.Thread ]                = []
        self._continuations: dict[
            str,
            tuple[
                'LocalWebNode',
                str,
                list[ tuple[ 'LocalWebNode', str ] ],
            ]
        ] = {}
    #/def __init__

    @classmethod
    def blank(
        cls,
        main_id:   str,
        input_ids: list[ str ] = [],
    ) -> 'LocalWebNode':
        return cls( Web( main_id = main_id, input_ids = input_ids ), input_ids = input_ids )
    #/def blank

    def register_continuation(
        self:          Self,
        op_id:         str,
        target_node:   'LocalWebNode',
        target_op_id:  str,
        table_sources: list[ tuple[ 'LocalWebNode', str ] ],
    ) -> None:
        self._continuations[ op_id ] = ( target_node, target_op_id, table_sources )
    #/def register_continuation

    def put(
        self:               Self,
        dfs:                tuple[ pl.DataFrame, ... ],
        op_id:              str,
        processes_table_id: str      = '__table_processes__',
        verbose:            int | None = None,
        verbose_prefix:     str      = '',
    ) -> None:
        if op_id not in self.input_ids:
            return
        self._inbox.put( ( dfs, op_id, processes_table_id, verbose, verbose_prefix ) )
    #/def put

    def bind(
        self:    Self,
        address: str,
        context: zmq.Context | None = None,
    ) -> None:
        own_ctx = context is None
        ctx     = context or zmq.Context()
        self._bindings.append( ( address, ctx, own_ctx ) )
        if self._thread is not None:
            t = threading.Thread( target=self._zmq_run, args=( address, ctx, own_ctx ), daemon=True )
            self._zmq_threads.append( t )
            t.start()
    #/def bind

    def start( self: Self ) -> None:
        self._stop.clear()
        self._zmq_threads.clear()
        self._thread = threading.Thread( target=self._run, daemon=True )
        self._thread.start()
        for address, ctx, own_ctx in self._bindings:
            t = threading.Thread(
                target=self._zmq_run,
                args=(
                    address, ctx, own_ctx
                ),
                daemon=True,
            )
            self._zmq_threads.append( t )
            t.start()
        #/for address, ctx, own_ctx in self._bindings
    #/def start

    def stop( self: Self ) -> None:
        self._stop.set()
    #/def stop

    def join( self: Self, timeout: float | None = None ) -> None:
        if self._thread is not None:
            self._thread.join( timeout=timeout )
        for t in self._zmq_threads:
            t.join( timeout=timeout )
    #/def join

    def _zmq_run( self: Self, address: str, context: zmq.Context, own_ctx: bool ) -> None:
        sock = context.socket( zmq.PULL )
        sock.setsockopt( zmq.RCVTIMEO, 200 )
        sock.bind( address )
        try:
            while not self._stop.is_set():
                try:
                    frames = sock.recv_multipart()
                except zmq.Again:
                    continue
                _, op_id = _unpack_envelope( frames[ 0 ] )
                dfs      = _unpack_dataframes( frames[ 1: ] )
                self.put( dfs, op_id )
        finally:
            sock.close()
            if own_ctx:
                context.term()
    #/def _zmq_run

    def _fire_continuation( self: Self, op_id: str, processes_table_id: str, verbose: int | None, verbose_prefix: str ) -> None:
        cont = self._continuations.get( op_id )
        if cont is None:
            return
        target_node, target_op_id, table_sources = cont
        cont_dfs = tuple( node._web.tables[ tid ] for node, tid in table_sources )
        target_node.put( cont_dfs, target_op_id, processes_table_id, verbose, verbose_prefix )
    #/def _fire_continuation

    def _run( self: Self ) -> None:
        while not self._stop.is_set():
            try:
                item = self._inbox.get( timeout=0.2 )
            except Empty:
                continue
            dfs, op_id, processes_table_id, verbose, verbose_prefix = item
            self._web.put( dfs, op_id, processes_table_id, verbose, verbose_prefix )
            self._fire_continuation( op_id, processes_table_id, verbose, verbose_prefix )
        while True:
            try:
                item = self._inbox.get_nowait()
            except Empty:
                break
            dfs, op_id, processes_table_id, verbose, verbose_prefix = item
            self._web.put( dfs, op_id, processes_table_id, verbose, verbose_prefix )
            self._fire_continuation( op_id, processes_table_id, verbose, verbose_prefix )
    #/def _run
#/class LocalWebNode

class CompositeWebNode:
    """
    Interior node: routes put calls to child WebNetwork implementations
    by exact op_id match.  Unknown op_ids are silently dropped.
    start/stop/join propagate to each unique child exactly once.
    """

    def __init__(
        self:   Self,
        routes: dict[ str, types.WebNode ],
    ) -> None:
        self._routes        = routes
        self.input_ids      = list( routes.keys() )
        self._stop          = threading.Event()
        self._started:      bool                                    = False
        self._bindings:     list[ tuple[ str, zmq.Context, bool ] ] = []
        self._zmq_threads:  list[ threading.Thread ]                = []
    #/def __init__

    def put(
        self:               Self,
        dfs:                tuple[ pl.DataFrame, ... ],
        op_id:              str,
        processes_table_id: str      = '__table_processes__',
        verbose:            int | None = None,
        verbose_prefix:     str      = '',
    ) -> None:
        child = self._routes.get( op_id )
        if child is not None:
            child.put( dfs, op_id, processes_table_id, verbose, verbose_prefix )
    #/def put

    def bind(
        self:    Self,
        address: str,
        context: zmq.Context | None = None,
    ) -> None:
        own_ctx = context is None
        ctx     = context or zmq.Context()
        self._bindings.append( ( address, ctx, own_ctx ) )
        if self._started:
            t = threading.Thread( target=self._zmq_run, args=( address, ctx, own_ctx ), daemon=True )
            self._zmq_threads.append( t )
            t.start()
    #/def bind

    def start( self: Self ) -> None:
        self._stop.clear()
        self._started = True
        self._zmq_threads.clear()
        for child in self._unique_children():
            child.start()
        for address, ctx, own_ctx in self._bindings:
            t = threading.Thread( target=self._zmq_run, args=( address, ctx, own_ctx ), daemon=True )
            self._zmq_threads.append( t )
            t.start()
    #/def start

    def stop( self: Self ) -> None:
        self._stop.set()
        for child in self._unique_children():
            child.stop()
    #/def stop

    def join( self: Self, timeout: float | None = None ) -> None:
        for t in self._zmq_threads:
            t.join( timeout=timeout )
        for child in self._unique_children():
            child.join( timeout=timeout )
    #/def join

    def _zmq_run( self: Self, address: str, context: zmq.Context, own_ctx: bool ) -> None:
        sock = context.socket( zmq.PULL )
        sock.setsockopt( zmq.RCVTIMEO, 200 )
        sock.bind( address )
        try:
            while not self._stop.is_set():
                try:
                    frames = sock.recv_multipart()
                except zmq.Again:
                    continue
                _, op_id = _unpack_envelope( frames[ 0 ] )
                dfs      = _unpack_dataframes( frames[ 1: ] )
                self.put( dfs, op_id )
        finally:
            sock.close()
            if own_ctx:
                context.term()
    #/def _zmq_run

    def delegate_ops(
        self:        Self,
        process_id:  str,
        start_op_id: str,
        child_key:   str,
        end_op_id:   str | None = None,
    ) -> None:
        """
        Move a contiguous range of terms [start_op_id … end_op_id] from the
        TableProcessSequence identified by process_id (in its source child) to
        the child identified by child_key.

        Both nodes must be LocalWebNode.  Raises ValueError if any delegated
        op has side effects recorded in __table_op_effects__.

        After delegation:
          - source runs the prefix (ops before start_op_id) under process_id
          - target runs the range under  f"{process_id}__delegated"
          - if a suffix exists, source runs it under f"{process_id}__suffix"
          - continuations are wired so prefix → delegate → suffix fire in order
        """
        # -- validate nodes
        if process_id not in self._routes:
            raise KeyError( process_id )
        if child_key not in self._routes:
            raise KeyError( child_key )
        source_node = self._routes[ process_id ]
        target_node = self._routes[ child_key ]
        if not isinstance( source_node, LocalWebNode ):
            raise TypeError( f"source child for {process_id!r} must be LocalWebNode" )
        if not isinstance( target_node, LocalWebNode ):
            raise TypeError( f"target child for {child_key!r} must be LocalWebNode" )

        # -- locate the sequence root in source
        proc_df = source_node._web.tables[ '__table_processes__' ]
        all_rows = { r[ 'node_id' ]: r for r in proc_df.to_dicts() }
        orig_root_nid = tableProcesses.find_root_pid( proc_df, process_id )
        orig_root     = all_rows[ orig_root_nid ]
        if orig_root[ 'type' ] != 'TableProcessSequence':
            raise ValueError(
                f"process_id {process_id!r} must be a TableProcessSequence, "
                f"got {orig_root['type']!r}"
            )

        term_nids = list( orig_root[ 'term_ids' ] )
        term_op_ids = [ all_rows[ nid ][ 'op_id' ] for nid in term_nids ]

        if start_op_id not in term_op_ids:
            raise ValueError( f"start_op_id {start_op_id!r} not found in sequence {process_id!r}" )
        start_idx = term_op_ids.index( start_op_id )

        if end_op_id is None:
            end_idx = len( term_nids ) - 1
        else:
            if end_op_id not in term_op_ids:
                raise ValueError( f"end_op_id {end_op_id!r} not found in sequence {process_id!r}" )
            end_idx = term_op_ids.index( end_op_id )

        if end_idx < start_idx:
            raise ValueError( "end_op_id must not precede start_op_id in the sequence" )

        prefix_term_nids   = term_nids[ :start_idx ]
        delegate_term_nids = term_nids[ start_idx : end_idx + 1 ]
        suffix_term_nids   = term_nids[ end_idx + 1 : ]
        has_suffix         = bool( suffix_term_nids )

        # -- effects check: no delegated op may have recorded side effects
        all_delegated_nids: set[ int ] = set()
        for nid in delegate_term_nids:
            all_delegated_nids |= _collect_subtree_ids( all_rows, nid )

        effects_op_ids: set[ str ] = set(
            source_node._web.tables[ '__table_op_effects__' ][ 'op_id' ].to_list()
        )
        for nid in all_delegated_nids:
            row = all_rows[ nid ]
            if row[ 'type' ] == 'Op' and row[ 'op_id' ] in effects_op_ids:
                raise ValueError(
                    f"op {row['op_id']!r} in delegated range has side effects "
                    f"and cannot be delegated"
                )

        # -- compute external dependencies
        delegated_ext_deps = _ordered_deps( delegate_term_nids, all_rows )
        suffix_ext_deps    = _ordered_deps( suffix_term_nids,   all_rows ) if has_suffix else []

        # -- classify delegate outputs (for back-continuation table routing)
        delegate_produced: set[ str ] = set()
        for nid in all_delegated_nids:
            for tid in ( all_rows[ nid ].get( 'target' ) or [] ):
                delegate_produced.add( tid )

        orig_target      = list( orig_root[ 'target' ] )
        delegated_target = (
            orig_target
            if not has_suffix
            else [ t for t in suffix_ext_deps if t in delegate_produced ]
        )

        # -- copy relevant tableOps to target
        new_ops: dict = {}
        for nid in all_delegated_nids:
            row = all_rows[ nid ]
            if row[ 'type' ] == 'Op' and row[ 'op_id' ] is not None:
                key = row[ 'op_id' ]
                if key in source_node._web.tableOps and key not in target_node._web.tableOps:
                    new_ops[ key ] = source_node._web.tableOps[ key ]
        if new_ops:
            target_node._web.register_tableOps( new_ops )

        # -- mutate source __table_processes__
        # 1. remove delegated subtrees
        proc_df = proc_df.filter( ~pl.col( 'node_id' ).is_in( list( all_delegated_nids ) ) )
        # 2. replace orig root row with updated prefix root (same node_id)
        proc_df = proc_df.filter( pl.col( 'node_id' ) != orig_root_nid )
        new_prefix_root = _make_process_row(
            node_id   = orig_root_nid,
            op_id     = process_id,
            parent_id = None,
            type      = 'TableProcessSequence',
            source    = list( orig_root[ 'source' ] ),
            target    = delegated_ext_deps,
            term_ids  = prefix_term_nids,
        )
        proc_df = pl.concat( [ proc_df, new_prefix_root ], how = 'vertical' )
        # 3. if suffix, register it
        if has_suffix:
            _max = proc_df[ 'node_id' ].max()
            suffix_root_nid = int( _max + 1 ) if _max is not None else 0
            # reparent suffix direct terms to new suffix root
            proc_df = proc_df.with_columns(
                pl.when(
                    pl.col( 'node_id' ).is_in( suffix_term_nids )
                    & ( pl.col( 'parent_id' ) == orig_root_nid )
                )
                .then( pl.lit( suffix_root_nid ) )
                .otherwise( pl.col( 'parent_id' ) )
                .alias( 'parent_id' )
            )
            suffix_op_id = f"{ process_id }__suffix"
            new_suffix_root = _make_process_row(
                node_id   = suffix_root_nid,
                op_id     = suffix_op_id,
                parent_id = None,
                type      = 'TableProcessSequence',
                source    = suffix_ext_deps,
                target    = orig_target,
                term_ids  = suffix_term_nids,
            )
            proc_df = pl.concat( [ proc_df, new_suffix_root ], how = 'vertical' )
            source_node.input_ids.append( suffix_op_id )
            self._routes[ suffix_op_id ] = source_node
        source_node._web.tables[ '__table_processes__' ] = proc_df

        # -- build delegated sequence in target
        delegated_op_id  = f"{ process_id }__delegated"
        delegated_df     = source_node._web.tables[ '__table_processes__' ]  # we mutated source above
        # extract original delegated rows from the original proc_df snapshot
        # (already removed from source, so re-derive from all_rows)
        delegated_rows_list = [ all_rows[ nid ] for nid in all_delegated_nids ]
        delegated_orig_df   = pl.DataFrame( delegated_rows_list, schema = proc_df.schema )

        tgt_proc_df = target_node._web.tables[ '__table_processes__' ]
        _tmax       = tgt_proc_df[ 'node_id' ].max()
        offset      = int( _tmax + 1 ) if _tmax is not None else 0

        reindexed = tableProcesses.reindex_process_df( delegated_orig_df, offset )
        # direct children of old orig_root had parent_id = orig_root_nid;
        # after reindexing that becomes orig_root_nid + offset → replace with new delegated root
        old_parent_sentinel = orig_root_nid + offset
        new_delegated_root_nid = int( reindexed[ 'node_id' ].max() + 1 )
        reindexed = reindexed.with_columns(
            pl.when( pl.col( 'parent_id' ) == old_parent_sentinel )
            .then( pl.lit( new_delegated_root_nid ) )
            .otherwise( pl.col( 'parent_id' ) )
            .alias( 'parent_id' )
        )
        new_delegated_root = _make_process_row(
            node_id   = new_delegated_root_nid,
            op_id     = delegated_op_id,
            parent_id = None,
            type      = 'TableProcessSequence',
            source    = delegated_ext_deps,
            target    = delegated_target,
            term_ids  = [ nid + offset for nid in delegate_term_nids ],
        )
        target_node._web.tables[ '__table_processes__' ] = pl.concat(
            [ tgt_proc_df, reindexed, new_delegated_root ], how = 'vertical'
        )
        target_node.input_ids.append( delegated_op_id )
        self._routes[ delegated_op_id ] = target_node

        # -- wire continuations
        source_node.register_continuation(
            process_id,
            target_node,
            delegated_op_id,
            [ ( source_node, tid ) for tid in delegated_ext_deps ],
        )
        if has_suffix:
            target_node.register_continuation(
                delegated_op_id,
                source_node,
                suffix_op_id,
                [
                    ( target_node if tid in delegate_produced else source_node, tid )
                    for tid in suffix_ext_deps
                ],
            )
    #/def delegate_ops

    def _unique_children( self: Self ) -> list:
        seen:   set[ int ] = set()
        result: list       = []
        for child in self._routes.values():
            cid = id( child )
            if cid not in seen:
                seen.add( cid )
                result.append( child )
        return result
    #/def _unique_children
#/class CompositeWebNode

class ZmqSenderNode:
    """
    Remote leaf: sends DataFrame tuples to a remote WebNode via ZMQ PUSH.
    Socket is opened lazily on first put and pooled for the lifetime of
    this object.  For inproc:// transport pass the same zmq.Context as the
    receiving node.  Call close() when done.
    """

    def __init__(
        self:    Self,
        address: str,
        web_id:  str,
        context: zmq.Context | None = None,
    ) -> None:
        own_ctx        = context is None
        self._ctx      = context or zmq.Context()
        self._own_ctx  = own_ctx
        self._address  = address
        self._web_id   = web_id
        self._sock:    zmq.Socket | None = None
        self._lock     = Lock()
        self.input_ids: list[ str ] = []
    #/def __init__

    def put(
        self:               Self,
        dfs:                tuple[ pl.DataFrame, ... ],
        op_id:              str,
        processes_table_id: str       = '__table_processes__',
        verbose:            int | None = None,
        verbose_prefix:     str       = '',
    ) -> None:
        frames = [ _pack_envelope( self._web_id, op_id ) ] + _pack_dataframes( dfs )
        with self._lock:
            if self._sock is None:
                self._sock = self._ctx.socket( zmq.PUSH )
                self._sock.connect( self._address )
            #/if self._sock is None
            self._sock.send_multipart( frames )
        #/with self._lock
    #/def put

    def close( self: Self ) -> None:
        with self._lock:
            if self._sock is not None:
                self._sock.close()
                self._sock = None
        if self._own_ctx:
            self._ctx.term()
    #/def close

    def bind(
        self:    Self,
        address: str,
        context: zmq.Context | None = None,
    ) -> None:
        pass
    #/def bind

    def start( self: Self ) -> None:
        pass
    #

    def stop( self: Self ) -> None:
        pass
    #

    def join( self: Self, timeout: float | None = None ) -> None:
        pass
    #
#/class ZmqSenderNode


# TODO: replace with a general wiring API once the pairing abstraction stabilises
def wire_parameter_calculator(
    param_node:       'LocalWebNode',
    calc_node:        'LocalWebNode',
    param_brain:      object,
    continuation_map: dict,
    step_source_map:  dict,
) -> None:
    """
    Wire ParameterBrain ↔ CalculatorBrain for the async branch/step loop.

    1. Registers a put_to_{step_id} op on param_brain for every step in
       step_source_map, so each branch body can enqueue work on calc_node.
    2. Registers a continuation on calc_node for every step in continuation_map
       so that after each step, the corresponding branch is re-fired on param_node.

    Must be called after both nodes exist and before start().

    Args:
        param_node:       LocalWebNode wrapping a ParameterBrain instance.
        calc_node:        LocalWebNode wrapping a CalculatorBrain instance.
        param_brain:      The ParameterBrain Web instance (param_node._web).
        continuation_map: brainProcessesMap.CONTINUATION_MAP
        step_source_map:  brainProcessesMap.CALCULATOR_STEP_SOURCE_MAP
    """
    from silverknockoff import brainOpsMap

    for step_id in step_source_map:
        op_key = f'put_to_{step_id}'
        param_brain.register_tableOps({
            op_key: brainOpsMap.make_put_to_node_op( calc_node, step_id ),
        })

    for step_id, ( branch_id, branch_source, calc_tables ) in continuation_map.items():
        table_sources = [
            ( calc_node if t in calc_tables else param_node, t )
            for t in branch_source
        ]
        calc_node.register_continuation(
            op_id         = step_id,
            target_node   = param_node,
            target_op_id  = branch_id,
            table_sources = table_sources,
        )
#/def wire_parameter_calculator


def _make_split_web(
    source:   Web,
    op_ids:   list[ str ],
    tables:   dict[ str, pl.DataFrame | None ],
) -> Web:
    """
    Build a UC-stub Web for use as a split child.
    Copies the process trees for each op_id in op_ids from source, shares
    source.tableOps by reference, and sets the caller-supplied domain tables.
    """
    web = Web( main_id = source.main_id, input_ids = op_ids, verbose = source.verbose )
    web.tableOps = source.tableOps
    proc_df = source.tables[ '__table_processes__' ]
    for op_id in op_ids:
        subtree = _extract_op_subtree( proc_df, op_id )
        if subtree.is_empty():
            continue
        _max   = web.tables[ '__table_processes__' ][ 'node_id' ].max()
        offset = int( _max + 1 ) if _max is not None else 0
        web.tables[ '__table_processes__' ] = pl.concat(
            [ web.tables[ '__table_processes__' ],
              tableProcesses.reindex_process_df( subtree, offset ) ],
            how = 'vertical',
        )
    web.tables |= tables
    return web
#/def _make_split_web


def split_node(
    node:     LocalWebNode | CompositeWebNode,
    group_a:  list[ str ],
    group_b:  list[ str ],
    tables_a: dict[ str, pl.DataFrame | None ] | None = None,
    tables_b: dict[ str, pl.DataFrame | None ] | None = None,
) -> CompositeWebNode:
    """
    Split a node into a CompositeWebNode with two children.

    LocalWebNode: each child gets a UC-stub Web (independent tables, shared
    tableOps dict) built from the process trees for its op_id group.
    tables_a / tables_b specify which domain tables each half receives; invalid
    splits (missing tables a process depends on) will fail at runtime.

    CompositeWebNode: routes are partitioned into two new CompositeWebNodes;
    tables_a / tables_b are ignored.
    """
    if isinstance( node, LocalWebNode ):
        child_a: types.WebNode = LocalWebNode(
            _make_split_web( node._web, group_a, tables_a or {} ),
            input_ids = group_a,
        )
        child_b: types.WebNode = LocalWebNode(
            _make_split_web( node._web, group_b, tables_b or {} ),
            input_ids = group_b,
        )
    else:
        child_a = CompositeWebNode( { op_id: node._routes[ op_id ] for op_id in group_a } )
        child_b = CompositeWebNode( { op_id: node._routes[ op_id ] for op_id in group_b } )
    routes: dict[ str, types.WebNode ] = { op_id: child_a for op_id in group_a }
    routes |= { op_id: child_b for op_id in group_b }
    return CompositeWebNode( routes )
#/def split_node
