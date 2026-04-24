#
#//  test_web_network.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/21/26.

import threading
from unittest.mock import MagicMock

import polars as pl
import pytest
import zmq

from silverbrain.web import Web
from silverbrain.tableOps import DiskWrite, InputSchema, OutputSchema, TransformOp
from silverbrain.tableProcesses import TableProcessRef, TableProcessSequence
from silverbrain.webNetwork import LocalWebNode, CompositeWebNode, ZmqSenderNode


def _identity_op(
    dfs: tuple,
    verbose: int = 0,
    verbose_prefix: str = '',
) -> tuple:
    return dfs
#/def _identity_op


def test_local_web_network() -> None:
    web = Web( 'test' )
    web.register_tableOps( { 'identity': _identity_op } )
    web.register_process(
        TableProcessRef(
            op_id  = 'store',
            source = [ 'incoming' ],
            target = [ 'result' ],
            terms  = [ 'identity' ],
        )
    )

    net = LocalWebNode( web, input_ids=[ 'store' ] )
    net.start()

    df = pl.DataFrame( { 'value': [ 1, 2, 3 ] } )
    net.put( ( df, ), 'store' )

    done = threading.Event()
    # poll until the worker thread writes the result
    def _wait():
        while web.tables.get( 'result' ) is None:
            pass
        done.set()
    threading.Thread( target=_wait, daemon=True ).start()
    assert done.wait( timeout=2.0 ), "put was never processed"
    assert web.tables[ 'result' ][ 'value' ].to_list() == [ 1, 2, 3 ]

    net.stop()
    net.join( timeout=2.0 )
#/def test_local_web_network


def test_local_web_network_ignores_unknown_op_id() -> None:
    web = Web( 'test' )
    net = LocalWebNode( web, input_ids=[ 'store' ] )
    net.start()
    net.put( ( pl.DataFrame( { 'x': [ 1 ] } ), ), 'unknown' )
    net.stop()
    net.join( timeout=2.0 )
    assert net._inbox.empty()
#/def test_local_web_network_ignores_unknown_op_id


def test_composite_web_network() -> None:
    def make_leaf( name: str ) -> LocalWebNode:
        w = Web( name )
        w.register_tableOps( { 'identity': _identity_op } )
        w.register_process(
            TableProcessRef(
                op_id  = name,
                source = [ 'incoming' ],
                target = [ 'result_' + name ],
                terms  = [ 'identity' ],
            )
        )
        return LocalWebNode( w, input_ids=[ name ] )
    #/def make_leaf

    leaf_a = make_leaf( 'a' )
    leaf_b = make_leaf( 'b' )

    composite = CompositeWebNode( routes={ 'a': leaf_a, 'b': leaf_b } )
    composite.start()

    df_a = pl.DataFrame( { 'value': [ 10 ] } )
    df_b = pl.DataFrame( { 'value': [ 20 ] } )
    composite.put( ( df_a, ), 'a' )
    composite.put( ( df_b, ), 'b' )
    composite.put( ( pl.DataFrame( { 'value': [ 99 ] } ), ), 'unknown' )

    done_a, done_b = threading.Event(), threading.Event()
    def _wait_a():
        while leaf_a._web.tables.get( 'result_a' ) is None:
            pass
        done_a.set()
    def _wait_b():
        while leaf_b._web.tables.get( 'result_b' ) is None:
            pass
        done_b.set()
    threading.Thread( target=_wait_a, daemon=True ).start()
    threading.Thread( target=_wait_b, daemon=True ).start()

    assert done_a.wait( timeout=2.0 ), "leaf_a never processed 'a'"
    assert done_b.wait( timeout=2.0 ), "leaf_b never processed 'b'"
    assert leaf_a._web.tables[ 'result_a' ][ 'value' ].to_list() == [ 10 ]
    assert leaf_b._web.tables[ 'result_b' ][ 'value' ].to_list() == [ 20 ]

    composite.stop()
    composite.join( timeout=2.0 )
#/def test_composite_web_network


def test_local_web_network_zmq_bind() -> None:
    ctx = zmq.Context()

    web = Web( 'test' )
    web.register_tableOps( { 'identity': _identity_op } )
    web.register_process(
        TableProcessRef(
            op_id  = 'store',
            source = [ 'incoming' ],
            target = [ 'result' ],
            terms  = [ 'identity' ],
        )
    )

    net = LocalWebNode( web, input_ids=[ 'store' ] )
    net.bind( 'inproc://test_zmq_bind', ctx )
    net.start()

    sender = ZmqSenderNode( 'inproc://test_zmq_bind', 'web', context=ctx )
    sender.put( ( pl.DataFrame( { 'value': [ 7, 8, 9 ] } ), ), 'store' )

    done = threading.Event()
    def _wait():
        while web.tables.get( 'result' ) is None:
            pass
        done.set()
    threading.Thread( target=_wait, daemon=True ).start()
    assert done.wait( timeout=2.0 ), "ZMQ message was never processed"
    assert web.tables[ 'result' ][ 'value' ].to_list() == [ 7, 8, 9 ]

    net.stop()
    net.join( timeout=2.0 )
    sender.close()
    ctx.term()
#/def test_local_web_network_zmq_bind


def test_shared_child_started_once() -> None:
    child = MagicMock()
    composite = CompositeWebNode( routes={ 'op_a': child, 'op_b': child } )

    composite.start()
    child.start.assert_called_once()

    composite.stop()
    child.stop.assert_called_once()

    composite.join()
    child.join.assert_called_once()
#/def test_shared_child_started_once


# -- delegate_ops helpers

_pass = TransformOp( lam = lambda dfs, verbose=0, verbose_prefix='': dfs )


def _make_sequence_web( process_id: str ) -> Web:
    """Web with a 3-step sequence: in -> mid1 -> mid2 -> out."""
    web = Web( process_id )
    web.register_tableOps( { 'pass_op': _pass } )
    web.register_process(
        TableProcessSequence(
            op_id  = process_id,
            source = [ 'in_table' ],
            target = [ 'out_table' ],
            terms  = [
                TableProcessRef( op_id='step_a', source=[ 'in_table' ], target=[ 'mid1' ], terms=[ 'pass_op' ] ),
                TableProcessRef( op_id='step_b', source=[ 'mid1' ],     target=[ 'mid2' ], terms=[ 'pass_op' ] ),
                TableProcessRef( op_id='step_c', source=[ 'mid2' ],     target=[ 'out_table' ], terms=[ 'pass_op' ] ),
            ],
        )
    )
    return web
#/def _make_sequence_web


def _wait_for_table(
    web:    Web,
    key:    str,
    timeout: float = 2.0,
) -> bool:
    done = threading.Event()
    def _poll():
        while web.tables.get( key ) is None:
            pass
        done.set()
    threading.Thread( target=_poll, daemon=True ).start()
    return done.wait( timeout=timeout )
#/def _wait_for_table

_SYSTEM_TABLE_KEYS = frozenset({
    '__tables_schema__', '__table_processes__',
    '__table_op_schema__', '__table_op_effects__',
    '__table_init__', '__process_init__',
})

def _run_reference( df: pl.DataFrame ) -> Web:
    """Run the 3-step sequence without delegation and return the finished web."""
    ref_web  = _make_sequence_web( 'proc' )
    ref_node = LocalWebNode( ref_web, input_ids=[ 'proc' ] )
    net      = CompositeWebNode( routes={ 'proc': ref_node } )
    net.start()
    net.put( ( df, ), 'proc' )
    assert _wait_for_table( ref_web, 'out_table' ), "reference run timed out"
    net.stop()
    net.join( timeout=2.0 )
    return ref_web
#/def _run_reference

def _assert_merge_matches_ref( source_web: Web, target_web: Web, ref_web: Web ) -> None:
    """Merge source + target and assert all domain tables and tableOps keys match ref."""
    merged = Web.merge( source_web, target_web )
    for key, ref_df in ref_web.tables.items():
        if key in _SYSTEM_TABLE_KEYS:
            continue
        assert key in merged.tables, f"merged missing table {key!r}"
        assert merged.tables[ key ].equals( ref_df ), f"table {key!r} differs after merge"
    assert set( merged.tableOps.keys() ) == set( ref_web.tableOps.keys() )
#/def _assert_merge_matches_ref


# -- blank

def test_blank_creates_local_web_node() -> None:
    node = LocalWebNode.blank( 'my_web' )
    assert isinstance( node, LocalWebNode )
    assert isinstance( node._web, Web )
    assert node._web.main_id == 'my_web'
    assert '__table_processes__' in node._web.tables
#/def test_blank_creates_local_web_node


def test_blank_with_input_ids() -> None:
    node = LocalWebNode.blank( 'w', input_ids=[ 'op_a', 'op_b' ] )
    assert node.input_ids == [ 'op_a', 'op_b' ]
    assert node._web.input_ids == [ 'op_a', 'op_b' ]
#/def test_blank_with_input_ids


# -- delegate_ops: no suffix

def test_delegate_ops_no_suffix() -> None:
    """Prefix (step_a) runs in source; delegate (step_b, step_c) runs in target."""
    df          = pl.DataFrame( { 'x': [ 1, 2, 3 ] } )
    ref_web     = _run_reference( df )

    source_web  = _make_sequence_web( 'proc' )
    source_node = LocalWebNode( source_web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )

    composite = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )
    composite.delegate_ops( 'proc', 'step_b', 'tgt' )
    composite.start()
    composite.put( ( df, ), 'proc' )

    assert _wait_for_table( target_node._web, 'out_table' ), "delegate never wrote out_table"

    composite.stop()
    composite.join( timeout=2.0 )

    _assert_merge_matches_ref( source_web, target_node._web, ref_web )
#/def test_delegate_ops_no_suffix


# -- delegate_ops: with suffix

def test_delegate_ops_with_suffix() -> None:
    """Prefix (step_a) in source, delegate (step_b) in target, suffix (step_c) back in source."""
    df          = pl.DataFrame( { 'x': [ 10, 20 ] } )
    ref_web     = _run_reference( df )

    source_web  = _make_sequence_web( 'proc' )
    source_node = LocalWebNode( source_web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )

    composite = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )
    composite.delegate_ops( 'proc', 'step_b', 'tgt', end_op_id='step_b' )
    composite.start()
    composite.put( ( df, ), 'proc' )

    assert _wait_for_table( source_node._web, 'out_table' ), "suffix never wrote out_table"

    composite.stop()
    composite.join( timeout=2.0 )

    _assert_merge_matches_ref( source_web, target_node._web, ref_web )
#/def test_delegate_ops_with_suffix


# -- delegate_ops: effects guard

def test_delegate_ops_raises_on_effects() -> None:
    """delegate_ops must raise ValueError if any delegated op has side effects."""
    effect_op = TransformOp(
        lam     = lambda dfs, verbose=0, verbose_prefix='': dfs,
        effects = [ DiskWrite( path_input=0, path_column='fp' ) ],
        input   = [ InputSchema( extra='open' ) ],
        output  = [ OutputSchema( passthrough_from=[ 0 ] ) ],
    )
    web = Web( 'proc' )
    web.register_tableOps( { 'pass_op': _pass, 'effect_op': effect_op } )
    web.register_process(
        TableProcessSequence(
            op_id  = 'proc',
            source = [ 'in_table' ],
            target = [ 'out_table' ],
            terms  = [
                TableProcessRef( op_id='step_a', source=[ 'in_table' ], target=[ 'mid1' ], terms=[ 'pass_op' ] ),
                TableProcessRef( op_id='step_b', source=[ 'mid1' ],     target=[ 'out_table' ], terms=[ 'effect_op' ] ),
            ],
        )
    )
    source_node = LocalWebNode( web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )
    composite   = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )

    with pytest.raises( ValueError, match='side effects' ):
        composite.delegate_ops( 'proc', 'step_b', 'tgt' )
#/def test_delegate_ops_raises_on_effects


# -- delegate_ops: type / value guards

def test_delegate_ops_raises_if_source_not_local_web_node() -> None:
    mock_child  = MagicMock()
    target_node = LocalWebNode.blank( 'target' )
    composite   = CompositeWebNode( routes={ 'proc': mock_child, 'tgt': target_node } )
    with pytest.raises( TypeError ):
        composite.delegate_ops( 'proc', 'step_a', 'tgt' )
#/def test_delegate_ops_raises_if_source_not_local_web_node


def test_delegate_ops_raises_if_target_not_local_web_node() -> None:
    source_web  = _make_sequence_web( 'proc' )
    source_node = LocalWebNode( source_web, input_ids=[ 'proc' ] )
    mock_child  = MagicMock()
    composite   = CompositeWebNode( routes={ 'proc': source_node, 'tgt': mock_child } )
    with pytest.raises( TypeError ):
        composite.delegate_ops( 'proc', 'step_b', 'tgt' )
#/def test_delegate_ops_raises_if_target_not_local_web_node


def test_delegate_ops_raises_if_not_sequence() -> None:
    web = Web( 'proc' )
    web.register_tableOps( { 'pass_op': _pass } )
    web.register_process(
        TableProcessRef( op_id='proc', source=[ 'in_table' ], target=[ 'out_table' ], terms=[ 'pass_op' ] )
    )
    source_node = LocalWebNode( web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )
    composite   = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )
    with pytest.raises( ValueError, match='TableProcessSequence' ):
        composite.delegate_ops( 'proc', 'proc', 'tgt' )
#/def test_delegate_ops_raises_if_not_sequence


# -- delegate_ops: result equivalence

def test_delegate_ops_result_matches_undelegated() -> None:
    """Delegating a middle op and routing back must produce the same result as running inline."""
    df      = pl.DataFrame( { 'x': [ 1, 2, 3 ], 'y': [ 4, 5, 6 ] } )
    ref_web = _run_reference( df )

    source_web  = _make_sequence_web( 'proc' )
    source_node = LocalWebNode( source_web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )
    composite   = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )
    composite.delegate_ops( 'proc', 'step_b', 'tgt', end_op_id='step_b' )
    composite.start()
    composite.put( ( df, ), 'proc' )
    assert _wait_for_table( source_web, 'out_table' ), "delegated run timed out"
    composite.stop()
    composite.join( timeout=2.0 )

    _assert_merge_matches_ref( source_web, target_node._web, ref_web )
#/def test_delegate_ops_result_matches_undelegated


def test_delegate_ops_no_end_op_id_result_matches_undelegated() -> None:
    """Omitting end_op_id delegates step_b through the last term; result must match inline."""
    df      = pl.DataFrame( { 'x': [ 7, 8, 9 ] } )
    ref_web = _run_reference( df )

    source_web  = _make_sequence_web( 'proc' )
    source_node = LocalWebNode( source_web, input_ids=[ 'proc' ] )
    target_node = LocalWebNode.blank( 'target' )
    composite   = CompositeWebNode( routes={ 'proc': source_node, 'tgt': target_node } )
    composite.delegate_ops( 'proc', 'step_b', 'tgt' )  # no end_op_id → step_b and step_c delegated
    composite.start()
    composite.put( ( df, ), 'proc' )
    assert _wait_for_table( target_node._web, 'out_table' ), "delegated run timed out"
    composite.stop()
    composite.join( timeout=2.0 )

    _assert_merge_matches_ref( source_web, target_node._web, ref_web )
#/def test_delegate_ops_no_end_op_id_result_matches_undelegated
