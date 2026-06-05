#
#//  test_web.py
#//  silverbrain
#//
#//  Created by Evan Mason on 5/29/26.
#

import asyncio
import threading
import uuid
from concurrent.futures import Future, wait

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.tableProcesses import TableProcessRef
from silverbrain.types import NodeDecl
from silverbrain.web import AsyncWebNode, Web, WebNode
from silverbrain.execution import ExecutionContext


def _identity_op(
    dfs: tuple,
    verbose: int = 0,
    verbose_prefix: str = '',
) -> tuple:
    return dfs
#/def _identity_op


def _make_web( name: str, op_idn: str, source: list, target: list ) -> Cell:
    w = Cell( name )
    w.register_tableOps( { 'identity': _identity_op } )
    w.register_process( TableProcessRef(
        op_idn = op_idn,
        source = source,
        target = target,
        terms  = [ 'identity' ],
    ) )
    return w
#/def _make_web


# -- WebNode

def test_web_node_declare() -> None:
    w = _make_web( 'a', 'store', [ 'inp' ], [ 'out' ] )
    node = WebNode( _cell=w )
    decls = node.declare()
    assert len( decls ) == 1
    assert decls[ 0 ] == NodeDecl( op_idn='store', source=( 'inp', ), target=( 'out', ) )
#/def test_web_node_declare


def test_web_node_run() -> None:
    w = _make_web( 'a', 'store', [ 'inp' ], [ 'out' ] )
    node = WebNode( _cell=w )
    df = pl.DataFrame( { 'value': [ 1, 2, 3 ] } )
    result = node.run( 'store', str( uuid.uuid4() ), { 'inp': df } ).result()
    assert result[ 'out' ][ 'value' ].to_list() == [ 1, 2, 3 ]
#/def test_web_node_run


def test_web_node_run_concurrent() -> None:
    """Two concurrent runs with different inputs complete independently."""
    w = _make_web( 'a', 'store', [ 'inp' ], [ 'out' ] )
    node = WebNode( _cell=w )

    pid = str( uuid.uuid4() )
    f1 = node.run( 'store', pid, { 'inp': pl.DataFrame( { 'v': [ 10 ] } ) } )
    f2 = node.run( 'store', pid, { 'inp': pl.DataFrame( { 'v': [ 20 ] } ) } )
    wait( [ f1, f2 ] )
    # Sequential mailbox: both complete without error
    assert f1.result()[ 'out' ][ 'v' ].to_list() == [ 10 ]
    assert f2.result()[ 'out' ][ 'v' ].to_list() == [ 20 ]
#/def test_web_node_run_concurrent


# -- Web

def test_composite_routes_by_op_idn() -> None:
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'in_a' ], [ 'out_a' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'op_b', [ 'in_b' ], [ 'out_b' ] ) )
    composite = Web( _nodes={ 'a': node_a, 'b': node_b } )

    df = pl.DataFrame( { 'x': [ 7 ] } )
    result = composite.run( 'op_a', str( uuid.uuid4() ), { 'in_a': df } ).result()
    assert 'out_a' in result
    assert result[ 'out_a' ][ 'x' ].to_list() == [ 7 ]
    assert 'out_b' not in result
#/def test_composite_routes_by_op_idn


def test_composite_chains_two_nodes() -> None:
    """op_a produces 'mid'; op_b declares 'mid' as source — should auto-chain."""
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'inp' ], [ 'mid' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'op_b', [ 'mid' ], [ 'final' ] ) )
    composite = Web( _nodes={ 'a': node_a, 'b': node_b } )

    df = pl.DataFrame( { 'v': [ 42 ] } )
    result = composite.run( 'op_a', str( uuid.uuid4() ), { 'inp': df } ).result()
    assert 'mid'   in result
    assert 'final' in result
    assert result[ 'final' ][ 'v' ].to_list() == [ 42 ]
#/def test_composite_chains_two_nodes


def test_composite_declare_exposes_entry_points_only() -> None:
    """Chain op_a→op_b: only op_a is an entry point; op_b is internal."""
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'x' ], [ 'y' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'op_b', [ 'y' ], [ 'z' ] ) )
    composite = Web( _nodes={ 'a': node_a, 'b': node_b } )
    decls = composite.declare()
    assert len( decls ) == 1
    assert decls[ 0 ].op_idn == 'op_a'
    # Reachable targets include both the intermediate and the final output
    assert set( decls[ 0 ].target ) == { 'y', 'z' }


def test_composite_declare_independent_ops_both_exposed() -> None:
    """Two independent ops (no shared tables): both are entry points."""
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'in_a' ], [ 'out_a' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'op_b', [ 'in_b' ], [ 'out_b' ] ) )
    composite = Web( _nodes={ 'a': node_a, 'b': node_b } )
    decls = composite.declare()
    assert { d.op_idn for d in decls } == { 'op_a', 'op_b' }
#/def test_composite_declare_aggregates_children


def test_composite_rejects_duplicate_op_idn() -> None:
    node_a = WebNode( _cell=_make_web( 'a', 'dup', [ 'x' ], [ 'y' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'dup', [ 'p' ], [ 'q' ] ) )
    with pytest.raises( ValueError, match='Duplicate op_idn' ):
        Web( _nodes={ 'a': node_a, 'b': node_b } )
#/def test_composite_rejects_duplicate_op_idn


def test_composite_raises_on_unknown_op_idn() -> None:
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'x' ], [ 'y' ] ) )
    composite = Web( _nodes={ 'a': node_a } )
    with pytest.raises( KeyError ):
        composite.run( 'does_not_exist', str( uuid.uuid4() ), {} )
#/def test_composite_raises_on_unknown_op_idn


def test_composite_nested() -> None:
    """Web inside Web."""
    node_a = WebNode( _cell=_make_web( 'a', 'op_a', [ 'inp' ], [ 'mid' ] ) )
    node_b = WebNode( _cell=_make_web( 'b', 'op_b', [ 'mid' ], [ 'out' ] ) )
    inner = Web( _nodes={ 'a': node_a, 'b': node_b } )
    outer = Web( _nodes={ 'inner': inner } )

    df = pl.DataFrame( { 'n': [ 99 ] } )
    result = outer.run( 'op_a', str( uuid.uuid4() ), { 'inp': df } ).result()
    assert result[ 'out' ][ 'n' ].to_list() == [ 99 ]
#/def test_composite_nested


# -- AsyncWebNode

def test_async_web_node_run() -> None:
    async def _coro(
        op_idn: str,
        process_id: str,
        inputs: dict,
    ) -> dict:
        return { 'async_out': inputs[ 'async_in' ] }

    decls = [ NodeDecl( op_idn='async_op', source=( 'async_in', ), target=( 'async_out', ) ) ]
    node  = AsyncWebNode( _declarations=decls, _coro_factory=_coro )

    df     = pl.DataFrame( { 'v': [ 5 ] } )
    result = node.run( 'async_op', str( uuid.uuid4() ), { 'async_in': df } ).result()
    assert result[ 'async_out' ][ 'v' ].to_list() == [ 5 ]
#/def test_async_web_node_run


def test_composite_with_async_node() -> None:
    """Composite chains a WebNode leaf into an AsyncWebNode leaf."""
    sync_node = WebNode( _cell=_make_web( 'a', 'step1', [ 'raw' ], [ 'processed' ] ) )

    async def _fetch( op_idn: str, process_id: str, inputs: dict ) -> dict:
        await asyncio.sleep( 0 )
        return { 'final': inputs[ 'processed' ] }

    async_node = AsyncWebNode(
        _declarations = [ NodeDecl( op_idn='step2', source=( 'processed', ), target=( 'final', ) ) ],
        _coro_factory = _fetch,
    )

    composite = Web( _nodes={ 's': sync_node, 'a': async_node } )
    df     = pl.DataFrame( { 'x': [ 1 ] } )
    result = composite.run( 'step1', str( uuid.uuid4() ), { 'raw': df } ).result()
    assert result[ 'final' ][ 'x' ].to_list() == [ 1 ]
#/def test_composite_with_async_node


# -- ExecutionContext

def test_execution_context_for_web() -> None:
    node = WebNode( _cell=_make_web( 'a', 'store', [ 'inp' ], [ 'out' ] ) )
    ctx  = ExecutionContext.for_web( node )
    df   = pl.DataFrame( { 'z': [ 3 ] } )
    result = ctx.run( 'store', inputs={ 'inp': df } )
    assert result[ 'out' ][ 'z' ].to_list() == [ 3 ]
#/def test_execution_context_for_web


def test_execution_context_for_web_wait_false() -> None:
    node = WebNode( _cell=_make_web( 'a', 'store', [ 'inp' ], [ 'out' ] ) )
    ctx  = ExecutionContext.for_web( node )
    df   = pl.DataFrame( { 'z': [ 9 ] } )
    fut  = ctx.run( 'store', inputs={ 'inp': df }, wait=False )
    assert isinstance( fut, Future )
    result = fut.result( timeout=5.0 )
    assert result[ 'out' ][ 'z' ].to_list() == [ 9 ]
#/def test_execution_context_for_web_wait_false
