#
#//  test_cell_verbose_trace.py
#//  silverbrain
#//

"""
Tests for Cell.__call__/_WiringEngine's verbose>=1 enter/exit trace:
a Cell prints '{path} {self.source}' at start (path = '{main_id}' at the
root, else '{verbose_prefix}/{main_id}' -- no leading '/' at the root) and
'/{path} {self.target}' at exit, no 'source:'/'target:' labels (the
leading '/' alone distinguishes exit from enter). It passes path down as
verbose_prefix to its children; a leaf TableOp child additionally gets
'{path}/{op_id} {source}' at start and '/{path}/{op_id} {target}' at exit
(nested Cell children print their own, not double counted here) -- and the
op itself receives '{path}/{op_id}' (not just '{path}') as ITS OWN
verbose_prefix, so anything the op prints internally is already scoped to
that specific op, not just the enclosing Cell.
"""

import polars as pl
import pytest

from silverbrain.cell import Cell


class _DoubleOp:
    def __init__( self, rng = None ) -> None:
        self.rng = rng
    def __call__( self, dfs, corr_id = (), verbose = 0, verbose_prefix = '' ):
        if verbose >= 1:
            print( f"{verbose_prefix}: doubling" )
        return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 2 ).alias( 'v' ) ), )


def _make_leaf_cell() -> Cell:
    c = Cell( main_id = 'leaf', ops = { 'double': _DoubleOp() }, source = [ 'inp' ], target = [ 'out' ] )
    c.register_node( 'double', source = [ 'inp' ], target = [ 'out' ] )
    return c


def test_verbose_0_prints_nothing( capsys: pytest.CaptureFixture ) -> None:
    c = _make_leaf_cell()
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 0 ).result( timeout = 5 )
    assert capsys.readouterr().out == ''


def test_verbose_1_cell_enter_and_exit_are_bare_prefix_plus_tuple( capsys: pytest.CaptureFixture ) -> None:
    c = _make_leaf_cell()
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 1 ).result( timeout = 5 )
    lines = capsys.readouterr().out.splitlines()
    assert "leaf ('inp',)" in lines   # enter: no leading '/', no 'source:' label
    assert "/leaf ('out',)" in lines  # exit: single leading '/', target not source


def test_verbose_1_leaf_op_enter_shows_source_exit_shows_target( capsys: pytest.CaptureFixture ) -> None:
    c = _make_leaf_cell()
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 1 ).result( timeout = 5 )
    lines = capsys.readouterr().out.splitlines()
    assert "leaf/double ('inp',)" in lines
    assert "/leaf/double ('out',)" in lines


def test_verbose_1_op_receives_its_own_op_idn_in_verbose_prefix( capsys: pytest.CaptureFixture ) -> None:
    """The op itself (not just _WiringEngine's own prints) gets
    '{path}/{op_id}' as its verbose_prefix -- confirmed via _DoubleOp's own
    internal print above, using ':' not indentation/newline."""
    c = _make_leaf_cell()
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 1 ).result( timeout = 5 )
    lines = capsys.readouterr().out.splitlines()
    assert 'leaf/double: doubling' in lines


def test_verbose_1_prefix_grows_one_segment_per_nested_cell( capsys: pytest.CaptureFixture ) -> None:
    child = _make_leaf_cell()
    parent = Cell( main_id = 'parent', ops = { 'kid': child }, source = [ 'inp' ], target = [ 'out' ] )
    parent.register_node( 'kid', source = [ 'inp' ], target = [ 'out' ] )

    parent( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 1 ).result( timeout = 5 )
    lines = capsys.readouterr().out.splitlines()

    # enter lines: no leading '/' anywhere, path grows one segment per level
    assert "parent ('inp',)" in lines
    assert "parent/leaf ('inp',)" in lines
    assert "parent/leaf/double ('inp',)" in lines
    # exits: single leading '/' marks exit, regardless of nesting depth
    assert "/parent/leaf/double ('out',)" in lines
    assert "/parent/leaf ('out',)" in lines
    assert "/parent ('out',)" in lines
    # nested Cell child ('leaf', kind='cell') must NOT get a duplicate
    # leaf-op-style enter line from the parent's _WiringEngine -- only its
    # own __call__'s line, never a second, op-shaped one also emitted by
    # _dispatch for a kind='cell' row.
    assert lines.count( "parent/leaf ('inp',)" ) == 1


def test_verbose_1_respects_incoming_verbose_prefix( capsys: pytest.CaptureFixture ) -> None:
    c = _make_leaf_cell()
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), verbose = 1, verbose_prefix = '[outer]' ).result( timeout = 5 )
    lines = capsys.readouterr().out.splitlines()
    assert "[outer]/leaf ('inp',)" in lines
    assert "/[outer]/leaf ('out',)" in lines
