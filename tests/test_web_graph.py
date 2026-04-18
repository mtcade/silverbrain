#
#//  test_web_graph.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/10/26.
#
#   Tests for split_web and join_web in web_graph.py
#

import threading

import polars as pl
import zmq

from silverbrain.web import Web
from silverbrain.tableProcesses import TableProcessRef, TableProcessSequence
from silverbrain.schema import table_schemas
from silverbrain.web_transport import WebSender, make_send_op
from silverbrain.web_graph import split_web, join_web, combine_web, partition_out


# ---------------------------------------------------------------------------
# Shared tableOps
# ---------------------------------------------------------------------------

def make_numbers_op(
    dfs: tuple,
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
    return ( pl.DataFrame( { 'value': [ 1, 2, 3, 4, 5 ] } ), )
#/def make_numbers_op

def double_op(
    dfs: tuple[ pl.DataFrame, ... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
    return ( dfs[ 0 ].with_columns( pl.col( 'value' ) * 2 ), )
#/def double_op

def sum_op(
    dfs: tuple[ pl.DataFrame, ... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
    total = dfs[ 0 ][ 'value' ].sum()
    return ( pl.DataFrame( { 'total': [ total ] } ), )
#/def sum_op

def negate_op(
    dfs: tuple[ pl.DataFrame, ... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
    return ( dfs[ 0 ].with_columns( pl.col( 'value' ) * -1 ), )
#/def negate_op


# ---------------------------------------------------------------------------
# Test 1: split_web  (generate → double | sum + print)
# ---------------------------------------------------------------------------

def test_split_web() -> None:
    """
    Single-web pipeline: generate → double → sum
    Split after double (step_idx=1, after the second term).

    web_a: [generate_step, double_step] → sends 'doubled' to web_b
    web_b: receives 'doubled' → [sum_step] → stores 'total'
    """
    ctx = zmq.Context()
    captured: list = []
    done = threading.Event()

    def capture_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        captured.append( dfs[ 0 ][ 'total' ][ 0 ] )
        done.set()
        return ( dfs[ 0 ], )
    #/def capture_op

    # Build the single web
    original = Web(
        main_id = 'original',
        tables  = {},
        tableOps = {
            'make_numbers': make_numbers_op,
            'double':       double_op,
            'sum':          sum_op,
            'capture':      capture_op,
        },
        tableProcesses = {
            'pipeline': TableProcessSequence(
                source = [],
                target = [ 'total' ],
                op_id   = 'pipeline',
                terms  = [
                    TableProcessRef(
                        source = [],
                        target = [ 'numbers' ],
                        op     = 'make_numbers',
                        op_id   = 'generate_step',
                    ),
                    TableProcessRef(
                        source = [ 'numbers' ],
                        target = [ 'doubled' ],
                        op     = 'double',
                        op_id   = 'double_step',
                    ),
                    TableProcessRef(
                        source = [ 'doubled' ],
                        target = [ 'total' ],
                        op     = 'sum',
                        op_id   = 'sum_step',
                    ),
                    TableProcessRef(
                        source = [ 'total' ],
                        target = [ 'total' ],
                        op     = 'capture',
                        op_id   = 'capture_step',
                    ),
                ],
            ),
        },
    )

    # Split after double_step (index 1)
    web_a, web_b = split_web(
        web        = original,
        process_id = 'pipeline',
        step_idx   = 1,
        web_a_id   = 'web_a',
        web_b_id   = 'web_b',
        address_b  = 'inproc://wb_in',
    )

    sender = WebSender( context=ctx )
    web_a.bind( address='inproc://wa_in', sender=sender, context=ctx )
    web_b.bind( address='inproc://wb_in', sender=sender, context=ctx )
    web_a.start()
    web_b.start()

    web_a.run_id( 'pipeline', allow_new=True )
    done.wait( timeout=2.0 )

    web_a.stop()
    web_b.stop()
    web_a.join( timeout=2.0 )
    web_b.join( timeout=2.0 )
    sender.close()
    ctx.term()

    assert done.is_set(), "capture_op was never called"
    assert captured == [ 30 ], f"Unexpected total: { captured }"
#/def test_split_web


# ---------------------------------------------------------------------------
# Test 2: join_web  (two independent webs → web_c concatenates)
# ---------------------------------------------------------------------------

def test_join_web() -> None:
    """
    web_a: generates [1, 2, 3] and sends to web_c as 'from_a'
    web_b: generates [10,20,30] and sends to web_c as 'from_b'
    web_c: receives both, concatenates, captures result
    """
    ctx = zmq.Context()
    captured: list = []
    done = threading.Event()

    routes_schema = table_schemas[ 'routes' ]

    def gen_a_op( dfs, verbose=0, verbose_prefix='' ):
        return ( pl.DataFrame( { 'value': [ 1, 2, 3 ] } ), )

    def gen_b_op( dfs, verbose=0, verbose_prefix='' ):
        return ( pl.DataFrame( { 'value': [ 10, 20, 30 ] } ), )

    outbox_a = web_a_outbox = __import__( 'queue' ).Queue()
    outbox_b = web_b_outbox = __import__( 'queue' ).Queue()

    # Routes will be updated after join_web is called
    web_a = Web(
        main_id   = 'web_a',
        tables    = { 'routes': pl.DataFrame(
            schema = routes_schema,
            data   = { 'web_id': [], 'op_id': [], 'address': [], 'topic': [] },
        ) },
        outbox    = outbox_a,
        tableOps  = { 'gen_a': gen_a_op },
        tableProcesses = {
            'produce': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'produce',
                terms  = [
                    TableProcessRef( source=[], target=[ 'data' ], op='gen_a', op_id='gen_a_step' ),
                    TableProcessRef(
                        source = [ 'data' ],
                        target = [],
                        op     = make_send_op( 'web_c', 'recv_a', outbox_a ),
                        op_id   = 'send_a',
                    ),
                ],
            ),
        },
    )

    web_b = Web(
        main_id   = 'web_b',
        tables    = { 'routes': pl.DataFrame(
            schema = routes_schema,
            data   = { 'web_id': [], 'op_id': [], 'address': [], 'topic': [] },
        ) },
        outbox    = outbox_b,
        tableOps  = { 'gen_b': gen_b_op },
        tableProcesses = {
            'produce': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'produce',
                terms  = [
                    TableProcessRef( source=[], target=[ 'data' ], op='gen_b', op_id='gen_b_step' ),
                    TableProcessRef(
                        source = [ 'data' ],
                        target = [],
                        op     = make_send_op( 'web_c', 'recv_b', outbox_b ),
                        op_id   = 'send_b',
                    ),
                ],
            ),
        },
    )

    def concat_and_capture(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple:
        merged = pl.concat( [ dfs[ 0 ], dfs[ 1 ] ] )
        captured.append( merged[ 'value' ].to_list() )
        done.set()
        return ()
    #/def concat_and_capture

    merge_process = TableProcessRef(
        source = [ 'from_a', 'from_b' ],
        target = [],
        op     = concat_and_capture,
        op_id   = 'merge_step',
    )

    web_c, route_row = join_web(
        web_a        = web_a,
        web_b        = web_b,
        web_c_id     = 'web_c',
        recv_a_op_id = 'recv_a',
        recv_b_op_id = 'recv_b',
        tables_a     = [ 'from_a' ],
        tables_b     = [ 'from_b' ],
        merge_process = merge_process,
        address_c    = 'inproc://wc_in',
    )

    web_a.tables[ 'routes' ] = pl.concat( [ web_a.tables[ 'routes' ], route_row ] )
    web_b.tables[ 'routes' ] = pl.concat( [ web_b.tables[ 'routes' ], route_row ] )

    sender = WebSender( context=ctx )
    web_a.bind( address='inproc://wa2_in', sender=sender, context=ctx )
    web_b.bind( address='inproc://wb2_in', sender=sender, context=ctx )
    web_c.bind( address='inproc://wc_in',  sender=sender, context=ctx )
    web_a.start()
    web_b.start()
    web_c.start()

    web_a.run_id( 'produce' )
    web_b.run_id( 'produce' )
    done.wait( timeout=2.0 )

    web_a.stop()
    web_b.stop()
    web_c.stop()
    web_a.join( timeout=2.0 )
    web_b.join( timeout=2.0 )
    web_c.join( timeout=2.0 )
    sender.close()
    ctx.term()

    assert done.is_set(), "merge was never called"
    assert sorted( captured[ 0 ] ) == [ 1, 2, 3, 10, 20, 30 ], f"Unexpected: { captured }"
#/def test_join_web


# ---------------------------------------------------------------------------
# Test 3: diamond  (fan-out + join)
# ---------------------------------------------------------------------------

def test_diamond() -> None:
    """
    web_a → splits to web_b (double) and web_c (negate) → web_d joins both
    web_d gets [2,4,6,8,10] from web_b and [-1,-2,-3,-4,-5] from web_c
    """
    ctx = zmq.Context()
    captured: list = []
    done = threading.Event()

    routes_schema = table_schemas[ 'routes' ]

    def gen_op( dfs, verbose=0, verbose_prefix='' ):
        return ( pl.DataFrame( { 'value': [ 1, 2, 3, 4, 5 ] } ), )

    def make_empty_routes():
        return pl.DataFrame(
            schema = routes_schema,
            data   = { 'web_id': [], 'op_id': [], 'address': [], 'topic': [] },
        )

    outbox_a = __import__( 'queue' ).Queue()
    outbox_b = __import__( 'queue' ).Queue()
    outbox_c = __import__( 'queue' ).Queue()

    web_a = Web(
        main_id = 'web_a',
        tables  = { 'routes': make_empty_routes() },
        outbox  = outbox_a,
        tableOps = { 'gen': gen_op },
        tableProcesses = {
            'send_to_b': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'send_to_b',
                terms  = [
                    TableProcessRef( source=[], target=[ 'numbers' ], op='gen', op_id='gen_step_b' ),
                    TableProcessRef(
                        source = [ 'numbers' ],
                        target = [],
                        op     = make_send_op( 'web_b', 'recv_numbers', outbox_a ),
                        op_id   = 'send_b_step',
                    ),
                ],
            ),
            'send_to_c': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'send_to_c',
                terms  = [
                    TableProcessRef( source=[], target=[ 'numbers' ], op='gen', op_id='gen_step_c' ),
                    TableProcessRef(
                        source = [ 'numbers' ],
                        target = [],
                        op     = make_send_op( 'web_c', 'recv_numbers', outbox_a ),
                        op_id   = 'send_c_step',
                    ),
                ],
            ),
        },
    )

    web_b = Web(
        main_id  = 'web_b',
        inputIds = [ 'recv_numbers' ],
        tables   = { 'routes': make_empty_routes() },
        outbox   = outbox_b,
        tableOps = { 'double': double_op },
        tableProcesses = {
            'recv_numbers': TableProcessSequence(
                source = [ 'numbers' ],
                target = [],
                op_id   = 'recv_numbers',
                terms  = [
                    TableProcessRef( source=[ 'numbers' ], target=[ 'doubled' ], op='double', op_id='double_step' ),
                    TableProcessRef(
                        source = [ 'doubled' ],
                        target = [],
                        op     = make_send_op( 'web_d', 'recv_b', outbox_b ),
                        op_id   = 'send_d_step',
                    ),
                ],
            ),
        },
    )

    web_c = Web(
        main_id  = 'web_c',
        inputIds = [ 'recv_numbers' ],
        tables   = { 'routes': make_empty_routes() },
        outbox   = outbox_c,
        tableOps = { 'negate': negate_op },
        tableProcesses = {
            'recv_numbers': TableProcessSequence(
                source = [ 'numbers' ],
                target = [],
                op_id   = 'recv_numbers',
                terms  = [
                    TableProcessRef( source=[ 'numbers' ], target=[ 'negated' ], op='negate', op_id='negate_step' ),
                    TableProcessRef(
                        source = [ 'negated' ],
                        target = [],
                        op     = make_send_op( 'web_d', 'recv_c', outbox_c ),
                        op_id   = 'send_d_step',
                    ),
                ],
            ),
        },
    )

    def collect_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple:
        captured.append( ( dfs[ 0 ][ 'value' ].to_list(), dfs[ 1 ][ 'value' ].to_list() ) )
        done.set()
        return ()
    #/def collect_op

    merge_d = TableProcessRef(
        source = [ 'from_b', 'from_c' ],
        target = [],
        op     = collect_op,
        op_id   = 'collect_step',
    )

    web_d, route_row = join_web(
        web_a        = web_b,
        web_b        = web_c,
        web_c_id     = 'web_d',
        recv_a_op_id = 'recv_b',
        recv_b_op_id = 'recv_c',
        tables_a     = [ 'from_b' ],
        tables_b     = [ 'from_c' ],
        merge_process = merge_d,
        address_c    = 'inproc://wd_in',
    )

    web_b.tables[ 'routes' ] = pl.concat( [ web_b.tables[ 'routes' ], route_row ] )
    web_c.tables[ 'routes' ] = pl.concat( [ web_c.tables[ 'routes' ], route_row ] )

    # web_a also needs routes to web_b and web_c
    web_a.tables[ 'routes' ] = pl.concat( [
        web_a.tables[ 'routes' ],
        pl.DataFrame(
            { 'web_id': [ 'web_b', 'web_c' ], 'op_id': [ 'recv_numbers', 'recv_numbers' ],
              'address': [ 'inproc://wb3_in', 'inproc://wc3_in' ], 'topic': [ '', '' ] },
            schema = routes_schema,
        ),
    ] )

    sender = WebSender( context=ctx )
    web_a.bind( address='inproc://wa3_in', sender=sender, context=ctx )
    web_b.bind( address='inproc://wb3_in', sender=sender, context=ctx )
    web_c.bind( address='inproc://wc3_in', sender=sender, context=ctx )
    web_d.bind( address='inproc://wd_in',  sender=sender, context=ctx )
    web_a.start()
    web_b.start()
    web_c.start()
    web_d.start()

    web_a.run_id( 'send_to_b' )
    web_a.run_id( 'send_to_c' )
    done.wait( timeout=2.0 )

    web_a.stop()
    web_b.stop()
    web_c.stop()
    web_d.stop()
    web_a.join( timeout=2.0 )
    web_b.join( timeout=2.0 )
    web_c.join( timeout=2.0 )
    web_d.join( timeout=2.0 )
    sender.close()
    ctx.term()

    assert done.is_set(), "merge on web_d was never called"
    b_values, c_values = captured[ 0 ]
    assert b_values == [ 2, 4, 6, 8, 10 ], f"web_b output: { b_values }"
    assert c_values == [ -1, -2, -3, -4, -5 ], f"web_c output: { c_values }"
#/def test_diamond


# ---------------------------------------------------------------------------
# Test 4: combine_web  (two webs merged; source webs remain agnostic)
# ---------------------------------------------------------------------------

def test_combine_web() -> None:
    """
    web_compute: inputId 'compute', doubles values, sends to web_out
    web_train:   inputId 'train',   triples values, sends to web_out
    web_source_a routes to 'inproc://compute_in' (web_compute's address)
    web_source_b routes to 'inproc://train_in'   (web_train's address)

    After combining:
      - web_combined binds to BOTH original addresses
      - web_source_a and web_source_b routes are unchanged
      - Both send paths work correctly
    """
    ctx = zmq.Context()
    routes_schema = table_schemas[ 'routes' ]

    results: list = []
    compute_done = threading.Event()
    train_done   = threading.Event()

    outbox_compute = __import__( 'queue' ).Queue()
    outbox_train   = __import__( 'queue' ).Queue()
    outbox_out     = __import__( 'queue' ).Queue()

    def triple_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        return ( dfs[ 0 ].with_columns( pl.col( 'value' ) * 3 ), )
    #/def triple_op

    def make_empty_routes() -> pl.DataFrame:
        return pl.DataFrame(
            schema = routes_schema,
            data   = { 'web_id': [], 'op_id': [], 'address': [], 'topic': [] },
        )
    #/def make_empty_routes

    # -- web_out: collects results -------------------------------------------

    def collect_compute(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple:
        results.append( ( 'compute', dfs[ 0 ][ 'value' ].to_list() ) )
        compute_done.set()
        return ()
    #/def collect_compute

    def collect_train(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple:
        results.append( ( 'train', dfs[ 0 ][ 'value' ].to_list() ) )
        train_done.set()
        return ()
    #/def collect_train

    web_out = Web(
        main_id  = 'web_out',
        inputIds = [ 'result_compute', 'result_train' ],
        tables   = {},
        tableOps = { 'collect_compute': collect_compute, 'collect_train': collect_train },
        tableProcesses = {
            'result_compute': TableProcessRef(
                source = [ 'data' ], target = [], op = 'collect_compute', op_id = 'result_compute',
            ),
            'result_train': TableProcessRef(
                source = [ 'data' ], target = [], op = 'collect_train', op_id = 'result_train',
            ),
        },
    )

    # -- web_compute ---------------------------------------------------------

    web_compute = Web(
        main_id  = 'web_compute',
        inputIds = [ 'compute' ],
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_out' ], 'op_id': [ 'result_compute' ],
                  'address': [ 'inproc://out_in' ], 'topic': [ '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = outbox_compute,
        tableOps = { 'double': double_op },
        tableProcesses = {
            'compute': TableProcessSequence(
                source = [ 'data' ],
                target = [],
                op_id   = 'compute',
                terms  = [
                    TableProcessRef( source=[ 'data' ], target=[ 'result' ], op='double', op_id='double_step' ),
                    TableProcessRef(
                        source = [ 'result' ],
                        target = [],
                        op     = make_send_op( 'web_out', 'result_compute', outbox_compute ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
        },
    )

    # -- web_train -----------------------------------------------------------

    web_train = Web(
        main_id  = 'web_train',
        inputIds = [ 'train' ],
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_out' ], 'op_id': [ 'result_train' ],
                  'address': [ 'inproc://out_in' ], 'topic': [ '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = outbox_train,
        tableOps = { 'triple': triple_op },
        tableProcesses = {
            'train': TableProcessSequence(
                source = [ 'data' ],
                target = [],
                op_id   = 'train',
                terms  = [
                    TableProcessRef( source=[ 'data' ], target=[ 'result' ], op='triple', op_id='triple_step' ),
                    TableProcessRef(
                        source = [ 'result' ],
                        target = [],
                        op     = make_send_op( 'web_out', 'result_train', outbox_train ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
        },
    )

    # -- Source webs that route to the ORIGINAL addresses --------------------
    # These are never modified — they remain agnostic about the combine.

    src_a_outbox = __import__( 'queue' ).Queue()
    web_source_a = Web(
        main_id  = 'web_source_a',
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_compute' ], 'op_id': [ 'compute' ],
                  'address': [ 'inproc://compute_in' ], 'topic': [ '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = src_a_outbox,
        tableOps = { 'gen': make_numbers_op },
        tableProcesses = {
            'send_compute': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'send_compute',
                terms  = [
                    TableProcessRef( source=[], target=[ 'data' ], op='gen', op_id='gen_step' ),
                    TableProcessRef(
                        source = [ 'data' ],
                        target = [],
                        op     = make_send_op( 'web_compute', 'compute', src_a_outbox ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
        },
    )

    src_b_outbox = __import__( 'queue' ).Queue()
    web_source_b = Web(
        main_id  = 'web_source_b',
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_train' ], 'op_id': [ 'train' ],
                  'address': [ 'inproc://train_in' ], 'topic': [ '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = src_b_outbox,
        tableOps = { 'gen': make_numbers_op },
        tableProcesses = {
            'send_train': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'send_train',
                terms  = [
                    TableProcessRef( source=[], target=[ 'data' ], op='gen', op_id='gen_step' ),
                    TableProcessRef(
                        source = [ 'data' ],
                        target = [],
                        op     = make_send_op( 'web_train', 'train', src_b_outbox ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
        },
    )

    # -- Combine and start ---------------------------------------------------

    web_combined = combine_web( web_compute, web_train, web_id='combined' )

    sender = WebSender( context=ctx )

    # Combined web binds to BOTH original addresses
    web_combined.bind( address='inproc://compute_in', sender=sender, context=ctx )
    web_combined.bind( address='inproc://train_in',   sender=sender, context=ctx )
    # Inherited send_ops write to the original outboxes — add OutboxSenders for each
    web_combined.bind_outbox( outbox_compute, sender )
    web_combined.bind_outbox( outbox_train,   sender )
    web_out.bind(      address='inproc://out_in',     sender=sender, context=ctx )
    web_source_a.bind( address='inproc://src_a_in',   sender=sender, context=ctx )
    web_source_b.bind( address='inproc://src_b_in',   sender=sender, context=ctx )

    web_combined.start()
    web_out.start()
    web_source_a.start()
    web_source_b.start()

    # Sources use unchanged routes — they route to the original addresses
    web_source_a.run_id( 'send_compute' )
    web_source_b.run_id( 'send_train' )

    compute_done.wait( timeout=2.0 )
    train_done.wait( timeout=2.0 )

    web_combined.stop()
    web_out.stop()
    web_source_a.stop()
    web_source_b.stop()
    web_combined.join( timeout=2.0 )
    web_out.join( timeout=2.0 )
    web_source_a.join( timeout=2.0 )
    web_source_b.join( timeout=2.0 )
    sender.close()
    ctx.term()

    assert compute_done.is_set(), "compute result never arrived"
    assert train_done.is_set(),   "train result never arrived"

    result_map = { tag: vals for tag, vals in results }
    assert result_map[ 'compute' ] == [ 2, 4, 6, 8, 10 ], f"compute: { result_map['compute'] }"
    assert result_map[ 'train'   ] == [ 3, 6, 9, 12, 15 ], f"train: { result_map['train'] }"
#/def test_combine_web


# ---------------------------------------------------------------------------
# Test 5: partition_out + absorb_back
# ---------------------------------------------------------------------------

def test_partition_absorb() -> None:
    """
    web_main has two inputIds: 'double_it' and 'triple_it'.
    web_source routes both op_ids to inproc://main_in — never modified.

    Phase 1 (partition):
      triple_it is partitioned out to sub_web at inproc://sub_in.
      web_main still accepts 'triple_it' at inproc://main_in but forwards it.
      Both ops must still produce correct results at web_out.

    Phase 2 (absorb):
      sub_web is absorbed back; triple_it now runs directly in web_main again.
      Result must still be correct.
    """
    ctx = zmq.Context()
    routes_schema = table_schemas[ 'routes' ]

    def triple_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        return ( dfs[ 0 ].with_columns( pl.col( 'value' ) * 3 ), )
    #/def triple_op

    # -- web_out: collects arithmetic results --------------------------------

    double_results: list = []
    triple_results: list = []
    double_done = threading.Event()
    triple_done = threading.Event()

    out_outbox = __import__( 'queue' ).Queue()

    def collect_double( dfs, verbose=0, verbose_prefix='' ):
        double_results.append( dfs[ 0 ][ 'value' ].to_list() )
        double_done.set()
        return ()

    def collect_triple( dfs, verbose=0, verbose_prefix='' ):
        triple_results.append( dfs[ 0 ][ 'value' ].to_list() )
        triple_done.set()
        return ()

    web_out = Web(
        main_id  = 'web_out',
        inputIds = [ 'result_double', 'result_triple' ],
        tables   = { 'routes': pl.DataFrame(
            schema = routes_schema,
            data   = { 'web_id': [], 'op_id': [], 'address': [], 'topic': [] },
        ) },
        outbox   = out_outbox,
        tableOps = { 'col_double': collect_double, 'col_triple': collect_triple },
        tableProcesses = {
            'result_double': TableProcessRef(
                source=['data'], target=[], op='col_double', op_id='result_double',
            ),
            'result_triple': TableProcessRef(
                source=['data'], target=[], op='col_triple', op_id='result_triple',
            ),
        },
    )

    # -- web_main: two ops, both sending to web_out --------------------------

    main_outbox = __import__( 'queue' ).Queue()

    web_main = Web(
        main_id  = 'web_main',
        inputIds = [ 'double_it', 'triple_it' ],
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_out', 'web_out' ],
                  'op_id':  [ 'result_double', 'result_triple' ],
                  'address': [ 'inproc://out_in', 'inproc://out_in' ],
                  'topic':  [ '', '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = main_outbox,
        tableOps = { 'double': double_op, 'triple': triple_op },
        tableProcesses = {
            'double_it': TableProcessSequence(
                source = [ 'data' ], target = [], op_id = 'double_it',
                terms  = [
                    TableProcessRef( source=[ 'data' ], target=[ 'result' ], op='double', op_id='d_step' ),
                    TableProcessRef(
                        source=[ 'result' ], target=[],
                        op=make_send_op( 'web_out', 'result_double', main_outbox ),
                        op_id='send_double',
                    ),
                ],
            ),
            'triple_it': TableProcessSequence(
                source = [ 'data' ], target = [], op_id = 'triple_it',
                terms  = [
                    TableProcessRef( source=[ 'data' ], target=[ 'result' ], op='triple', op_id='t_step' ),
                    TableProcessRef(
                        source=[ 'result' ], target=[],
                        op=make_send_op( 'web_out', 'result_triple', main_outbox ),
                        op_id='send_triple',
                    ),
                ],
            ),
        },
    )

    # -- web_source: routes both ops to web_main's address (never modified) --

    src_outbox = __import__( 'queue' ).Queue()

    web_source = Web(
        main_id  = 'web_source',
        tables   = {
            'routes': pl.DataFrame(
                { 'web_id': [ 'web_main', 'web_main' ],
                  'op_id':  [ 'double_it', 'triple_it' ],
                  'address': [ 'inproc://main_in', 'inproc://main_in' ],
                  'topic':  [ '', '' ] },
                schema = routes_schema,
            ),
        },
        outbox   = src_outbox,
        tableOps = { 'gen': make_numbers_op },
        tableProcesses = {
            'send_double': TableProcessSequence(
                source=[], target=[], op_id='send_double',
                terms=[
                    TableProcessRef( source=[], target=[ 'data' ], op='gen', op_id='gen_d' ),
                    TableProcessRef(
                        source=[ 'data' ], target=[],
                        op=make_send_op( 'web_main', 'double_it', src_outbox ),
                        op_id='fwd_d',
                    ),
                ],
            ),
            'send_triple': TableProcessSequence(
                source=[], target=[], op_id='send_triple',
                terms=[
                    TableProcessRef( source=[], target=[ 'data' ], op='gen', op_id='gen_t' ),
                    TableProcessRef(
                        source=[ 'data' ], target=[],
                        op=make_send_op( 'web_main', 'triple_it', src_outbox ),
                        op_id='fwd_t',
                    ),
                ],
            ),
        },
    )

    sender = WebSender( context=ctx )
    web_main.bind(   address='inproc://main_in', sender=sender, context=ctx )
    web_out.bind(    address='inproc://out_in',  sender=sender, context=ctx )
    web_source.bind( address='inproc://src_in',  sender=sender, context=ctx )
    web_main.start()
    web_out.start()
    web_source.start()

    # -- Phase 1: partition triple_it out ------------------------------------

    sub_web = partition_out(
        web       = web_main,
        op_ids    = [ 'triple_it' ],
        new_web_id = 'sub',
        new_address = 'inproc://sub_in',
    )
    sub_web.bind( address='inproc://sub_in', sender=sender, context=ctx )
    # sub_web's send_op also writes to main_outbox (via forwarding stub),
    # but its OWN send_ops write to sub_web.outbox — add a sender for that
    sub_web.bind_outbox( sub_web.outbox, sender )
    sub_web.start()

    # Both ops reach web_out via different paths
    web_source.run_id( 'send_double' )
    double_done.wait( timeout=2.0 )
    triple_done.clear()
    web_source.run_id( 'send_triple' )
    triple_done.wait( timeout=2.0 )

    assert double_done.is_set(), "double_it never reached web_out"
    assert triple_done.is_set(), "triple_it never reached web_out after partition"
    assert double_results[ -1 ] == [ 2, 4, 6, 8, 10 ]
    assert triple_results[ -1 ] == [ 3, 6, 9, 12, 15 ]

    # -- Phase 2: absorb sub_web back ----------------------------------------

    sub_web.stop()
    sub_web.join( timeout=2.0 )
    web_main.absorb_back( sub_web )

    triple_done.clear()
    web_source.run_id( 'send_triple' )
    triple_done.wait( timeout=2.0 )

    assert triple_done.is_set(), "triple_it never reached web_out after absorb"
    assert triple_results[ -1 ] == [ 3, 6, 9, 12, 15 ]

    # Verify web_source.tables['routes'] was never touched
    src_routes = web_source.tables[ 'routes' ]
    assert set( src_routes[ 'address' ].to_list() ) == { 'inproc://main_in' }

    # -- Cleanup -------------------------------------------------------------

    web_main.stop()
    web_out.stop()
    web_source.stop()
    web_main.join( timeout=2.0 )
    web_out.join( timeout=2.0 )
    web_source.join( timeout=2.0 )
    sender.close()
    ctx.term()
#/def test_partition_absorb
