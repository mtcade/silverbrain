#
#//  test_pair_webs.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/8/26.
#
#   Two webs communicating via ZMQ inproc (shared memory within the process):
#
#     web_a  →  generate numbers [1,2,3,4,5]  →  send to web_b.compute
#     web_b  →  double each value             →  send to web_a.print_result
#     web_a  →  print result and record it
#

import threading
from queue import Queue

import polars as pl
import zmq

from silverbrain.web import Web
from silverbrain.tableProcesses import TableProcessRef, TableProcessSequence
from silverbrain.schema import table_schemas
from silverbrain.web_transport import WebSender, make_send_op


def test_pair_webs_arithmetic() -> None:

    # Shared ZMQ context — required for inproc:// (shared memory) transport
    ctx = zmq.Context()

    # Queues created up front so make_send_op can capture them before Web construction
    outbox_a: Queue = Queue()
    outbox_b: Queue = Queue()

    # Routes tables: each web needs to know the inproc address of the other
    routes_schema = table_schemas[ 'routes' ]

    routes_a = pl.DataFrame(
        {
            'web_id':  [ 'web_b' ],
            'op_id':   [ 'compute' ],
            'address': [ 'inproc://web_b_in' ],
            'topic':   [ '' ],
        },
        schema = routes_schema,
    )

    routes_b = pl.DataFrame(
        {
            'web_id':  [ 'web_a' ],
            'op_id':   [ 'print_result' ],
            'address': [ 'inproc://web_a_in' ],
            'topic':   [ '' ],
        },
        schema = routes_schema,
    )

    # Synchronisation: print_result sets this so the test can assert
    result_received = threading.Event()
    captured: list[ list[ int ] ] = []

    # -- tableOps --

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

    def print_result_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple:
        df = dfs[ 0 ]
        print( "Result received by web_a:" )
        print( df )
        captured.append( df[ 'value' ].to_list() )
        result_received.set()
        return ()
    #/def print_result_op

    # -- Web B: receives numbers, doubles them, sends result back to web_a --

    web_b = Web(
        main_id = 'web_b',
        inputIds = [ 'compute', ],
        tables = { 'routes': routes_b },
        outbox = outbox_b,
        tableOps = { 'double': double_op },
        tableProcesses = {
            'compute': TableProcessSequence(
                source = [ 'received' ],
                target = [],
                op_id   = 'compute',
                terms  = [
                    TableProcessRef(
                        source = [ 'received' ],
                        target = [ 'result' ],
                        op     = 'double',
                        op_id   = 'double_step',
                    ),
                    TableProcessRef(
                        source = [ 'result' ],
                        target = [],
                        op     = make_send_op( 'web_a', 'print_result', outbox_b ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
        },
    )

    # -- Web A: generates numbers, sends to web_b, receives and prints result --

    web_a = Web(
        main_id = 'web_a',
        inputIds = [ 'print_result' ],
        tables = { 'routes': routes_a },
        outbox = outbox_a,
        tableOps = {
            'make_numbers': make_numbers_op,
            'print_result': print_result_op,
        },
        tableProcesses = {
            'generate_and_send': TableProcessSequence(
                source = [],
                target = [],
                op_id   = 'generate_and_send',
                terms  = [
                    TableProcessRef(
                        source = [],
                        target = [ 'numbers' ],
                        op     = 'make_numbers',
                        op_id   = 'generate_step',
                    ),
                    TableProcessRef(
                        source = [ 'numbers' ],
                        target = [],
                        op     = make_send_op( 'web_b', 'compute', outbox_a ),
                        op_id   = 'send_step',
                    ),
                ],
            ),
            'print_result': TableProcessRef(
                source = [ 'received' ],
                target = [],
                op     = 'print_result',
                op_id   = 'print_result',
            ),
        },
    )

    # -- Transport --

    sender = WebSender( context=ctx )

    web_a.bind(
        address='inproc://web_a_in',
        sender=sender,
        context=ctx,
    )
    web_b.bind(
        address='inproc://web_b_in',
        sender=sender,
        context=ctx,
    )
    
    web_a.start()
    web_b.start()

    # -- Run --

    # Web A generates numbers and dispatches them; heartbeats drive the rest
    web_a.run_id('generate_and_send',)
    result_received.wait( timeout=2.0 )

    # -- Assert --

    assert result_received.is_set(), "print_result was never called"
    assert captured == [ [ 2, 4, 6, 8, 10 ] ], f"Unexpected values: { captured }"

    # -- Cleanup --

    web_a.stop()
    web_b.stop()
    web_a.join( timeout=2.0 )
    web_b.join( timeout=2.0 )
    sender.close()
    ctx.term()
#/def test_pair_webs_arithmetic
