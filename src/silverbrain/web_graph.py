#
#//  web_graph.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/10/26.
#

"""
Higher-level construction operations for building multi-web topologies.

split_web    —  bisect a TableProcessSequence across two Webs
join_web     —  create a new Web that receives from two existing Webs and fires a
                merge process once both inputs have arrived
combine_web  —  merge two Webs into one; bind to both original addresses so
                external webs routing to either original remain unchanged
"""

import polars as pl

from . import types
from .web import Web
from .tableProcesses import TableProcessRef, TableProcessSequence
from .web_transport import make_send_op
from .schema import table_schemas

# -- Internal helpers

def _identity_op(
    dfs: tuple[ pl.DataFrame, ... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame, ... ]:
    return dfs
#/def _identity_op

def _make_routes_row(
    web_id: str,
    op_id: str,
    address: str,
    ) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'web_id':  [ web_id ],
            'op_id':   [ op_id ],
            'address': [ address ],
            'topic':   [ '' ],
        },
        schema = table_schemas[ 'routes' ],
    )
#/def _make_routes_row

def _append_route(
    tables: dict[ str, pl.DataFrame | None ],
    web_id: str,
    op_id: str,
    address: str,
    ) -> None:
    """Add one route row to tables['routes'], creating the table if absent."""
    row = _make_routes_row( web_id, op_id, address )
    if tables.get( 'routes' ) is None:
        tables[ 'routes' ] = row
    else:
        tables[ 'routes' ] = pl.concat(
            [ tables[ 'routes' ], row ],
            how = 'vertical',
        )
#/def _append_route

def _make_join_recv_op(
    web_c_tables: dict[ str, pl.DataFrame | None ],
    recv_names: list[ str ],
    all_names: list[ str ],
    merge_process: types.TaggedTableProcess,
    web_c_tableOps_ref: dict,
    ) -> types.TableProcess:
    """
    Returns a TableProcess that:
      1. Stores the received dfs directly into web_c.tables under recv_names.
      2. If every name in all_names is now non-None in web_c.tables, runs
         merge_process using the stored tables, and stores the results.

    Closes over web_c.tables and web_c.tableOps so live mutations (from later
    tableOp registrations) are always visible.
    """
    def recv_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:

        # 1. Store received tables
        for name, df in zip( recv_names, dfs ):
            web_c_tables[ name ] = df
        #

        # 2. Fire merge if all inputs are present
        if all( web_c_tables.get( k ) is not None for k in all_names ):
            merge_dfs = tuple( web_c_tables[ k ] for k in merge_process.source )
            merge_result = merge_process(
                merge_dfs,
                web_c_tableOps_ref,
                verbose,
                verbose_prefix,
            )
            for name, df in zip( merge_process.target, merge_result ):
                web_c_tables[ name ] = df
            #
        #

        return ()
    #/def recv_op
    return recv_op
#/def _make_join_recv_op

# -- Main Functions

def split_web(
    web: Web,
    process_id: str,
    step_idx: int,
    web_a_id: str,
    web_b_id: str,
    address_b: str,
    recv_op_id: str = 'receive_split',
    context_tables_b: dict[ str, pl.DataFrame | None ] | None = None,
    ) -> tuple[ Web, Web ]:
    """
    Split a TableProcessSequence across two Webs.

    web_a runs terms[0..step_idx] then sends the outputs to web_b.
    web_b receives those outputs and runs terms[step_idx+1..].

    Parameters
    ----------
    web:
        Source web.  Its tableProcesses[process_id] must be a TableProcessSequence.
    process_id:
        Key of the process to split.
    step_idx:
        Run terms[0..step_idx] on web_a; the rest on web_b.
    web_a_id, web_b_id:
        main_id values for the two resulting Webs.
    address_b:
        ZMQ address that web_b will bind.  Added to web_a's routes table so
        OutboxSender can reach web_b.
    recv_op_id:
        The opId/inputId web_b exposes for the incoming message.
    context_tables_b:
        Extra tables needed by web_b's steps (e.g. 'paths').  The caller is
        responsible for supplying these; only the step_idx outputs are
        transmitted automatically.
    """
    seq = web.tableProcesses[ process_id ]
    if not isinstance( seq, TableProcessSequence ):
        raise TypeError(
            "tableProcesses['{}'] must be a TableProcessSequence".format( process_id )
        )
    #

    transmitted: list[ str ] = list( seq.terms[ step_idx ].target )

    # -- web_a

    outbox_a_tables: dict[ str, pl.DataFrame | None ] = dict( web.tables )
    _append_route( outbox_a_tables, web_b_id, recv_op_id, address_b )

    web_a = Web(
        main_id        = web_a_id,
        rng            = web.rng,
        inputIds       = list( web.inputIds ),
        tables         = outbox_a_tables,
        tableOps       = dict( web.tableOps ),
        tableProcesses = { k: v for k, v in web.tableProcesses.items() if k != process_id },
        verbose        = web.verbose,
    )

    send_ref = TableProcessRef(
        source = transmitted,
        target = [],
        op     = make_send_op( web_b_id, recv_op_id, web_a.outbox ),
        opId   = 'send_split',
    )

    web_a.tableProcesses[ process_id ] = TableProcessSequence(
        source = list( seq.source ),
        target = [],
        opId   = process_id,
        terms  = list( seq.terms[ :step_idx + 1 ] ) + [ send_ref ],
    )

    # -- web_b

    tables_b: dict[ str, pl.DataFrame | None ] = dict( context_tables_b or {} )
    # Pre-seed target keys so process_inbox_once (allow_new=False) doesn't reject them
    for t in seq.target:
        if t not in tables_b:
            tables_b[ t ] = None

    web_b = Web(
        main_id        = web_b_id,
        rng            = web.rng,
        inputIds       = [ recv_op_id ],
        tables         = tables_b,
        tableOps       = dict( web.tableOps ),
        tableProcesses = {
            recv_op_id: TableProcessSequence(
                source = transmitted,
                target = list( seq.target ),
                opId   = recv_op_id,
                terms  = list( seq.terms[ step_idx + 1: ] ),
            ),
        },
        verbose        = web.verbose,
    )

    return web_a, web_b
#/def split_web

def join_web(
    web_a: Web,
    web_b: Web,
    web_c_id: str,
    recv_a_op_id: str,
    recv_b_op_id: str,
    tables_a: list[ str ],
    tables_b: list[ str ],
    merge_process: types.TaggedTableProcess,
    address_c: str,
    ) -> tuple[ Web, pl.DataFrame ]:
    """
    Create web_c that receives from web_a and web_b and fires merge_process
    once both inputs have arrived.

    Parameters
    ----------
    web_a, web_b:
        Existing Webs that will send to web_c.  They are NOT mutated here;
        the caller adds the returned route_row to each sender's routes table.
    web_c_id:
        main_id for the new Web.
    recv_a_op_id, recv_b_op_id:
        opId values web_a and web_b use when addressing web_c's inbox.
    tables_a, tables_b:
        Ordered lists of table names carried in each sender's message.  These
        become the keys under which the data is stored in web_c.tables.
    merge_process:
        TaggedTableProcess run once both inputs are present.  Its source must
        reference names from tables_a + tables_b.
    address_c:
        ZMQ address web_c will bind.

    Returns
    -------
    web_c:
        Newly constructed Web.
    route_row:
        A two-row DataFrame (schema = table_schemas['routes']) with entries
        for recv_a_op_id and recv_b_op_id, both pointing to address_c.
        The caller does::

            web_a.tables['routes'] = pl.concat([web_a.tables['routes'], route_row])
            web_b.tables['routes'] = pl.concat([web_b.tables['routes'], route_row])
    """
    all_names = tables_a + tables_b

    # Seed tables with None placeholders so the merge check works correctly
    tables_c: dict[ str, pl.DataFrame | None ] = { k: None for k in all_names }
    tableOps_c: dict = {}

    web_c = Web(
        main_id        = web_c_id,
        inputIds       = [ recv_a_op_id, recv_b_op_id ],
        tables         = tables_c,
        tableOps       = tableOps_c,
        tableProcesses = {},
        verbose        = max( web_a.verbose, web_b.verbose ),
    )

    # Build closure-based receive ops (close over web_c.tables and web_c.tableOps)
    recv_a_op = _make_join_recv_op( web_c.tables, tables_a, all_names, merge_process, web_c.tableOps )
    recv_b_op = _make_join_recv_op( web_c.tables, tables_b, all_names, merge_process, web_c.tableOps )

    web_c.tableProcesses[ recv_a_op_id ] = TableProcessRef(
        source = tables_a,
        target = [],
        op     = recv_a_op,
        opId   = recv_a_op_id,
    )
    web_c.tableProcesses[ recv_b_op_id ] = TableProcessRef(
        source = tables_b,
        target = [],
        op     = recv_b_op,
        opId   = recv_b_op_id,
    )

    route_row = pl.DataFrame(
        {
            'web_id':  [ web_c_id,      web_c_id      ],
            'op_id':   [ recv_a_op_id,  recv_b_op_id  ],
            'address': [ address_c,     address_c     ],
            'topic':   [ '',            ''            ],
        },
        schema = table_schemas[ 'routes' ],
    )

    return web_c, route_row
#/def join_web

def combine_web(
    web_a: Web,
    web_b: Web,
    web_id: str,
    ) -> Web:
    """
    Merge two Webs into one.

    The combined Web inherits all tableProcesses, tableOps, tables, and
    inputIds from both originals.  Because `WebReceiver._run` filters only
    on `op_id` (not the `web_id` in the envelope), binding the combined web
    to **both original ZMQ addresses** makes all external senders agnostic:
    their routes tables need no changes.

    Typical usage::

        web_combined = combine_web(web_a, web_b, 'combined')

        web_a.stop()
        web_b.stop()

        web_combined.bind(address=address_a, sender=sender, context=ctx)
        web_combined.bind(address=address_b, sender=sender, context=ctx)
        web_combined.start()

    Raises
    ------
    ValueError
        If web_a and web_b share any inputId, tableProcess key, tableOp key,
        or non-routes table key (ambiguous merge).
    """
    # -- inputIds

    dup_inputs = set( web_a.inputIds ) & set( web_b.inputIds )
    if dup_inputs:
        raise ValueError(
            "Cannot combine: duplicate inputIds {}".format( sorted( dup_inputs ) )
        )
    #

    # -- tableProcesses

    dup_procs = set( web_a.tableProcesses ) & set( web_b.tableProcesses )
    if dup_procs:
        raise ValueError(
            "Cannot combine: duplicate tableProcesses keys {}".format( sorted( dup_procs ) )
        )
    #

    # -- tableOps

    dup_ops = set( web_a.tableOps ) & set( web_b.tableOps )
    if dup_ops:
        raise ValueError(
            "Cannot combine: duplicate tableOps keys {}".format( sorted( dup_ops ) )
        )
    #

    # -- tables

    non_route_a = { k: v for k, v in web_a.tables.items() if k != 'routes' }
    non_route_b = { k: v for k, v in web_b.tables.items() if k != 'routes' }

    dup_tables = set( non_route_a ) & set( non_route_b )
    if dup_tables:
        raise ValueError(
            "Cannot combine: duplicate table keys {}".format( sorted( dup_tables ) )
        )
    #

    tables_combined: dict[ str, pl.DataFrame | None ] = { **non_route_a, **non_route_b }

    routes_a = web_a.tables.get( 'routes' )
    routes_b = web_b.tables.get( 'routes' )

    if routes_a is not None or routes_b is not None:
        parts = [ r for r in ( routes_a, routes_b ) if r is not None ]
        merged_routes = pl.concat( parts, how = 'vertical' ) if len( parts ) > 1 else parts[ 0 ]
        # Deduplicate on (web_id, op_id) — keep first occurrence
        tables_combined[ 'routes' ] = merged_routes.unique(
            subset = [ 'web_id', 'op_id' ],
            keep   = 'first',
        )
    #

    # -- build combined web --------------------------------------------------

    return Web(
        main_id        = web_id,
        rng            = web_a.rng,
        inputIds       = list( web_a.inputIds ) + list( web_b.inputIds ),
        tables         = tables_combined,
        tableOps       = { **web_a.tableOps,       **web_b.tableOps       },
        tableProcesses = { **web_a.tableProcesses, **web_b.tableProcesses },
        verbose        = max( web_a.verbose, web_b.verbose ),
    )
#/def combine_web

def partition_out(
    web: Web,
    op_ids: list[ str ],
    new_web_id: str,
    new_address: str,
    context_tables: dict[ str, pl.DataFrame | None ] | None = None,
    ) -> Web:
    """
    Move a subset of a Web's tableProcesses to a new sub-Web in-place.

    For each op_id in ``op_ids``, the original process is extracted from
    ``web`` and placed in the returned sub-Web.  A forwarding stub replaces
    it in ``web``: the stub has the same ``source`` length so the inbox
    dispatch check passes, but its op is a send_op that routes the incoming
    dfs through ``web.outbox`` to the sub-Web's address.

    ``web.inputIds`` is **unchanged** — the web still accepts those op_ids
    at its ZMQ address, so external senders need no route-table updates.

    The caller binds and starts the sub-Web then stops and absorbs it later:

        sub = partition_out(web, ['op_a'], 'sub', 'inproc://sub_in')
        sub.bind('inproc://sub_in', sender, ctx)
        sub.start()
        ...
        sub.stop()
        sub.join()
        web.absorb_back( sub )

    Parameters
    ----------
    web:
        Web to partition (mutated in-place).
    op_ids:
        Keys in web.tableProcesses to move out.
    new_web_id:
        main_id for the returned sub-Web.
    new_address:
        ZMQ address the sub-Web will bind to.  Added to web.tables['routes'].
    context_tables:
        Optional tables to seed into the sub-Web (e.g. 'paths').
    """
    for op_id in op_ids:
        if op_id not in web.tableProcesses:
            raise ValueError(
                "op_id '{}' not in web.tableProcesses".format( op_id )
            )
    #

    original_processes: dict[ str, types.TaggedTableProcess ] = {
        op_id: web.tableProcesses[ op_id ]
        for op_id in op_ids
    }

    # Replace each real process with a forwarding stub
    for op_id in op_ids:
        original = original_processes[ op_id ]
        stub = TableProcessRef(
            source = list( original.source ),
            target = [],
            op     = make_send_op( new_web_id, op_id, web.outbox ),
            opId   = op_id,
        )
        web.tableProcesses[ op_id ] = stub
        _append_route( web.tables, new_web_id, op_id, new_address )
    #

    return Web(
        main_id        = new_web_id,
        inputIds       = list( op_ids ),
        tables         = dict( context_tables or {} ),
        tableOps       = dict( web.tableOps ),
        tableProcesses = original_processes,
        verbose        = web.verbose,
    )
#/def partition_out

