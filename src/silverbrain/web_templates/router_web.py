#
#//  router_web.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/8/26.
#

import polars as pl

from ..webOld import Web
from ..tableProcesses import TableProcessRef
from ..schema import table_schemas


def _empty_routes() -> pl.DataFrame:
    return pl.DataFrame(
        schema = table_schemas[ 'routes' ],
    )
#/def _empty_routes


def _merge_routes_op(
    dfs: tuple[ pl.DataFrame, ... ],
    verbose: int = 0,
    verbose_prefix: str = '',
) -> tuple[ pl.DataFrame ]:
    """
        Upsert route_updates into routes on (web_id, op_id).
        Rows in route_updates replace matching rows; new rows are appended.
    """
    route_updates, routes = dfs
    merged = routes.update( route_updates, on=[ 'web_id', 'op_id' ], how='full' )
    return ( merged, )
#/def _merge_routes_op


def make_router_web(
    main_id: str = 'router',
    initial_routes: pl.DataFrame | None = None,
) -> Web:
    """
        Returns a Web configured as a live routing registry.

        Tables:
            'routes'        — canonical routing table {web_id, op_id, address, topic}
            'route_updates' — staging area; consumed by 'update_routes' process

        inputIds: ['update_routes']
            Push a DataFrame matching the routes schema to this opId to upsert
            new or changed routes at runtime.

        Usage:
            router = make_router_web()
            ctx = zmq.Context()
            net = LocalWebNode( router, input_ids=[ 'update_routes' ] )
            net.bind( 'inproc://router', ctx )
            net.start()
    """
    routes = initial_routes if initial_routes is not None else _empty_routes()

    return Web(
        main_id = main_id,
        inputIds = [ 'update_routes' ],
        tables = {
            'routes':        routes,
            'route_updates': _empty_routes(),
        },
        tableOps = {
            'merge_routes': _merge_routes_op,
        },
        tableProcesses = {
            'update_routes': TableProcessRef(
                source = [ 'route_updates', 'routes' ],
                target = [ 'routes' ],
                op     = 'merge_routes',
                op_id  = 'update_routes',
            ),
        },
    )
#/def make_router_web
