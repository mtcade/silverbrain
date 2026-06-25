#
#//  networkOps.py
#//  silverbrain
#//
#//  Created by Evan Mason on 6/25/26.
#//
"""
System TableOps and TableProcess factories for inter-node routing.

Every Cell that participates in a ZMQ network carries three additional system
tables (registered alongside the standard cell system tables):

    __node_routing_table__     — pipeline routing config (terminal vs. relay)
    __node_correlation_table__ — in-flight relay requests (request_id → origin)
    __node_connections__       — live DEALER socket handles

Incoming messages on the ROUTER socket are either:

  DATA   — a normal tuple[pl.DataFrame, ...] dispatched to domain processes
  SYSTEM — a tuple whose first table has a '__system_op__' column; dispatched
           to the system processes defined here

System process factories build TableProcessRef / TableProcessBranch descriptors
that can be registered into a Cell's __table_processes__ exactly like any other
process.  The corresponding ops are listed in NETWORK_BUILTIN_OPS and
registered automatically when the Cell is set up for network use.
"""

import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Self, Sequence

import polars as pl

from . import types
from .schema import table_schemas, cannonical_schema_for_keys
from .tableOps import TableOp, InputSchema, OutputSchema
from .tableProcesses import (
    TableProcessRef,
    TableCheckRef,
    TableProcessBranch,
    TableProcessSequence,
)


# ---------------------------------------------------------------------------
# System TableChecks
# ---------------------------------------------------------------------------

class IsSystemMessageOp:
    """
    True when dfs[0] carries a '__system_op__' discriminator column, marking
    the message as a routing control command rather than domain data.

    Source: any single incoming table (the first table of the received tuple).
    """
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return '__system_op__' in dfs[0].columns
#/class IsSystemMessageOp


class IsRoutingUpdateOp:
    """
    True when dfs[0] is a routing-update system message
    (``__system_op__ == 'routing_update'``).

    Source: [__inbound_system__]
    """
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['__system_op__'].item() == 'routing_update'
#/class IsRoutingUpdateOp


class IsConnectOp:
    """
    True when dfs[0] is a connect system message
    (``__system_op__ == 'connect'``).

    Source: [__inbound_system__]
    """
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['__system_op__'].item() == 'connect'
#/class IsConnectOp


class IsDisconnectOp:
    """
    True when dfs[0] is a disconnect system message
    (``__system_op__ == 'disconnect'``).

    Source: [__inbound_system__]
    """
    def __call__(
        self,
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> bool:
        return dfs[0]['__system_op__'].item() == 'disconnect'
#/class IsDisconnectOp


# ---------------------------------------------------------------------------
# System TableOps — routing table management
# ---------------------------------------------------------------------------

class UpdateRoutingTableOp( TableOp ):
    """
    Upsert one row into ``__node_routing_table__`` from an incoming
    routing-update system message.

    dfs[0]: __routing_update__ — single row with op_idn, mode, address
    dfs[1]: __node_routing_table__ — current routing config

    Returns updated __node_routing_table__.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema(
                    columns = {
                        '__system_op__': pl.Utf8,
                        'op_idn':        pl.Utf8,
                        'mode':          pl.Utf8,
                        'address':       pl.Utf8,
                    },
                    extra = 'open',
                ),
                InputSchema(
                    columns = table_schemas[ '__node_routing_table__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_routing_table__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        update, routing_table = dfs
        row = update.select( ['op_idn', 'mode', 'address'] )
        existing = routing_table.filter(
            pl.col( 'op_idn' ) != row['op_idn'].item()
        )
        return ( pl.concat( [existing, row], how='vertical' ), )
    #/def __call__
#/class UpdateRoutingTableOp


class LookupRoutingOp( TableOp ):
    """
    Resolve the routing entry for a given pipeline.

    dfs[0]: single-row table with 'op_idn' — the pipeline being dispatched
    dfs[1]: __node_routing_table__

    Returns a single-row table with columns (op_idn, mode, address).
    Raises KeyError if the pipeline is not registered; callers should ensure
    all pipelines have a default terminal entry.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema( columns = { 'op_idn': pl.Utf8 }, extra = 'open' ),
                InputSchema(
                    columns = table_schemas[ '__node_routing_table__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_routing_table__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        op_idn_val   = dfs[0]['op_idn'].item()
        routing_row  = dfs[1].filter( pl.col( 'op_idn' ) == op_idn_val )
        if routing_row.is_empty():
            raise KeyError( f"No routing entry for pipeline {op_idn_val!r}" )
        return ( routing_row, )
    #/def __call__
#/class LookupRoutingOp


# ---------------------------------------------------------------------------
# System TableOps — correlation table management
# ---------------------------------------------------------------------------

class RecordCorrelationOp( TableOp ):
    """
    Insert a new row into ``__node_correlation_table__`` to track a relay
    request in flight.

    dfs[0]: single-row table with (request_id, origin_identity)
    dfs[1]: __node_correlation_table__

    Returns updated __node_correlation_table__.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema(
                    columns = { 'request_id': pl.Utf8, 'origin_identity': pl.Binary },
                    extra   = 'open',
                ),
                InputSchema(
                    columns = table_schemas[ '__node_correlation_table__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_correlation_table__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        incoming, correlation_table = dfs
        new_row = pl.DataFrame(
            {
                'request_id':      [ incoming['request_id'].item() ],
                'origin_identity': [ incoming['origin_identity'].item() ],
                'received_at':     [ int( time.time() * 1000 ) ],
            },
            schema = table_schemas[ '__node_correlation_table__' ],
        )
        return ( pl.concat( [correlation_table, new_row], how='vertical' ), )
    #/def __call__
#/class RecordCorrelationOp


class LookupCorrelationOp( TableOp ):
    """
    Find and return the correlation row for a given request_id, so the relay
    node knows where to send the response.

    dfs[0]: single-row table with 'request_id'
    dfs[1]: __node_correlation_table__

    Returns a single-row table matching the __node_correlation_table__ schema.
    Raises KeyError if request_id is unknown.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema( columns = { 'request_id': pl.Utf8 }, extra = 'open' ),
                InputSchema(
                    columns = table_schemas[ '__node_correlation_table__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_correlation_table__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        request_id_val = dfs[0]['request_id'].item()
        row = dfs[1].filter( pl.col( 'request_id' ) == request_id_val )
        if row.is_empty():
            raise KeyError( f"No correlation entry for request_id {request_id_val!r}" )
        return ( row, )
    #/def __call__
#/class LookupCorrelationOp


class RemoveCorrelationOp( TableOp ):
    """
    Remove the correlation entry for a completed relay request.

    dfs[0]: single-row table with 'request_id'
    dfs[1]: __node_correlation_table__

    Returns updated __node_correlation_table__ with the entry removed.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema( columns = { 'request_id': pl.Utf8 }, extra = 'open' ),
                InputSchema(
                    columns = table_schemas[ '__node_correlation_table__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_correlation_table__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        request_id_val = dfs[0]['request_id'].item()
        return (
            dfs[1].filter( pl.col( 'request_id' ) != request_id_val ),
        )
    #/def __call__
#/class RemoveCorrelationOp


# ---------------------------------------------------------------------------
# System TableOps — connection management (ZMQ stubs)
# ---------------------------------------------------------------------------

class ConnectOp( TableOp ):
    """
    Establish a new ZMQ DEALER connection to a peer and record it in
    ``__node_connections__``.

    dfs[0]: __connect__ system message — single row with address
    dfs[1]: __node_connections__

    Returns updated __node_connections__.

    UC: ZMQ socket management not yet implemented.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema(
                    columns = { '__system_op__': pl.Utf8, 'address': pl.Utf8 },
                    extra   = 'open',
                ),
                InputSchema(
                    columns = table_schemas[ '__node_connections__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_connections__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        raise NotImplementedError( 'UC: ZMQ DEALER connect not yet implemented' )
    #/def __call__
#/class ConnectOp


class DisconnectOp( TableOp ):
    """
    Close an existing ZMQ DEALER connection and update ``__node_connections__``.

    dfs[0]: __disconnect__ system message — single row with address
    dfs[1]: __node_connections__

    Returns updated __node_connections__ with the entry marked 'disconnected'.

    UC: ZMQ socket management not yet implemented.
    """
    def __init__( self: Self, rng = None ) -> None:
        super().__init__(
            rng     = rng,
            input   = [
                InputSchema(
                    columns = { '__system_op__': pl.Utf8, 'address': pl.Utf8 },
                    extra   = 'open',
                ),
                InputSchema(
                    columns = table_schemas[ '__node_connections__' ],
                    extra   = 'forbidden',
                ),
            ],
            output  = [
                OutputSchema( columns = table_schemas[ '__node_connections__' ] ),
            ],
            effects = [],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame ]:
        raise NotImplementedError( 'UC: ZMQ DEALER disconnect not yet implemented' )
    #/def __call__
#/class DisconnectOp


# ---------------------------------------------------------------------------
# Builtin op registry
# ---------------------------------------------------------------------------

NETWORK_BUILTIN_OPS: types.TableProcessDict = {
    '_is_system_message_op':  IsSystemMessageOp(),
    '_is_routing_update_op':  IsRoutingUpdateOp(),
    '_is_connect_op':         IsConnectOp(),
    '_is_disconnect_op':      IsDisconnectOp(),
    'update_routing_table_op': UpdateRoutingTableOp(),
    'lookup_routing_op':       LookupRoutingOp(),
    'record_correlation_op':   RecordCorrelationOp(),
    'lookup_correlation_op':   LookupCorrelationOp(),
    'remove_correlation_op':   RemoveCorrelationOp(),
    'connect_op':              ConnectOp(),
    'disconnect_op':           DisconnectOp(),
}


# ---------------------------------------------------------------------------
# Process factories
# ---------------------------------------------------------------------------

def routing_update_process(
    op_idn:                  str,
    source_inbound_system:   str = '__inbound_system__',
    source_routing_table:    str = '__node_routing_table__',
    target_routing_table:    str = '__node_routing_table__',
) -> TableProcessRef:
    """
    TableProcessRef that applies an incoming routing-update system message to
    ``__node_routing_table__``.
    """
    return TableProcessRef(
        op_idn = op_idn,
        source = [ source_inbound_system, source_routing_table ],
        target = [ target_routing_table ],
        terms  = [ 'update_routing_table_op' ],
    )
#/def routing_update_process


def connect_process(
    op_idn:               str,
    source_inbound_system: str = '__inbound_system__',
    source_connections:   str = '__node_connections__',
    target_connections:   str = '__node_connections__',
) -> TableProcessRef:
    """
    TableProcessRef that opens a new DEALER connection described by an incoming
    connect system message.
    """
    return TableProcessRef(
        op_idn = op_idn,
        source = [ source_inbound_system, source_connections ],
        target = [ target_connections ],
        terms  = [ 'connect_op' ],
    )
#/def connect_process


def disconnect_process(
    op_idn:               str,
    source_inbound_system: str = '__inbound_system__',
    source_connections:   str = '__node_connections__',
    target_connections:   str = '__node_connections__',
) -> TableProcessRef:
    """
    TableProcessRef that closes a DEALER connection described by an incoming
    disconnect system message.
    """
    return TableProcessRef(
        op_idn = op_idn,
        source = [ source_inbound_system, source_connections ],
        target = [ target_connections ],
        terms  = [ 'disconnect_op' ],
    )
#/def disconnect_process


def system_dispatch_process(
    op_idn:                str,
    source_inbound_system: str = '__inbound_system__',
    routing_update_op_idn: str = 'handle_routing_update',
    connect_op_idn:        str = 'handle_connect',
    disconnect_op_idn:     str = 'handle_disconnect',
) -> TableProcessBranch:
    """
    TableProcessBranch that dispatches an incoming system message to the
    correct handler based on its ``__system_op__`` discriminator value.

    Expects the following processes to be pre-registered:
      - routing_update_op_idn  (from routing_update_process)
      - connect_op_idn         (from connect_process)
      - disconnect_op_idn      (from disconnect_process)

    No output — handlers update system tables in place.
    """
    return TableProcessBranch(
        op_idn = op_idn,
        source = [ source_inbound_system ],
        target = [],
        ifs    = [
            '_check_is_routing_update',
            '_check_is_connect',
            '_check_is_disconnect',
        ],
        thens  = [
            routing_update_op_idn,
            connect_op_idn,
            disconnect_op_idn,
        ],
        otherwise = None,
    )
#/def system_dispatch_process


# Check refs used internally by system_dispatch_process — register alongside it.

def _is_routing_update_check( op_idn: str = '_check_is_routing_update' ) -> TableCheckRef:
    return TableCheckRef(
        op_idn = op_idn,
        source = [ '__inbound_system__' ],
        terms  = [ '_is_routing_update_op' ],
    )
#/def _is_routing_update_check

def _is_connect_check( op_idn: str = '_check_is_connect' ) -> TableCheckRef:
    return TableCheckRef(
        op_idn = op_idn,
        source = [ '__inbound_system__' ],
        terms  = [ '_is_connect_op' ],
    )
#/def _is_connect_check

def _is_disconnect_check( op_idn: str = '_check_is_disconnect' ) -> TableCheckRef:
    return TableCheckRef(
        op_idn = op_idn,
        source = [ '__inbound_system__' ],
        terms  = [ '_is_disconnect_op' ],
    )
#/def _is_disconnect_check
