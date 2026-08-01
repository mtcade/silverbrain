#
#//  types_nu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

"""
Abstract types for building blocks
"""

import enum
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Protocol, Self, Type

import polars as pl

# Simple Table processes; input is only a tuple. Mostly used for TableOp

class TableProcess( Protocol ):
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def __call__
#/class TableProcess

class TableCheck( Protocol ):
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> bool:
        ...
    #/def __call__
#/class TableCheck

TableProcessDict: Type = dict[ str, TableProcess | TableCheck ]


class ExecutionStrategy( Protocol ):
    def execute(
        self,
        op_idn:             str,
        inputs:             dict[ str, pl.DataFrame ],
        processes_table_id: str,
        verbose:            int,
        verbose_prefix:     str,
    ) -> dict[ str, pl.DataFrame ]: ...
    #/def execute
#/class ExecutionStrategy


# -- Nu types

@dataclass
class NodeDecl:
    op_idn:      str
    source:      tuple[ str, ... ]
    target:      tuple[ str, ... ]
    node_op_idn: str | None = None
#/class NodeDecl


class WebNodeProtocol( Protocol ):
    def declare( self ) -> list[ NodeDecl ]: ...
    def run(
        self,
        op_idn:     str,
        process_id: str,
        inputs:     dict[ str, pl.DataFrame ],
    ) -> Future[ dict[ str, pl.DataFrame ] ]: ...
#/class WebNodeProtocol


# -- Network / distributed types

class MessageKind( enum.Enum ):
    DATA   = 'data'
    SYSTEM = 'system'
#/class MessageKind


@dataclass
class RequestContext:
    """
    Per-request routing metadata that travels alongside table data through the
    pipeline without entering any table.  Set by the receive side; consumed by
    the send/relay side.
    """
    request_id:      str    # correlation key; used to rejoin relay responses
    origin_identity: bytes  # ZMQ ROUTER routing frame for this request
#/class RequestContext


@dataclass
class InboundMessage:
    """
    Decoded envelope from a ROUTER socket, before dispatch to system or data
    processing.  The kind field determines which processing path fires.
    """
    kind:    MessageKind
    context: RequestContext
    tables:  tuple[ pl.DataFrame, ... ]
#/class InboundMessage

