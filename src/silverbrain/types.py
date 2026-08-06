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

from .schema import InputSchema, OutputSchema

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


# -- Actor: the recursive actor interface. TableOp and Cell both conform to
# this, whether running in-process or as a Ray actor proxy -- a Cell may
# nest other Cells directly, so there is no separate Web class (see
# nested-gathering-russell.md §1.6). Per the Static Execution Binding
# invariant (see plan), an Actor's mode of execution is fixed once at
# bind() time -- this Protocol only describes the call shape, not how a
# given implementation is reached.
#
# input/output declare what __call__ consumes/produces (InputSchema/
# OutputSchema, already used ad hoc by TableOp -- see tableOps.py:399-476).
# This is the declared interface that replaces Web's declare()/NodeDecl
# exposure machinery (§1.6): a genuine schema instead of routing metadata.
# Either may be None, same as TableOp today, when no schema has been
# declared.
#
# corr_id (squishy-sleeping-snowflake.md) is a correlation id: a tuple of
# strings identifying which "round" of execution a call belongs to. It's
# needed once wiring supports fan-in (a node with several named inputs
# must know which arrivals belong together) and cycles/loops (the same
# node can fire more than once per outer call, each firing under its own
# id -- see Cell.__call__'s wiring engine). Every Cell invocation extends
# corr_id with a fresh segment of its own, so its internal joins can't
# cross-talk with a concurrent invocation of the same Cell. A TableOp
# doesn't need it (it's atomic, no children to route between) but every
# TableOp.__call__ takes corr_id like every other Actor, for interface
# consistency and optional use in logging/tracing -- it's simply unused in
# TableOp.__call__ bodies. Every Actor instance -- TableOp, Cell, and any
# test/ad hoc callable used as a node op -- is required to accept corr_id;
# Cell's dispatch engine (_call_actor) passes it unconditionally, with no
# structural probing.
#
# The actor contract is asynchronous in every case: a caller must never
# block waiting for an actor to finish. A Cell's own __call__ returns a
# concurrent.futures.Future immediately, resolving once its declared
# target names are available. TableOp.__call__ stays a plain synchronous
# function (unchanged) -- Cell's dispatch engine and bind() are what adapt
# a synchronous TableOp into the async world (thread-pool submission), so
# from a caller's perspective every bound Actor behaves consistently.

class Actor( Protocol ):
    input:  list[ InputSchema ]  | None
    output: list[ OutputSchema ] | None

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> 'tuple[ pl.DataFrame, ... ] | Future[ tuple[ pl.DataFrame, ... ] ]':
        ...
    #/def __call__
#/class Actor


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

