#
#//  types_nu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

"""
Abstract types for building blocks
"""

from concurrent.futures import Future
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
# nest other Cells directly, so there is no separate Web class. Per the
# Static Execution Binding invariant, an Actor's mode of execution is fixed
# once at bind() time -- this Protocol only describes the call shape, not
# how a given implementation is reached.
#
# input/output declare what __call__ consumes/produces (InputSchema/
# OutputSchema, already used ad hoc by TableOp -- see tableOps.py:399-476).
# This is the declared interface a Cell/TableOp exposes -- a genuine schema
# instead of routing metadata. Either may be None, same as TableOp today,
# when no schema has been declared.
#
# corr_id is a tuple of strings, carried alongside dfs on every message a
# node sends or receives. It is PURELY a
# tracing/correlation aid, not correctness-relevant: cell.py's
# _WiringEngine wires actors point-to-point (Kahn Process Network-style
# named ports, each an ordered FIFO queue, not a shared overwritable
# blackboard), so pairing tokens across a node's several source ports
# never needs an explicit identity check -- per-port FIFO ordering alone
# guarantees it, given actors are never re-dispatched while a prior firing
# of themselves is still in flight (see _WiringEngine's docstring for the
# full argument). A root corr_id is minted once, at a true top-level
# Cell.__call__ (no corr_id given); every ordinary node/Cell firing
# thereafter relays whatever corr_id its dispatch carried, from whichever
# source port participated (first in declared order), unchanged -- no
# extension, no re-minting. Concurrency isolation between invocations of
# the same Cell does NOT depend on corr_id's uniqueness -- a fresh
# _WiringEngine (its own per-call logs/cursors) is constructed every call
# regardless of what corr_id it's given. A TableOp doesn't need it (it's
# atomic, no children to route between) but every TableOp.__call__ takes
# corr_id like every other Actor, for interface consistency and optional
# use in logging/tracing -- it's simply unused in TableOp.__call__ bodies.
# Every Actor instance -- TableOp, Cell, and any test/ad hoc callable used
# as a node op -- is required to accept corr_id; Cell's dispatch engine
# (_call_actor) passes it unconditionally, with no structural probing.
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

