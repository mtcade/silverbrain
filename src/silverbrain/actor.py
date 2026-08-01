#
#//  actor.py
#//  silverbrain
#//

"""
The recursive actor interface: TableOp and Cell both conform to this,
whether running in-process or as a Ray actor proxy -- a Cell may nest other
Cells directly, so there is no separate Web class (see nested-gathering-
russell.md §1.6). Per the Static Execution Binding invariant (see plan), an
Actor's mode of execution is fixed once at bind() time -- this Protocol only
describes the call shape, not how a given implementation is reached.

input/output declare what __call__ consumes/produces (InputSchema/
OutputSchema, already used ad hoc by TableOp -- see tableOps.py:399-476).
This is the declared interface that replaces Web's declare()/NodeDecl
exposure machinery (§1.6): a genuine schema instead of routing metadata.
Either may be None, same as TableOp today, when no schema has been declared.

corr_id (squishy-sleeping-snowflake.md) is a correlation id: a tuple of
strings identifying which "round" of execution a call belongs to. It's
needed once wiring supports fan-in (a node with several named inputs must
know which arrivals belong together) and cycles/loops (the same node can
fire more than once per outer call, each firing under its own id -- see
Cell.__call__'s wiring engine). Every Cell invocation extends corr_id with
a fresh segment of its own, so its internal joins can't cross-talk with a
concurrent invocation of the same Cell. A TableOp doesn't need to (it's
atomic, no children to route between) but receives corr_id for interface
consistency and optional use in logging/tracing -- TableOp.__call__ itself
is unchanged (still just dfs/verbose/verbose_prefix; TableOps are out of
scope for this rearchitecture), so it structurally doesn't accept corr_id.
Cell's dispatch engine (and bind()) detect this and call it without corr_id
-- Actor was never runtime-checked (not @runtime_checkable), so this is a
documented, tolerated exception, not a violation enforced anywhere.

The actor contract is asynchronous in every case: a caller must never block
waiting for an actor to finish. A Cell's own __call__ returns a
concurrent.futures.Future immediately, resolving once its declared target
names are available. TableOp.__call__ stays a plain synchronous function
(unchanged) -- Cell's dispatch engine and bind() are what adapt a
synchronous TableOp into the async world (thread-pool submission), so from
a caller's perspective every bound Actor behaves consistently.
"""

from concurrent.futures import Future
from typing import Protocol, Self

import polars as pl

from .tableOps import InputSchema, OutputSchema


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
