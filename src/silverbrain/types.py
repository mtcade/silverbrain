#
#//  types_nu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

"""
Abstract types for building blocks
"""

from dataclasses import dataclass, field
from typing import Callable, Protocol, Self, Sequence, Type

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

# -- Helper Classes

@dataclass
class SignatureCollector():
    """
        Helper class to recursively trawl ModTableProcesses to build their signatures, before being compared against a dictionary of tables to determine contextual and intermediate tables.
    """
    source: set[ str ] = field( default_factory = set )
    target: set[ str ] = field( default_factory = set )

    def __or__( self: Self, other: Self ) -> Self:
        if not isinstance( other, type( self ) ):
            raise TypeError(
                "Expected got type(other)={}, expected {}".format(
                    type( other ), type( self )
                )
            )
        #/if not isinstance( other, type( self ) )

        return type( self )(
            source = self.source | other.source,
            target = self.target | other.target,
        )
    #/def __or__
#/class SignatureCollector

@dataclass
class TaggedTableProcessSignature():
    source: list[ str ]
    context: list[ str ]
    intermediate: list[ str ]
    target: list[ str ]
#/class ModTableProcessSignature

# -- Recursive Table Processors

class TaggedTableProcess( Protocol ):
    source: Sequence[ str ]
    target: Sequence[ str ]
    op_id: str
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def __call__
    
    def get_signatureCollector(
        self: Self,
        ) -> SignatureCollector:
        ...
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        ...
    #/def get_op_bindings
#/class TaggedTableProcess

class TaggedTableCheck( Protocol ):
    source: Sequence[ str ]
    op_id: str

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> bool:
        ...
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> SignatureCollector:
        ...
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        ...
    #/def get_op_bindings
#/class TaggedTableCheck

TaggedTableProcessDict: Type = dict[ str, TaggedTableProcess | TaggedTableCheck ]


# -- Web

from queue import Queue

class WebProtocol( Protocol ):
    """
        Structural interface for the subset of Web attributes used by
        WebReceiver and OutboxSender in web_transport.  Any Web subclass
        satisfies this automatically.
    """
    inputIds: list[ str ]
    inbox:    Queue
    outbox:   Queue
    tables:   dict[ str, pl.DataFrame | None ]

    def notify( self ) -> None:
        """Signal the web that a new item has been placed in inbox."""
        ...
    #/def notify
#/class WebProtocol





