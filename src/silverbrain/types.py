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
    op_idn: str

    def as_polars( self: Self ) -> pl.DataFrame:
        ...
    #/def as_polars
#/class TaggedTableProcess

class TaggedTableCheck( Protocol ):
    source: Sequence[ str ]
    op_idn: str

    def as_polars( self: Self ) -> pl.DataFrame:
        ...
    #/def as_polars
#/class TaggedTableCheck

TaggedTableProcessDict: Type = dict[ str, TaggedTableProcess | TaggedTableCheck ]


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

