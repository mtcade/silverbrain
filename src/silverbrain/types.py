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

class WebNode( Protocol ):
    """
    Minimal structural interface shared by web.Web and all webNetwork
    implementations.  Anything that exposes put and input_ids satisfies this.
    """
    input_ids: list[ str ]

    def put(
        self,
        dfs:                tuple[ pl.DataFrame, ... ],
        op_id:              str,
        processes_table_id: str       = '__table_processes__',
        verbose:            int | None = None,
        verbose_prefix:     str       = '',
    ) -> None: ...
    #/def put

    def bind(
        self,
        address: str,
        context: 'zmq.Context | None' = None,
    ) -> None: ...
    #/def bind
#/class WebNode


class WebNetwork( Protocol ):
    """
    Full network lifecycle interface: WebNode extended with start/stop/join.
    Satisfied by all webNetwork implementations but not by web.Web directly.
    """
    input_ids: list[ str ]

    def put(
        self,
        dfs:                tuple[ pl.DataFrame, ... ],
        op_id:              str,
        processes_table_id: str       = '__table_processes__',
        verbose:            int | None = None,
        verbose_prefix:     str       = '',
    ) -> None: ...
    #/def put

    def bind(
        self,
        address: str,
        context: 'zmq.Context | None' = None,
    ) -> None: ...
    #/def bind

    def start( self ) -> None: ...
    def stop( self )  -> None: ...

    def join(
        self,
        timeout: float | None = None,
    ) -> None: ...
    #/def join
#/class WebNetwork



