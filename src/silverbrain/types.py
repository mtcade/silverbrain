"""
Abstract types for building blocks
"""

from typing import Callable, Protocol, Self, Type

import polars as pl

CellId: Type = int | str | bytes

class Cell( Protocol ):
    def queueUpdate( self: Self, update: pl.DataFrame ) -> None:
        ...
    
    def updateOnce( self: Self ) -> None:
        ...
    
    def updateAll( self: Self ) -> None:
        ...
#/class CellInterface
