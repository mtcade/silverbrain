"""
Abstract types for building blocks
"""

from typing import Callable, Literal, Protocol, Self
from abc import abstractmethod

import polars as pl

class Cell( Protocol ):
    @abstractmethod
    def queueUpdate( self: Self, update: pl.DataFrame ) -> None:
        raise NotImplementedError()
    #
    
    @abstractmethod
    def updateOnce( self: Self ) -> None:
        raise NotImplementedError
    #
    
    @abstractmethod
    def updateAll( self: Self ) -> None:
        raise NotImplementedError
    #
#/class CellInterface
