"""
    A holder of a single table of data, and potentially a status dictionary
"""

from typing import Callable, Protocol, Self
from abc import abstractmethod
from queue import Queue

import polars as pl

class CellInterface( Protocol ):
    @abstractmethod
    def queueUpdate( self: Self, update: pl.DataFrame ):
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

UpdateLambda = Callable[ [ CellInterface, pl.DataFrame ], pl.DataFrame ]
GetLambda = Callable[ [ CellInterface ], pl.DataFrame ]

class Nucleus():
    """
        :param dict[str, any] status: Dictionary of single values. Common keys include:
            - `primary_key: list[ str ]` update key for `self.table`
        A basic unit, holding one table, and one status dictionary
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        table: pl.DataFrame | None = None,
        table_schema: pl.Schema | None = None,
        update_lambda: UpdateLambda | None = None,
        get_lambda: GetLambda | None = None,
        queue: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None
        ) -> None:
        self.status = status
        
        if table is None and table_schema is not None:
            self.table = pl.DataFrame( data = None, schema = table_schema )
        elif table is not None:
            self.table = table
        #
        
        self.table_schema = table_schema
        
        self.update_lambda = update_lambda
        self.get_lambda = get_lambda
        
        
        if queue is None:
            self.queue = Queue()
        #
        else:
            self.queue = queue
        #
        
        if outbox is None:
            self.outbox = Queue()
        #
        else:
            self.outbox = outbox
        #
        
        if log is None:
            self.log = Queue()
        #
        else:
            self.log = log
        #
        return
    #/def __init__
    
    def queueUpdate(
        self: Self,
        update: pl.DataFrame
        ) -> None:
        self.queue.put( update )
        return
    #/def queueUpdate
    
    def updateOnce( self: Self ) -> None:
        """
            Run a self update only once
        """
        if not self.queue.empty():
            self.table = self.update_lambda( self, self.queue.get() )
            self.queue.task_done()
        #
        
        if self.get_lambda:
            self.outbox.put(
                self.get_lambda( self )
            )
        #
        else:
            self.outbox.put(
                self.table.clone()
            )
        #/if self.get_lambda/else
        
        return
    #
    
    def updateAll( self: Self ) -> None:
        """
            Apply all updates in the queue using `self.update_lambda`
        """
        while not self.queue.empty():
            self.table = self.update_lambda( self, self.queue.get() )
            self.queue.task_done()
        #
        
        if self.get_lambda:
            self.outbox.put(
                self.get_lambda( self )
            )
        #
        else:
            self.outbox.put(
                self.table.clone()
            )
        #
        
        return
    #/def applyUpdates
#/class Nucleus

# -- Update functions
def getUpdateLambda_forKey(
    on: list[ str ],
    how: str = 'full',
    *args,
    **kwargs
    ) -> UpdateLambda:
    """
        :param *args: Passed to `table.update()`
        :param **kwargs: Passed to `table.update()`
        
        Uses `pl.DataFrame.update()`
    """
    return lambda nuc, tab: nuc.table.update(
        tab,
        on = on,
        how = how,
        *args,
        **kwargs
    )
#

def getUpdateLambda_auto(
    nucleus: Nucleus,
    how: str = 'full',
    *args,
    **kwargs
    ) -> UpdateLambda:
    """
        Update `nucleus` by its `status["primary_key"]
    """
    return getUpdateLambda_forKey(
        on = nucleus.status["primary_key"],
        how = how,
        *args,
        **kwargs
    )
#/getUpdateLambda_auto

# Update to simply replace with new table
updateLambda_replace: UpdateLambda = lambda nuc, tab: tab
