"""
    Holds multiple cells in a graph; see ./cell.py
"""

from typing import Callable, Literal, Protocol, Self
from abc import abstractmethod
from queue import Queue

from . import cell

class AcyclicWeb():
    """
        Has a series of cells, connected by `.connections`. Must be acyclic as a directed graph.
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        cells: dict[ str, cell.CellInterface ] = {},
        connections: dict[ str, str ] = {},
        input_id: str = '',
        get_lambda: cell.GetLambda,
        queue: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None
        ) -> None:
        self.cells = cells
        self.connections = connections
        self.input_id = input_id
        
        # Fill out sources using `connections`
        self.sources = {}
        for sid, dids in connections.items():
            for did in dids:
                if did not in self.sources:
                    self.sources[ did ] = [ sid ]
                else:
                    self.sources[ did ].append( sid )
                #
            #/for did in dids
        #/for sid, dids in connections.items()
        
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
        if self.queue.empty():
            return
        #/if self.queue.empty()
        
        tab: pl.DataFrame
        tab = self.queue.get()
        self.queue.task_done()
        
        self.cells[ input_id ].queueUpdate( tab )
        self.cells[ input_id ].updateAll()
        
        cells_current: list[ str ] = [ input_id ]
        cells_next: list[ str ]
        update_counter: int = 0
        
        while update_counter <= len( self.cells ):
            cells_next = []
            for cid in cells_current:
                while not cells[ cid ].outbox.empty():
                    tab = cells[ cid ].outbox.get()
                    for did in self.connections[ cid ]:
                        self.cells[ did ].queueUpdate( tab.clone() )
                    #/for did in self.connections[ cid ]
                    cells[ cid ].task_done()
                    if cid in self.connections:
                        cells_next.extend( self.connections[ cid ] )
                    #/if cid in self.connections
                #/while not cells[ cid ].outbox.empty()
            #/for cid in cells_current
            
            update_counter += 1
            
            if cells_next:
                cells_current = list( set( cells_next ) )
            #
            else:
                break
            #/if cells_next
        #/while update_counter <= len( self.cells )
        
        assert update_counter <= len( self.cells )
        
        if self.get_lambda:
            self.outbox.put(
                self.get_lambda( self )
            )
        #
        else:
            assert len( cells_current ) == 1
            cid: str = cells_current[ 0 ]
            while not self.cells[ cid ].outbox.empty():
                tab = self.cells[ cid ].outbox.get()
                self.outbox.put(
                    tab
                )
                self.cells[ cid ].outbox.task_done()
            #
        #/if self.get_lambda/else
        
        return
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Apply all updates in the queue to the input cell, and for each output
        """
        raise NotImplementedError()
    #/def applyUpdates
#/class AcyclicWeb
