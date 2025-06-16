"""
    Holds multiple cells in a graph; see ./cell.py
"""

from typing import Callable, Literal, Protocol, Self
from abc import abstractmethod
from queue import Queue

from . import cell

class Web( cell.Nucleus ):
    """
        Has a series of cells, connected by `.connections`. Must be acyclic as a directed graph.
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        cells: dict[ str, cell.CellInterface ] = {},
        connections: dict[ str, str ] = {},
        input_ids: list[ str ] = [],
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None
        ) -> None:
        
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log
        )
        
        self.cells = cells
        self.connections = connections
        self.input_ids = input_ids
        
        return
    #/def __init__
    
    def queueUpdate(
        self: Self,
        update: pl.DataFrame
        ) -> None:
        self.inbox.put( update )
        return
    #/def queueUpdate
    
    def cycleOnce( self: Self ) -> None:
        """
            Has each cell run `.updateAll`, consuming everything in their queues, and pushing results to their outbox. Then, move everything from each outbox to their destination inboxes via `self.connections`
        """
        
        # Have each cell run its entire queue
        for cid, cell in self.cells.items():
            cell.updateAll()
        #/for cid, cell in self.cells.items()
        
        for cid, cell in self.cells.items():
            while not cell.outbox.empty():
                # Get output
                tab: pl.DataFrame = cell.outbox.get()

                # Send output to destinations
                for did in self.connections[ cid ]:
                    self.cells[ did ].queueUpdate( tab )
                #/for did in self.connections[ cid ]
                
                cell.outbox.task_done()
            #/while not cell.outbox.empty()
        #/for cid, cell in self.cells.items()
        
        return
    #/def cycleOnce
    
    def updateOnce( self: Self ) -> None:
        if self.inbox.empty():
            return
        #/if self.inbox.empty()
        
        tab: pl.DataFrame = self.inbox.get()
        self.inbox.task_done()
        
        for cid in self.input_ids:
            self.cells[ cid ].queueUpdate( tab )
        #/for cid in self.input_ids
        
        self.cycleOnce()
        
        return
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Apply all updates in the inbox to the input cell, and for each output
            
            TODO: decide how often to cycle
        """
        if self.inbox.empty():
            return
        #/if self.inbox.empty()
        
        tab: pl.DataFrame
        while not self.inbox.empty():
            tab = self.inbox.get()
            self.inbox.task_done()
            
            for cid in self.input_ids:
                self.cells[ cid ].queueUpdate( tab )
            #/for cid in self.input_ids
        #/while not self.inbox.empty()
        
        self.cycleOnce()
        
        return
    #/def updateAll
#/class Web

def web_fromTable(
    table: pl.DataFrame,
    status: dict[ str, any ] = {},
    inbox: Queue | None = None,
    outbox: Queue | None = None,
    log: Queue | None = None
    ) -> Web:
    """
        :param pl.DataFrame table: A web map, with columns
            - `"cell_id": str`
            - `"cell_type": Literal[
                    'transformer',
                    'eagerMemory',
                    'patientMemory'
                ]`
            - `"cell_status": dict[ str, any ]`
            - `"table_schema": dict[ str, str ]`
            - `"update_lambda": any`
            - `"get_lambda": any`
            - `"transform_lambda": any`
            - `"destinations": list[ str ]`
            - '"input": bool'
    """
    
    cells: dict[ str, cell.Nucleus ] = {}
    connections: dict[ str, str ] = {}
    input_ids: list[ str ] = []
    
    # Gather everything from the table
    for row in table.iter_rows( named = True ):
        cells[ row['cell_id'] ] = cell.fromWebRow( row )
        
        if row['destinations']:
            connections[ row['cell_id'] ] = row['destinations']
        #
        
        if row[ input ]:
            input_ids.append( row['cell_id'] )
        #
    #/for row in table.iter_rows( named = True )
    
    return Web(
        status = status,
        cells = cells,
        connections = connecctions,
        input_ids = input_ids,
        inbox = inbox,
        outbox = outbox,
        log = log
    )
#/def web_fromTable
