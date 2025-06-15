"""
    A holder of a single table of data
"""

import duckdb
from queue import Queue

class Cell():
    """
        A basic unit, holding one table, and one status dictionary
    """
    def __init__(
        self: Self,
        status: dict[ str, any ],
        cursor: duckdb.duckdb.DuckDBPyConnection | None = None,
        table_name: str = ''
    ) -> None:
        self.status = status
        self.cursor = cursor
        self.queue = Queue()
        self.log = Queue()

    #/def __init__
    
    def queueUpdate(
        self: Self,
        update: dict[ str, any ]
        ) -> None:
        self.queue.put( update )
        return
    #/def queueUpdate
    
    def applyUpdates( self: Self ) -> None:
        """
            Apply all updates in the queue, and aggregate them appropriately
        """
        raise NotImplementedError
    #/def applyUpdates
#/class Cell
