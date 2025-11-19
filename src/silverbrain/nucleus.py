"""
    A holder of up to one single table of data, and potentially a status dictionary
    
    Nothing pushes output, instead you use `.queueUpdate(...)` to add a `pl.DataFrame`, potentially run `.updateOnce(...)` or `.updateAll(...)` and then find the results in the `Nucleus` `.outbox`
"""

from . import types

import polars as pl

from typing import Callable, Literal, Optional, Protocol, Self, Type
from abc import abstractmethod
from queue import Queue





TableLambda: Type = Callable[
    [
        tuple[ pl.DataFrame,...], # dfs
        Optional[ int ], # verbose
        Optional[ str ], # verbose_prefix
    ],
    pl.DataFrame
]


class Nucleus():
    def __init__(
        self: Self,
        status: dict[ str, any ] | None = None,
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None
        ):
        self.status = status or {}
    
        self.inbox = inbox or Queue()
        
        self.outbox = outbox or Queue()
        
        self.log = log or Queue()

        return
    #/def __init__
    
    def queueUpdate(
        self: Self,
        update: pl.DataFrame
        ) -> None:
        self.inbox.put( update )
        return
    #/def queueUpdate
    
    def updateOnce( self: Self ) -> None:
        raise NotImplementedError()
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        raise NotImplementedError
    #/def updateAll
#/class Nucleus

class MemoryCell( Nucleus ):
    """
        ...
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        table: pl.DataFrame | None = None,
        table_schema: pl.Schema | None = None,
        update_lambda: TableLambda | None = None
        ):
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log
        )
        
        if table is None and table_schema is not None:
            self.table = pl.DataFrame(
                data = None,
                schema = table_schema
            )
        #
        elif table is not None:
            self.table = table
        #/switch table, table_schema
        
        self.table_schema = table_schema
        
        self.update_lambda = update_lambda
    #/def __init__
    
    def updateOnce( self: Self ) -> None:
        """
           Run only one table in `.inbox`
        """
        raise NotImplementedError()
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Run all data in `.inbox`
        """
        raise NotImplementedError()
    #/def updateAll
#/class MemoryCell

PatientUpdateLambda = Callable[
    [],
    pl.DataFrame
]

class EagerMemoryCell( MemoryCell ):
    """
        Pushes a result to `.outbox` for every input, using both self and the input table
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        table: pl.DataFrame | None = None,
        table_schema: pl.Schema | None = None,
        update_lambda: TableLambda | None = None,
        get_lambda: TableLambda | None = None
        ):
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log,
            table = table,
            table_schema = table_schema,
            update_lambda = update_lambda
        )

        self.get_lambda = get_lambda
        
        return
    #/def __init__
    
    def updateOnce( self: Self ) -> None:
        """
           Process one input from `.inbox` with `.update_lambda` and push the result to `.outbox`
        """
        if self.inbox.empty():
            return
        #
        
        tab: pl.DataFrame = self.inbox.get()
        self.table = self.update_lambda( tab )

        if self.get_lambda:
            self.outbox.put(
                self.get_lambda(
                    tab
                )
            )
        #
        else:
            self.outbox.put(
                self.table.clone()
            )
        #/if self.get_lambda/else
        
        self.inbox.task_done()
        return
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Process all of `.inbox` with `.update_lambda` and push a result to `.outbox` for each
        """
        if self.inbox.empty():
            return
        #
        
        tab: pl.DataFrame
        
        while not self.inbox.empty():
            tab = self.inbox.get()
            self.table = self.update_lambda( tab )
            
            if self.get_lambda:
                self.outbox.put(
                    self.get_lambda(
                        tab
                    )
                )
            #
            else:
                self.outbox.put(
                    self.table.clone()
                )
            #
            
            self.inbox.task_done()
        #/while not self.inbox.empty()

        return
    #/def updateAll
#/class EagerMemoryCell( MemoryCell )

class PatientMemoryCell( MemoryCell ):
    """
        A memory cell which produces output using only `.table`, not any input tables, after processing all input
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        table: pl.DataFrame | None = None,
        table_schema: pl.Schema | None = None,
        update_lambda: TableLambda | None = None,
        patient_get_lambda: PatientUpdateLambda | None = None
        ):
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log,
            table = table,
            table_schema = table_schema,
            update_lambda = update_lambda
        )

        self.patient_get_lambda = patient_get_lambda
        
        return
    #/def __init__
    
    def updateOnce( self: Self ) -> None:
        """
           Run one input fom `.inbox`, process with `.update_lambda`, and then push output with `.patient_get_lambda`
        """
        if self.inbox.empty():
            return
        #
        
        tab: pl.DataFrame = self.inbox.get()
        self.table = self.update_lambda( tab )
        self.inbox.task_done()
        
        if self.patient_get_lambda:
            self.outbox.put(
                self.patient_get_lambda()
            )
        #
        else:
            self.outbox.put(
                self.table.clone()
            )
        #/if self.get_lambda/else
        
        
        return
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Apply everything in `.inbox` with `.update_lambda`, then then use `.patient_get_lambda` for output
        """
        if self.inbox.empty():
            return
        #
        
        tab: pl.DataFrame
        
        while not self.inbox.empty():
            tab = self.inbox.get()
            self.table = self.update_lambda( tab )

            self.inbox.task_done()
        #/while not self.inbox.empty()
        
        if self.patient_get_lambda:
            self.outbox.put(
                self.patient_get_lambda()
            )
        #
        else:
            self.outbox.put(
                self.table.clone()
            )
        #
            
        return
    #/def updateAll
#/class PatientMemoryCell( MemoryCell )

class TransformerCell( Nucleus ):
    """
        A Nucleus which only transforms input tables, with no personal memory
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] = {},
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        transform_lambda: TableLambda | None = None
        ):
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log
        )
        
        self.transform_lambda = transform_lambda
        
        return
    #/def __init__
        
    def updateOnce( self: Self ) -> None:
        if not self.inbox.empty():
            tab: pl.DataFrame = self.inbox.get()
            self.outbox.put(
                self.transform_lambda(
                    tab
                )
            )
            self.inbox.task_done()
        #/if not self.inbox.empty()
        return
    #/def updateOnce
        
    def updateAll( self: Self ) -> None:
        while not self.inbox.empty():
            tab: pl.DataFrame = self.inbox.get()

            self.outbox.put(
                self.transform_lambda(
                    tab
                )
            )
            self.inbox.task_done()
        #/while not self.inbox.empty()
        return
    #/def updateAll
#/class Transformer

# -- Lambda makers: transform, update, get

def getUpdateLambda_forKey(
    cel: Nucleus,
    on: list[ str ],
    how: str = 'full',
    *args,
    **kwargs
    ) -> TableLambda:
    """
        :param Nucleus cel: Table holder
        :param list[ str ] on: Keys to update by, shared by the memoryCell.table, and the input table
        :param str how: Join method, used by `table.update()`
        :param *args: Passed to `table.update()`
        :param **kwargs: Passed to `table.update()`
        
        Uses `pl.DataFrame.update()` to simply update `nuc.table
    """
    # TODO: kill
    return lambda tab: cel.table.update(
        tab,
        on = on,
        how = how,
        *args,
        **kwargs
    )
#/def getUpdateLambda_forKey

def getUpdateLambda_auto(
    memoryCell: MemoryCell,
    how: str = 'full',
    *args,
    **kwargs
    ) -> TableLambda:
    """
        :param MemoryCell memoryCell: A `MemoryCell` with `.status["primary_key"]`, which will be the `on` value in `getUpdateLambda_forKey`
        :param str how: Join method, used by `table.update()`
        :param *args: Passed to `table.update()`
        :param **kwargs: Passed to `table.update()`
        
        Update `memoryCell` by its `status["primary_key"]
    """
    return getUpdateLambda_forKey(
        cel = memoryCell,
        on = memoryCell.status["primary_key"],
        how = how,
        *args,
        **kwargs
    )
#/getUpdateLambda_auto

# Update to simply get the table and ignore the cell
updateLambda_replace: TableLambda = lambda tab: tab

# TODO: All deez

def infer_updateLambda( any ) -> TableLambda:
    raise Exception("UC")
#

def infer_eagerGetLambda( any ) -> TableLambda:
    raise Exception("UC")
#

def infer_patientGetLambda( any ) -> PatientUpdateLambda:
    raise Exception("UC")
#

def infer_transformLamba( any ) -> TableLambda:
    raise Exception("UC")
#

# -- Cell Factories

def eagerMemory_fromWebRow(
    row: dict[ str, any ]
    ) -> EagerMemoryCell:
    # TODO: set table
    return EagerMemoryCell(
        status = row["cell_status"],
        table_schema = row["table_schema"],
        update_lambda = infer_updateLambda(
            row["update_lambda"]
        ),
        eager_get_lambda = infer_eagerGetLambda(
            row["get_lambda"]
        )
    )
#/def eagerMemory_fromWebRow

def patientMemory_fromWebRow(
    row: dict[ str, any ]
    ) -> PatientMemoryCell:
    # TODO: set table
    return PatientMemoryCell(
        status = row["cell-status"],
        table_schema = row["table_schema"],
        update_lambda = infer_updateLambda(
            row["update_lambda"]
        ),
        patient_get_lambda = infer_patientGetLambda(
            row["get_lambda"]
        )
    )
#/def patientMemory_fromWebRow

def transformer_fromWebRow(
    row: dict[ str, any ]
    ) -> TransformerCell:
    
    return TransformerCell(
        status = row["cell_status"],
        transform_lambda = infer_transformLamba(
            row["transform_lambda"]
        )
    )
    
#/def transformer_fromWebRow()

def fromWebRow(
    row: dict[ str, any ]
    ) -> Nucleus:
    """
        :param dict[ str, any ] row: From a web table, including:
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
            
        General handler for making cells from a web table
    """
    if False:
        pass
    #
    elif row["cell_type"] == 'transformer':
        return transformer_fromWebRow( row )
    #
    elif row["cell_type"] == 'eagerMemory':
        return eagerMemory_fromWebRow( row )
    #
    elif row["cell_type"] == 'patientMemory':
        return patientMemory_fromWebRow( row )
    #
    else:
        raise ValueError(
            "Bad row['cell_type']={}".format(
                row["cell_type"]
            )
        )
    #/switch row["cell_type"]
    
    raise Exception("Unexpected EoF")
#/def fromWebRow
