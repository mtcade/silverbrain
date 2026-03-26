#
#//  web_1.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/11/26.
#//  Update to web, removing TableOpParent and its decendents,
#//     in favor of TableOpRef and TableOp

from . import tableOps, types

import polars as pl
import numpy as np

from typing import Self
from queue import Queue


class Web():
    """
        Holds tables, TableOps, and TableSequences to acccept, transform, return, and potentially write tables
        
        - inputIds: list[ str ] | None = None
            List of keys from `self.tableProcesses` which are taken as inputs
        - tableProcesses: types.TaggedTableProcessDict
        - tables: dict[ str, pl.DataFrame | None ]
            str -> pl.DataFrame | None
                key references from a TableProcessRef.source[*] or TableProcesspRef.target[*]
        - tableOps: types.TableProcessDict
        - inbox: Queue | None = None
        - outbox: Queue | None = None
        - log: Queue | None = None
        - verbose: int = 0
    """
    def __init__(
        self: Self,
        inputIds: list[
            str
        ] | None = None,
        tables: dict[
            str,
            pl.DataFrame | None
        ] | None = None,
        tableOps: types.TableProcessDict | None = None,
        tableProcesses: types.TaggedTableProcessDict | None = None,
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        verbose: int = 0,
        ) -> None:

        self.inputIds = inputIds or []
        self.tables = tables or {}
        self.tableOps = tableOps or {}
        self.tableProcesses = tableProcesses or {}
        
        self.inbox = inbox or Queue()
        self.outbox = outbox or Queue()
        self.log = log or Queue()
        
        self.verbose = verbose

        return
    #/def __init__
    
    def init_table(
        self: Self,
        df: pl.DataFrame,
        cellId: str,
        ) -> None:
        """
            Adds a new table to `self.tables`. If the `cellId` is already present, the value must be `None`, so we do not overwrite anything.
        """
        if cellId in self.tables:
            assert self.tables[ cellId ] is None
        #
        
        self.tables[ cellId ] = df
        
        return
    #/def init_table
    
    # Processing
    
    def apply(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        taggedTableProcess: types.TaggedTableProcess,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> tuple[ pl.DataFrame,... ]:
        return taggedTableProcess(
            dfs = dfs,
            tableOps = self.tableOps,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def apply
    
    def apply_id(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        id: str,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> tuple[ pl.DataFrame,... ]:
        """
            Run `apply` with `self.tableProcesses[id]`
        """
        return self.apply(
            dfs = dfs,
            taggedTableProcess = self.tableProcesses[ id ],
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def apply_id
    
    def put(
        self,
        dfs: tuple[ pl.DataFrame,... ],
        taggedTableProcess: types.TaggedTableProcess,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        """
            Apply dfs as the whole source to the taggedTableProcess instead of self.tables, but write the result to self.tables based on taggedTableProcess.target
            
            :param dfs: New dataframe tuple matching taggedTableProcess.source
            :allow_new: If any tableId in taggedTableProcess.target is not in self.tables, write it. Otherwise, raises an error.
        """
        if len( taggedTableProcess.source ) != len( dfs ):
            raise ValueError(
                "len( taggedTableProcess.source )={}, expected {}".format(
                    len( taggedTableProcess.source ), len( dfs )
                )
            )
        #
        
        if not allow_new:
            if not all( tableId in self.tables for tableId in taggedTableProcess.target ):
                raise ValueError("Bad taggedTableProcess.target={}".format(taggedTableProcess.target))
            #
        #
        
        result: tuple[ pl.DataFrame,... ] = self.apply(
            dfs = dfs,
            taggedTableProcess = taggedTableProcess,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
        
        # Update self.tables with the result
        self.tables |= dict(
            zip(
                taggedTableProcess.target,
                result,
            )
        )
        
        return
    #/def put
    
    def put_id(
        self,
        dfs: tuple[ pl.DataFrame,... ],
        id: str,
        verify: bool = False,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        return self.put(
            dfs = dfs,
            taggedTableProcess = self.tableProcesses[ id ],
            verify = verify,
            allow_new = allow_new,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def put_id
    
    def run(
        self: Self,
        taggedTableProcess: types.TaggedTableProcess,
        verify: bool = False,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        """
            Runs the tagged tableProcess with the entire source coming from self.tables
        """
        if verify:
            processSignature: types.TaggedTableProcessSignature = self.tableProcessSignature(
                taggedTableProcess,
            )
            if processSignature.source != []:
                raise ValueError(
                    "processSignature.source={}, expected []".format(
                        processSignature,
                    )
                )
            #
        #/if verify
        
        dfs: tuple[ pl.DataFrame,... ] = tuple(
            self.tables[ tableId ]
            for tableId in taggedTableProcess.source
        )
        
        return self.put(
            dfs = dfs,
            taggedTableProcess = taggedTableProcess,
            allow_new = allow_new,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def run
    
    def run_id(
        self,
        id: str,
        verify: bool = False,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        return self.run(
            taggedTableProcess = self.tableProcesses[ id ],
            verify = verify,
            allow_new = allow_new,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def run_id
    
    
    
    # -- Web Graph Analysis
    
    def tableProcessSignature(
        self: Self,
        taggedTableProcess: types.TaggedTableProcess
        ) -> types.TaggedTableProcessSignature:
        """
            Signature for one TaggedTableProcess in the context of self
        """
        tableKeys: set[ str ] = set( self.tables.keys() )
        collector: types.SignatureCollector = taggedTableProcess.get_signatureCollector()

        # Subtract collector.target from collector.source to exclude intermediates:
        # intermediate tables appear in collector.source (consumed by a later step)
        # but were produced within the process, not supplied externally.
        external_inputs: set[ str ] = collector.source - collector.target

        return types.TaggedTableProcessSignature(
            source = list( sorted( external_inputs - tableKeys ) ),
            context = list( sorted( collector.source & tableKeys ) ),
            intermediate = list( sorted( collector.target - tableKeys ) ),
            target = list( sorted( collector.target & tableKeys ) ),
        )
    #
    
    def get_tableProcessesSignatures(
        self: Self,
        ) -> dict[
            str, # modTableProcess key
            types.TaggedTableProcessSignature
        ]:
        """
            Trawl each `self.modTableProcess` to get its ModTableProcessSignature, which must be done in the context of `self.tables`
            
            source: Tables which are inputs to some TableProcess but not in self.tables
            context: Tables which are inputs to some TableProcess, and are in self.tables.
            target: Tables which are either the target of the top level ModTableProcess, or which are outputs of some TableProcess and are in self.tables. They might change when calling the process.
            intermediate: Tables which are the output of some TableProcess, but which are neither in the top level target nor in self.tables; they will not persist between calls.
            
            Note that there might be overlap:
                - between context and target since existing tables can get modified
            
            There should not be overlap between:
                - source and context: Source should be 'inputs' to the ModTableProcess while context should be in self.tables
                - source and target: If we are modifying a table, putting it in target, then it should exist before calling the ModTableProcess; thus, it would not be in source.
        """
        
        tableKeys: set[ str ] = set( self.tables.keys() )

        result: dict[ str, types.TaggedTableProcessSignature ] = {}

        for processId, modTableProcess in self.tableProcesses.items():
            collector: types.SignatureCollector = modTableProcess.get_signatureCollector()

            # Subtract collector.target from collector.source to exclude intermediates:
            # intermediate tables appear in collector.source (consumed by a later step)
            # but were produced within the process, not supplied externally.
            external_inputs: set[ str ] = collector.source - collector.target

            result[ processId ] = types.TaggedTableProcessSignature(
                source = list( sorted( external_inputs - tableKeys ) ),
                context = list( sorted( collector.source & tableKeys ) ),
                target = list( sorted( collector.target & tableKeys ) ),
                intermediate = list( sorted( collector.target - tableKeys ) ),
            )
        #/for processId, modTableProcess in self.tableProcesses.items()

        return result
    #/def get_tableProcessesSignatures
#/class Web

def from_polars(
    df: pl.DataFrame,
    inbox: Queue | None = None,
    outbox: Queue | None = None,
    log: Queue | None = None
    ) -> Web:
    """
        :param df: ...
    """
    raise Exception("UC")
#/def from_polars
