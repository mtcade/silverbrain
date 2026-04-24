#
#//  web.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/11/26.

from . import tableInit, tableOps, tableProcesses, types, utilities
from .schema import table_schemas, table_schemas_to_df
from .polarsDataTypeStrings import dtype_to_str

import threading
import polars as pl
import numpy as np

from typing import Callable, Self
from queue import Queue, Empty

def _collect_subtree_pids(
    rows: dict,
    pid: int,
    ) -> set[int]:
    """Return the set of all node_ids in the subtree rooted at pid."""
    pids = { pid }
    row  = rows[ pid ]
    for child in ( row.get( 'term_ids' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    if row.get( 'condition' ) is not None:
        pids |= _collect_subtree_pids( rows, row[ 'condition' ] )
    #
    for child in ( row.get( 'ifs' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    for child in ( row.get( 'thens' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    return pids
#/def _collect_subtree_pids

def _collect_subtree_sources_targets(
    rows: dict,
    pid: int,
    ) -> tuple[ set[str], set[str] ]:
    """Walk the subtree and union all source/target table keys."""
    row = rows[ pid ]
    if row[ 'type' ] == 'Op':
        return set(), set()
    sources  = set( row.get( 'source' ) or [] )
    targets  = set( row.get( 'target' ) or [] )
    children = list( row.get( 'term_ids' ) or [] )
    if row.get( 'condition' ) is not None:
        children.append( row[ 'condition' ] )
    children += list( row.get( 'ifs' ) or [] )
    children += list( row.get( 'thens' ) or [] )
    for child_pid in children:
        cs, ct = _collect_subtree_sources_targets( rows, child_pid )
        sources |= cs
        targets |= ct
    return sources, targets
#/def _collect_subtree_sources_targets


class Web():
    """
        Holds tables, TableOps, and TableSequences to acccept, transform, return, and potentially write tables

        - inputIds: list[ str ] | None = None
            List of op_ids from `tables['__table_processes__']` which are taken as inputs
        - tableProcesses: TaggedTableProcessDict | pl.DataFrame | None
        - tables: dict[ str, pl.DataFrame | None ]
            str -> pl.DataFrame | None
                key references from a TableProcessRef.source[*] or TableProcesspRef.target[*]
        - tableOps: types.TableProcessDict
        - table_inits: dict[ str, TableInitRef ] | None = None
            Declarative table initializers; ops are registered into tableProcesses and
            stored as self._init_tables for use by init_data()
        - process_inits: dict[ str, ProcessInitRef ] | None = None
            Lazy process factories; stored as self._init_processes for use by init_data()
        - inbox: Queue | None = None
        - outbox: Queue | None = None
        - log: Queue | None = None
        - verbose: int = 0
    """
    def __init__(
        self: Self,
        main_id: str = '',
        rng: np.random.Generator | None = None,
        inputIds: list[
            str
        ] | None = None,
        tables: dict[
            str,
            pl.DataFrame | None
        ] | None = None,
        tableOps: types.TableProcessDict | None = None,
        tableProcesses: types.TaggedTableProcessDict | pl.DataFrame | None = None,
        table_inits: dict[
            str,
            tableInit.TableInitRef
        ] | None = None,
        init_table_ops: dict[
            str,
            types.TaggedTableProcess
        ] | None = None,
        process_inits: dict[
            str,
            tableInit.ProcessInitRef
        ] | None = None,
        tableProcessFactories: dict[
            str,
            Callable
        ] | None = None,
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        verbose: int = 0,
        ) -> None:

        self.main_id = main_id
        self.rng = rng

        self.inputIds = inputIds or []
        self.tables = tables or {}
        self.tableOps = tableOps or {}

        # -- build __table_processes__ DataFrame
        if isinstance( tableProcesses, pl.DataFrame ):
            self.tables[ '__table_processes__' ] = tableProcesses
        #
        elif tableProcesses:
            frames, next_id = [], 0
            for p in tableProcesses.values():
                df, next_id = p.as_polars( start_id = next_id )
                frames.append( df )
            #
            self.tables[ '__table_processes__' ] = pl.concat( frames, how = 'vertical' )
        else:
            self.tables[ '__table_processes__' ] = pl.DataFrame(
                schema = table_schemas[ '__table_processes__' ]
            )
        #

        self._init_tables: dict[ str, tableInit.TableInitRef ] = table_inits or {}
        self._init_processes: dict[ str, tableInit.ProcessInitRef ] = process_inits or {}
        self.tableProcessFactories: dict[ str, 'Callable' ] = tableProcessFactories or {}

        _init_ops = init_table_ops or {}
        for _ref in self._init_tables.values():
            existing_ops = self.tables[ '__table_processes__' ].filter(
                pl.col( 'parent_id' ).is_null()
            )[ 'op_id' ].to_list()
            if _ref.op_id not in existing_ops:
                self._register_process_df( _init_ops[ _ref.op_id ] )
            #
        #/for _ref in self._init_tables.values()

        self.inbox = inbox or Queue()
        self.outbox = outbox or Queue()
        self.log = log or Queue()

        self.verbose = verbose
        
        self._heartbeat:      threading.Event = threading.Event()
        self._transport_stop: threading.Event = threading.Event()
        self._heartbeat_thread: threading.Thread | None         = None

        return
    #/def __init__
    
    @classmethod
    def init_default_with_main_id(
        cls,
        main_id: str,
        rng: np.random.Generator | None = None,
        main_root: str | None = None,
        verbose: int = 0,
        ) -> Self:
        """
            Initialize from main_root or getcwd();
            
            {main_root}/
                settings/
                    paths_index.json
                        - 'default': "./default/paths.json"
                        - {main_id}: "./{main_id}/paths.json"
                    default/
                        paths.json
                            - '__main_root__': "../.."
                            - '__data_root_default__': "data/default"
                    {main_id}/
                        paths.json
                            - '__data_root__': "data/{main_id}"
                data/
                    default/
                    {main_id}/
                    
            Setup a data root at main_root/data/{main_id}
        """
        from os import path
        if main_root is None:
            from os import getcwd
            main_root = getcwd()
        #
        
        if verbose > 0:
            print(
                "Initializing default"
            )
            print(
                "  main_id='{}'".format(
                    main_id
                )
            )
            print(
                "  main_root='{}'".format(
                    main_root
                )
            )
        #/if verbose > 0

        
        # Get a list of dirs we need to make
        mkdir_list: list[ str ] = []
        
        # Data
        # {main_root}/data
        if not path.isdir(
            data_all_dir:= path.join(
                main_root, "data"
            )
        ):
            mkdir_list.append( data_all_dir )
        #
        
        # {main_root}/data/{main_id}
        if not path.isdir(
            data_self_dir:= path.join(
                data_all_dir, main_id
            )
        ):
            mkdir_list.append( data_self_dir )
        #
        # {main_root}/data/default
        if not path.isdir(
            data_default_dir:= path.join(
                data_all_dir, "default",
            )
        ):
            mkdir_list.append( data_default_dir )
        #
        
        # Settings
        # {main_root}/settings
        if not path.isdir(
            settings_all_dir:= path.join(
                main_root, "settings"
            )
        ):
            mkdir_list.append( settings_all_dir )
        #
        
        # {main_root}/settings/{main_id}
        if not path.isdir(
            settings_self_dir:= path.join(
                settings_all_dir, main_id
            )
        ):
            mkdir_list.append( settings_self_dir )
        #
        # {main_root}/settings/default
        if not path.isdir(
            settings_default_dir:= path.join(
                settings_all_dir, "default",
            )
        ):
            mkdir_list.append( settings_default_dir )
        #
        
        # TEST
        paths_default_dict: dict[ str, str ] = {
            '__main_root__': path.relpath(
                main_root,
                start = settings_default_dir,
            ),
            # Relative to main_root
            '__data_root_default__': path.join(
                "data",
                "default",
            )
        }
        print( paths_default_dict )
        raise Exception("paths_default_dict")
        
        if mkdir_list:
            print("Create directories?")
            for _dir in mkdir_list:
                print("  {}".format(_dir))
            #
            
            if not input(
                "'y' to continue\n".format(
                    data_self_dir
                )
            ) == "y":
                quit()
            #/if not { verify making dirs }
            from os import mkdir
            
            if verbose > 0:
                print( "mkdir:")
            #
            for _dir in mkdir_list:
                if verbose > 0:
                    print("  {}".format( _dir ) )
                #
                mkdir( _dir )
            #/for _dir in mkdir_list
        #/if mkdir_list
        
        # Paths
        import json
        if not path.isfile(
            paths_index_fp := path.join(
                settings_all_dir, 'paths_index.json',
            )
        ):
            paths_index_dict: dict[ str, str ] = {
                'default': path.join(
                    ".",
                    "default",
                    "paths.json",
                ),
                main_id: path.join(
                    ".",
                    main_id,
                    "paths.json",
                )
            }
            with open( paths_index_dict, "w", ) as f:
                json.dump(
                    paths_index_dict, f,
                    indent = 2
                )
            #
        #/if not path.isfile( { paths_index_fp } )
        else:
            # Check we're in paths_index
            paths_index_dict: dict[ str, str ]
            with open( paths_index_fp ) as f:
                paths_index_dict = json.load( f )
            #
            if main_id not in paths_index_dict:
                # Add main_id and save
                paths_index_dict[ main_id ] = path.join(
                    ".",
                    main_id,
                    "paths.json",
                )
                
                with open( paths_index_fp, 'w', ) as f:
                    json.dump(
                        paths_index_dict, f,
                        indent = 2,
                    )
                #
            #
        #/if not path.isfile( { paths_index_fp } )/else
        
        paths_default_dict: dict[ str, str ]
        if not path.isfile(
            paths_default_fp := path.join(
                settings_default_dir,
                'paths.json',
            )
        ):
            paths_default_dict = {
                '__main_root__': path.relpath(
                    main_root,
                    start = settings_default_dir,
                ),
                # Relative to main_root
                '__data_root_default__': path.join(
                    "data",
                    "default",
                )
            }
            with open( paths_default_fp, 'w', ) as f:
                json.dump(
                    paths_default_dict, f,
                    indent = 2
                )
            #
        #/if not path.isfile( { paths_default_fp } )
        else:
            with open( paths_default_fp, 'w', ) as f:
                paths_default_dict = json.load( f )
            #
        #/if not path.isfile( { paths_default_fp } )/else
        
        paths_self_dict: dict[ str, str ]
        if not path.isfile(
            paths_self_fp := path.join(
                settings_self_dir,
                "paths.json",
            )
        ):
            paths_self_dict = {
                '__data_root__': path.join(
                    "data",
                    main_id,
                )
            }
            with open( paths_self_fp, 'w', ) as f:
                json.dump(
                    paths_self_dict, f,
                    indent = 2,
                )
            #
        #/if not path.isfile( { paths_self_fp } )
        else:
            with open( paths_self_fp ) as f:
                paths_self_dict = json.load( f )
            #
        #/if not path.isfile( { paths_self_fp } )/else
        
        return cls(
            main_id = main_id,
            rng = rng,
            tables = {
                'paths': utilities.paths_df_from_dict(
                    paths_default_dict
                    | paths_self_dict
                ),
            },
            verbose = verbose,
        )
    #/def init_default_with_main_id
    
    @classmethod
    def load_with_main_id(
        cls,
        main_id: str,
        paths_index_fp: str | None = None,
        paths_dict: dict[ str, str ] | None = None,
        rng: np.random.Generator | None = None,
        verbose: int = 0,
        ) -> Self:
        """
            Find tables['paths'] resolving the paths using utilities.get_paths_for_main_id
        """
        if paths_dict is None:
            paths_dict = utilities.get_paths_for_main_id(
                main_id = main_id,
                paths_index_fp = paths_index_fp,
            )
        #
        
        return cls(
            main_id = main_id,
            rng = rng,
            tables = {
                'paths': utilities.paths_df_from_dict(
                    paths_dict,
                ),
            },
            verbose = verbose,
        )
    #/def load_with_main_id
    
    def _register_process_df(
        self: Self,
        process: types.TaggedTableProcess,
    ) -> None:
        """Serialize `process` and append its rows to tables['__table_processes__']."""
        existing = self.tables[ '__table_processes__' ]
        next_id  = int( existing[ 'node_id' ].max() + 1 ) if existing.shape[0] > 0 else 0
        df, _    = process.as_polars( start_id = next_id )
        self.tables[ '__table_processes__' ] = pl.concat( [ existing, df ], how = 'vertical' )
    #/def _register_process_df

    def notify(self) -> None:
        self._heartbeat.set()
    #/def notify

    def absorb_back(
        self: Self,
        sub_web: Self,
        ) -> None:
        """
        Reverse a partition_out: swap the real processes from ``sub_web`` back
        into this web and remove the routing entries for ``sub_web``.

        Stop ``sub_web`` before calling this; once absorbed its threads are no
        longer needed:

            sub_web.stop()
            sub_web.join()
            web.absorb_back( sub_web )

        No tableOps merge is performed — ``partition_out`` already copied
        ``web.tableOps`` in full, so the re-installed processes resolve against
        the existing tableOps without any additions.
        """
        sub_df = sub_web.tables.get(
            '__table_processes__',
            pl.DataFrame( schema = table_schemas[ '__table_processes__' ] ),
        )
        if sub_df.shape[0] > 0:
            sub_rows = { r[ 'node_id' ]: r for r in sub_df.to_dicts() }
            all_pids: set[int] = set()
            for op_id in sub_web.inputIds:
                root_pid = tableProcesses.find_root_pid( sub_df, op_id )
                all_pids |= _collect_subtree_pids( sub_rows, root_pid )
            #
            subtree_df = sub_df.filter( pl.col( 'node_id' ).is_in( list( all_pids ) ) )
            existing = self.tables[ '__table_processes__' ]
            offset = int( existing[ 'node_id' ].max() + 1 ) if existing.shape[0] > 0 else 0
            self.tables[ '__table_processes__' ] = pl.concat(
                [ existing, tableProcesses.reindex_process_df( subtree_df, offset ) ],
                how = 'vertical',
            )
        #

        routes = self.tables.get( 'routes' )
        if routes is not None:
            self.tables[ 'routes' ] = routes.filter(
                pl.col( 'web_id' ) != sub_web.main_id
            )
        #/if routes is not None
    #/def absorb_back

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
        """Run a process by op_id with explicit dfs; does not write to self.tables."""
        df       = self.tables[ '__table_processes__' ]
        root_pid = tableProcesses.find_root_pid( df, id )
        return tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
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
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        """Run a process by op_id with explicit dfs and write results to self.tables."""
        df       = self.tables[ '__table_processes__' ]
        root_pid = tableProcesses.find_root_pid( df, id )
        rows     = { r[ 'node_id' ]: r for r in df.to_dicts() }
        top_row  = rows[ root_pid ]
        if not allow_new and not all( tid in self.tables for tid in top_row[ 'target' ] ):
            raise ValueError( "Bad target={}".format( top_row[ 'target' ] ) )
        result = tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
        self.tables |= dict( zip( top_row[ 'target' ], result ) )
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
        return self.run_df(
            op_id          = id,
            allow_new      = allow_new,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def run_id

    def run_df(
        self: Self,
        op_id: str,
        df: pl.DataFrame | None = None,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> None:
        if df is None:
            df = self.tables[ '__table_processes__' ]
        root_pid   = tableProcesses.find_root_pid( df, op_id )
        rows       = { r[ 'node_id' ]: r for r in df.to_dicts() }
        top_row    = rows[ root_pid ]
        top_source = top_row[ 'source' ]
        top_target = top_row[ 'target' ]
        if not allow_new:
            if not all( tid in self.tables for tid in top_target ):
                raise ValueError( "Bad target={}".format( top_target ) )
        dfs = tuple( self.tables[ tid ] for tid in top_source )
        result = tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
        self.tables |= dict( zip( top_target, result ) )
        return
    #/def run_df

    def process_inbox_once(
        self,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
    ) -> bool:
        """
            Consume one item from inbox and dispatch it to the matching tableProcess.
            Items are (op_id, dfs) tuples placed there by WebReceiver.
            Returns True if an item was processed, False if inbox was empty.
        """
        try:
            item = self.inbox.get_nowait()
        except Empty:
            return False
        op_id, dfs = item
        df       = self.tables[ '__table_processes__' ]
        root_pid = tableProcesses.find_root_pid( df, op_id )
        rows     = { r[ 'node_id' ]: r for r in df.to_dicts() }
        top_row  = rows[ root_pid ]
        result   = tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
        if allow_new or all( tid in self.tables for tid in top_row[ 'target' ] ):
            self.tables |= dict( zip( top_row[ 'target' ], result ) )
        return True
    #/def process_inbox_once

    def process_inbox_all(
        self,
        allow_new: bool = False,
        verbose: int = 0,
        verbose_prefix: str = "",
    ) -> int:
        """
            Consume all pending inbox items. Returns count processed.
        """
        count = 0
        while self.process_inbox_once(
            allow_new = allow_new,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        ):
            count += 1
        return count
    #/def process_inbox_all
    
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
    #/def tableProcessSignature
    
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
        
        df        = self.tables[ '__table_processes__' ]
        rows      = { r[ 'node_id' ]: r for r in df.to_dicts() }
        root_rows = df.filter( pl.col( 'parent_id' ).is_null() ).to_dicts()
        tableKeys = set( self.tables.keys() )
        result: dict[ str, types.TaggedTableProcessSignature ] = {}

        for root_row in root_rows:
            if root_row[ 'type' ] == 'Op':
                continue
            sources, targets = _collect_subtree_sources_targets( rows, root_row[ 'node_id' ] )
            external_inputs  = sources - targets
            result[ root_row[ 'op_id' ] ] = types.TaggedTableProcessSignature(
                source       = sorted( external_inputs - tableKeys ),
                context      = sorted( sources & tableKeys ),
                target       = sorted( targets & tableKeys ),
                intermediate = sorted( targets - tableKeys ),
            )
        #/for root_row in root_rows

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
