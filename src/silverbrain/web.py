#
#//  webNu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/11/26.

from . import tableOps, tableProcesses, types
from .tableOps import TableOpSchema
from .schema import table_schemas, table_schemas_to_df
from .polarsDataTypeStrings import dtype_to_str
from .tableInit import (
    TableInitRef, ProcessInitRef,
    process_init_df_from_dict,
    topological_init_order_from_df,
)

import polars as pl

from typing import Self

# -- System table schemas
_SYSTEM_SCHEMAS: dict[ str, dict ] = {
    '__tables_schema__':    table_schemas[ '__tables_schema__'   ],
    '__table_processes__':  table_schemas[ '__table_processes__' ],
    '__table_op_schema__':  table_schemas[ '__table_op_schema__'  ],
    '__table_op_effects__': table_schemas[ '__table_op_effects__' ],
    '__table_init__':       table_schemas[ '__table_init__'  ],
    '__process_init__':     table_schemas[ '__process_init__'],
}

# -- Built-in tableOps (shared across all Web instances)

# concat new process rows (reindexed to avoid ID collisions) onto __table_processes__
_register_process_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat(
            [
                dfs[ 1 ],  # existing __table_processes__
                tableProcesses.reindex_process_df(
                    dfs[ 0 ],  # __new_process__ (serialized at start_id=0)
                    offset = int( dfs[ 1 ][ 'node_id' ].max() + 1 )
                             if dfs[ 1 ].shape[ 0 ] > 0 else 0,
                ),
            ],
            how = 'vertical',
        ),
    )
)

# concat new schema rows onto __table_op_schema__
_register_op_schema_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat( [ dfs[ 1 ], dfs[ 0 ] ], how = 'vertical' ),
    )
)

# concat new effects rows onto __table_op_effects__
_register_op_effects_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat( [ dfs[ 1 ], dfs[ 0 ] ], how = 'vertical' ),
    )
)

class _ComputeOpBindingsOp( tableOps.TableOp ):
    """
        Compute the op_bindings for each tableProcess, from __table_processes__ to __op_bindings__. Gets run in `web.init_data`, once all table processes have been registered
    """
    def _collect_subtree_pids(
        self: Self,
        rows: dict,
        pid:  int,
        ) -> set[ int ]:
        """
        Return the set of all node_ids in the subtree rooted at pid.
        """
        pids = { pid }
        row  = rows[ pid ]
        for child in ( row.get( 'term_ids' ) or [] ):
            pids |= self._collect_subtree_pids( rows, child )
        #
        if row.get( 'condition' ) is not None:
            pids |= self._collect_subtree_pids( rows, row[ 'condition' ] )
        #
        for child in ( row.get( 'ifs' ) or [] ):
            pids |= self._collect_subtree_pids( rows, child )
        #
        for child in ( row.get( 'thens' ) or [] ):
            pids |= self._collect_subtree_pids( rows, child )
        #
        return pids
    #/def _collect_subtree_pids
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        """
            :param dfs:
                - [0]: __table_processes__
            :returns:
                - [0]: __op_bindings__
        """
        rows      = {
            r[ 'node_id' ]: r for r in dfs[0].to_dicts()
        }
        root_rows = dfs[0].filter(
            pl.col( 'parent_id' ).is_null() & ( pl.col( 'type' ) != 'Op' )
        ).to_dicts()
        
        frames: list[ pl.DataFrame ] = []
        for root_row in root_rows:
            root_op_id = root_row[ 'op_id' ]
            for pid in self._collect_subtree_pids( rows, root_row[ 'node_id' ] ):
                r = rows[ pid ]
                if r[ 'type' ] == 'Op' or not r.get( 'source' ):
                    continue
                frames.append( pl.DataFrame(
                    [{
                        'root_op_id': root_op_id,
                        'op_id':      r[ 'op_id' ],
                        'source':     r[ 'source' ] or [],
                        'target':     r[ 'target' ] or [],
                    }],
                    schema = table_schemas[ 'op_bindings' ],
                ) )
            #
        #
        
        op_bindings_df: pl.DataFrame = (
            pl.concat( frames, how = 'vertical' )
            if frames
            else pl.DataFrame( schema = table_schemas[ 'op_bindings' ] )
        )
        
        return ( op_bindings_df, )
    #/def __call__
#/class _ComputeOpBindingsOp

_BUILTIN_OPS: types.TableProcessDict = {
    '_register_process_op':     _register_process_op,
    '_register_op_schema_op':   _register_op_schema_op,
    '_register_op_effects_op':  _register_op_effects_op,
    '_compute_op_bindings_op':  _ComputeOpBindingsOp(),
    '_always_true':  lambda dfs, verbose=0, verbose_prefix='': True,
    '_always_false': lambda dfs, verbose=0, verbose_prefix='': False,
}

# -- Built-in process definitions

_COMPUTE_OP_BINDINGS_PROCESS = tableProcesses.TableProcessRef(
    op_id  = 'compute_op_bindings',
    source = ( '__table_processes__', ),
    target = ( 'op_bindings', ),
    terms  = [ '_compute_op_bindings_op' ],
)

_REGISTER_PROCESS_PROCESS = tableProcesses.TableProcessRef(
    op_id   = 'register_process',
    source = ( '__new_process__', '__table_processes__' ),
    target = ( '__table_processes__', ),
    terms  = [ '_register_process_op' ],
)

_REGISTER_TABLEOPS_PROCESS = tableProcesses.TableProcessSequence(
    op_id   = 'register_tableOps',
    source = ( '__new_op_schema__', '__new_op_effects__', '__table_op_schema__', '__table_op_effects__' ),
    target = ( '__table_op_schema__', '__table_op_effects__' ),
    terms  = [
        tableProcesses.TableProcessRef(
            op_id   = '_reg_op_schema',
            source = ( '__new_op_schema__', '__table_op_schema__' ),
            target = ( '__table_op_schema__', ),
            terms  = [ '_register_op_schema_op' ],
        ),
        tableProcesses.TableProcessRef(
            op_id   = '_reg_op_effects',
            source = ( '__new_op_effects__', '__table_op_effects__' ),
            target = ( '__table_op_effects__', ),
            terms  = [ '_register_op_effects_op' ],
        ),
    ],
)


_BUILTIN_ROOT_OP_IDS: frozenset[ str ] = frozenset({
    'register_process',
    'register_tableOps',
    'compute_op_bindings',
})


def _domain_process_df( df: pl.DataFrame ) -> pl.DataFrame:
    """Return __table_processes__ rows not belonging to built-in trees."""
    rows_by_id = { r[ 'node_id' ]: r for r in df.to_dicts() }
    children: dict[ int, list[ int ] ] = { nid: [] for nid in rows_by_id }
    for nid, r in rows_by_id.items():
        if r[ 'parent_id' ] is not None:
            children[ r[ 'parent_id' ] ].append( nid )
    builtin_ids: set[ int ] = set()
    for r in rows_by_id.values():
        if r[ 'parent_id' ] is None and r[ 'op_id' ] in _BUILTIN_ROOT_OP_IDS:
            stack = [ r[ 'node_id' ] ]
            while stack:
                nid = stack.pop()
                builtin_ids.add( nid )
                stack.extend( children.get( nid, [] ) )
    return df.filter( ~pl.col( 'node_id' ).is_in( list( builtin_ids ) ) )
#/def _domain_process_df


def _extract_op_subtree( df: pl.DataFrame, op_id: str ) -> pl.DataFrame:
    """Return all rows in __table_processes__ belonging to op_id's tree."""
    rows_by_id = { r[ 'node_id' ]: r for r in df.to_dicts() }
    children: dict[ int, list[ int ] ] = { nid: [] for nid in rows_by_id }
    for nid, r in rows_by_id.items():
        if r[ 'parent_id' ] is not None:
            children[ r[ 'parent_id' ] ].append( nid )
    roots = [
        r for r in rows_by_id.values()
        if r[ 'parent_id' ] is None and r[ 'op_id' ] == op_id
    ]
    if not roots:
        return df.slice( 0, 0 )
    subtree_ids: set[ int ] = set()
    stack = [ roots[ 0 ][ 'node_id' ] ]
    while stack:
        nid = stack.pop()
        subtree_ids.add( nid )
        stack.extend( children.get( nid, [] ) )
    return df.filter( pl.col( 'node_id' ).is_in( list( subtree_ids ) ) )
#/def _extract_op_subtree


class Web:
    """
    Fresh implementation of Web with a fixed set of system tables that every
    instance carries.

    System tables (always present, named with dunder convention):
        __tables_schema__    — schema registry; one row per table
                               (table_id, column_names, column_types)
        __table_processes__  — serialized process trees; dispatched by apply()
        __table_op_schema__  — per-column schema declarations for registered tableOps
        __table_op_effects__ — side-effect declarations for registered tableOps
        __table_init__       — serialized TableInitRef rows
        __process_init__     — serialized ProcessInitRef rows

    All system-table schemas are themselves rows in __tables_schema__, so the
    table is self-describing from construction.

    Running:
        apply(op_id)  — the single dispatch entry point; looks up op_id in
                        __table_processes__, pulls inputs from self.tables,
                        runs the process, and writes outputs back to self.tables.

    Extension:
        register_tableOps(ops)    — merge ops into self.tableOps and update
                                    __table_op_schema__ / __table_op_effects__.
        register_process(process) — serialize and append a TaggedTableProcess to
                                    __table_processes__ via the staging-table pattern.
        register_tables(tables)   — merge tables into self.tables and append
                                    schema rows to __tables_schema__ for each new
                                    DataFrame.

    The built-in 'register_process' and 'register_tableOps' processes are
    themselves registered during __init__, so they are available via apply().
    """

    def __init__(
        self:      Self,
        main_id:   str,
        input_ids: list[ str ]                = [],
        ops:       types.TableProcessDict | None = None,
        verbose:   int                        = 0,
        ) -> None:
        self.main_id:   str                   = main_id
        self.input_ids: list[ str ]           = list( input_ids )
        self.verbose:   int                   = verbose
        self.tableOps:  types.TableProcessDict = dict( _BUILTIN_OPS )

        # -- Initialise system tables
        self.tables: dict[ str, pl.DataFrame ] = {
            '__table_processes__':  pl.DataFrame( schema = table_schemas[ '__table_processes__'   ] ),
            '__table_op_schema__':  pl.DataFrame( schema = table_schemas[ '__table_op_schema__'    ] ),
            '__table_op_effects__': pl.DataFrame( schema = table_schemas[ '__table_op_effects__'   ] ),
            '__table_init__':       pl.DataFrame( schema = table_schemas[ '__table_init__'    ] ),
            '__process_init__':     pl.DataFrame( schema = table_schemas[ '__process_init__'  ] ),
            # __tables_schema__ bootstraps itself — its own schema is one of the rows
            '__tables_schema__':    table_schemas_to_df( _SYSTEM_SCHEMAS ),
        }

        # -- Seed __table_processes__ with built-in process definitions
        # -- process to register new processes
        _df, _ = _REGISTER_PROCESS_PROCESS.as_polars( start_id = 0 )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df ], how = 'vertical'
        )
        
        # -- process to register table ops
        _next_id = int( self.tables[ '__table_processes__' ][ 'node_id' ].max() + 1 )
        _df2, _  = _REGISTER_TABLEOPS_PROCESS.as_polars( start_id = _next_id )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df2 ], how = 'vertical'
        )
        
        # -- process to compute op bindings
        _next_id = int( self.tables[ '__table_processes__' ][ 'node_id' ].max() + 1 )
        _df3, _  = _COMPUTE_OP_BINDINGS_PROCESS.as_polars( start_id = _next_id )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df3 ], how = 'vertical'
        )

        if ops:
            self.register_tableOps( ops )
    #/def __init__
    
    def bind(
        self: Self,
        address: str,
        context: 'zmq.Context | None' = None,
        ) -> None:
        """
            No op, makes Web conform to types.WebNode
        """
        pass
    #/def bind
    
    # -- Dispatch

    def apply(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        op_id: str,
        processes_table_id: str = '__table_processes__',
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        """
        Stateless dispatch: look up op_id in __table_processes__, run it against
        the provided dfs, and return the result tuple.
        """
        df       = self.tables[ processes_table_id ]
        root_pid = tableProcesses.find_root_pid( df, op_id )
        return tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def apply

    def put(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        op_id: str,
        processes_table_id: str = '__table_processes__',
        verbose: int | None = None,
        verbose_prefix: str = '',
        ) -> None:
        """
        Run op_id against the provided dfs and write the results back to
        self.tables by target name.  Like run but the caller supplies the
        input dfs directly instead of reading them from self.tables.
        """
        _verbose = self.verbose if verbose is None else verbose
        df       = self.tables[ processes_table_id ]
        root_pid = tableProcesses.find_root_pid( df, op_id )
        rows     = { r[ 'node_id' ]: r for r in df.to_dicts() }
        top_row  = rows[ root_pid ]
        result   = self.apply(
            dfs                = dfs,
            op_id              = op_id,
            processes_table_id = processes_table_id,
            verbose            = _verbose,
            verbose_prefix     = verbose_prefix,
        )
        self.tables |= dict(
            zip( top_row[ 'target' ], result )
        )
    #/def put
    
    def get(
        self: Self,
        op_id: str,
        extra: dict[ str, pl.DataFrame ] | None = None,
        processes_table_id: str = '__table_processes__',
        verbose: int | None = None,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        """
            Stateful get, but no change: pull inputs named in source from self.tables merged with extra, run the process, return the result.
        """
        _verbose   = self.verbose if verbose is None else verbose
        df         = self.tables[ processes_table_id ]
        root_pid   = tableProcesses.find_root_pid( df, op_id )
        rows       = {
            r[ 'node_id' ]: r for r in df.to_dicts()
        }
        top_row    = rows[ root_pid ]
        all_tables = dict( self.tables ) | dict( extra or {} )
        dfs        = tuple(
            all_tables[ tid ]
            for tid in top_row[ 'source' ]
        )
        
        return self.apply(
            dfs                 = dfs,
            op_id               = op_id,
            processes_table_id  = processes_table_id,
            verbose             = _verbose,
            verbose_prefix      = verbose_prefix,
        )
    #/def get
    
    def run(
        self: Self,
        op_id: str,
        extra: dict[ str, pl.DataFrame ] | None = None,
        processes_table_id: str = '__table_processes__',
        verbose: int | None = None,
        verbose_prefix: str = '',
        ) -> None:
        """
        Stateful dispatch: pull all inputs named in source from self.tables merged
        with extra, run the process, and write outputs back to self.tables by target.
        Pass extra to inject tables not yet in self.tables (e.g. staged inputs).
        """
        _verbose   = self.verbose if verbose is None else verbose
        df         = self.tables[ processes_table_id ]
        root_pid   = tableProcesses.find_root_pid( df, op_id )
        rows       = {
            r[ 'node_id' ]: r for r in df.to_dicts()
        }
        top_row    = rows[ root_pid ]
        all_tables = dict( self.tables ) | dict( extra or {} )
        dfs        = tuple(
            all_tables[ tid ]
            for tid in top_row[ 'source' ]
        )
        
        result     = self.apply(
            dfs                 = dfs,
            op_id               = op_id,
            processes_table_id  = processes_table_id,
            verbose             = _verbose,
            verbose_prefix      = verbose_prefix,
        )
        self.tables |= dict(
            zip( top_row[ 'target' ], result )
        )
    #/def run

    # -- Registration

    def register_tableOps(
        self: Self,
        ops: types.TableProcessDict,
        ) -> None:
        """
        Merge `ops` into self.tableOps and update __table_op_schema__ /
        __table_op_effects__ for ops that expose input / output / effects.
        Dispatches 'register_tableOps' via apply(), passing new rows directly
        as dfs and writing the results back to self.tables.
        """
        self.tableOps |= ops
        
        # Check for schema
        _schema_rows:  list[ pl.DataFrame ] = []
        _effects_rows: list[ pl.DataFrame ] = []
        for op_id, op in ops.items():
            _has_schema  = getattr( op, 'input', None ) is not None or getattr( op, 'output',  None ) is not None
            _has_effects = getattr( op, 'effects', None ) is not None
            if _has_schema:
                _schema_df, _effects_df = TableOpSchema(
                    inputs  = op.input  or [],
                    outputs = op.output or [],
                    effects = op.effects or [],
                ).to_polars( op_id )
                _schema_rows.append( _schema_df )
                if not _effects_df.is_empty():
                    _effects_rows.append( _effects_df )
            elif _has_effects and op.effects:
                _, _effects_df = TableOpSchema(
                    inputs = [], outputs = [], effects = op.effects,
                ).to_polars( op_id )
                _effects_rows.append( _effects_df )
        #/for op_id, op in ops.items()
        
        _new_schema  = (
            pl.concat( _schema_rows,  how = 'vertical' )
            if _schema_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_schema__' ] )
        )
        _new_effects = (
            pl.concat( _effects_rows, how = 'vertical' )
            if _effects_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_effects__' ] )
        )
        
        (
            self.tables[ '__table_op_schema__'  ],
            self.tables[ '__table_op_effects__' ],
        ) = self.apply(
            dfs = (
                _new_schema,
                _new_effects,
                self.tables[ '__table_op_schema__'  ],
                self.tables[ '__table_op_effects__' ],
            ),
            op_id = 'register_tableOps',
        )
    #/def register_tableOps

    def register_process(
        self: Self,
        process: types.TaggedTableProcess,
        ) -> None:
        """
        Serialize `process` and append it to __table_processes__.
        The new rows are serialized at start_id=0; run('register_process')
        reindexes them to avoid collisions before appending.

        NOTE: If you have a process already serialized as a DataFrame, you can call
        run( 'register_process', extra = { '__new_process__': process_df } ) directly.
        """
        _df, _ = process.as_polars( start_id = 0 )
        self.run(
            'register_process',
            extra = { '__new_process__': _df }
        )
    #/def register_process

    def register_tables(
        self: Self,
        tables: dict[ str, pl.DataFrame | None ],
        ) -> None:
        """
        Merge `tables` into self.tables.
        Appends a __tables_schema__ row for each new entry that is a DataFrame
        (None-valued placeholders are merged silently with no schema row).
        """
        
        
        self.tables |= tables
        
        # Collect schema of new tables to add to __tables_schema__
        new_rows: list[ dict ] = []
        for tid, df in tables.items():
            if isinstance( df, pl.DataFrame ) and tid not in self.tables:
                new_rows.append({
                    'table_id':     tid,
                    'column_names': list( df.columns ),
                    'column_types': [ dtype_to_str( df.schema[ c ] ) for c in df.columns ],
                })
            #/if isinstance( df, pl.DataFrame ) and tid not in self.tables
        #/for tid, df
        
        if new_rows:
            self.tables[ '__tables_schema__' ] = pl.concat(
                [
                    self.tables[ '__tables_schema__' ],
                    pl.DataFrame( new_rows, schema = table_schemas[ '__tables_schema__' ] ),
                ],
                how = 'vertical',
            )
        #/if new_rows
    #/def register_tables

    def init_data(
        self: Self,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> None:
        """
        Run all registered init ops in topological dependency order.
        For each entry in topological order derived from __table_init__ and
        __process_init__:
          - table:   run( ref.op_id )   if always_run or table not yet populated
          - process: run( ref.factory_id ) if process not yet in __table_processes__
          
        Factory processes are TaggedTableProcesses already registered in
        __table_processes__; when run they produce and register the target process.
        """
        for _type, _init_id in topological_init_order_from_df(
            self.tables[ '__table_init__'   ],
            self.tables[ '__process_init__' ],
        ):
            if _type == 'table':
                row = self.tables[ '__table_init__' ].filter(
                    pl.col( 'table_id' ) == _init_id
                ).row( 0, named = True )
                if row[ 'always_run' ] or self.tables.get( row[ 'table_id' ] ) is None:
                    self.run(
                        row[ 'op_id' ],
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                #
            else:
                row = self.tables[ '__process_init__' ].filter(
                    pl.col( 'op_id' ) == _init_id
                ).row( 0, named = True )
                if self.tables[ '__table_processes__' ].filter(
                    ( pl.col( 'op_id' ) == row[ 'op_id' ] )
                    & pl.col( 'parent_id' ).is_null()
                ).is_empty():
                    if row[ 'factory_id' ] is None:
                        raise RuntimeError(
                            f"Process '{row[ 'op_id' ]}' has factory_id=None "
                            f"but is absent from __table_processes__"
                        )
                    self.run(
                        row[ 'factory_id' ],
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                #
            #
        #/for _type, _init_id

        self.run( 'compute_op_bindings' )
    #/def init_data

    @classmethod
    def merge(
        cls,
        a: Self,
        b: Self,
        main_id: str = '',
        ) -> Self:
        """
        Produce a new Web combining a and b.
        main_id is '{a.main_id}_{b.main_id}'.
        tableOps: non-builtin ops from both (b overwrites a on conflict).
        __table_processes__: non-builtin trees from both, reindexed to avoid
            node_id collisions; on op_id conflict, a's tree is shadowed by b's.
        __table_init__ / __process_init__: concatenated and deduplicated on
            their primary keys (b wins on conflict).
        Domain tables (non-system keys): a's, then b's (b overwrites a).
        """
        merged = cls(
            main_id   = main_id or f"{ a.main_id }_{ b.main_id }",
            input_ids = list( dict.fromkeys( a.input_ids + b.input_ids ) ),
        )

        _builtin_keys = set( _BUILTIN_OPS )
        ops: types.TableProcessDict = {
            k: v for k, v in a.tableOps.items() if k not in _builtin_keys
        } | {
            k: v for k, v in b.tableOps.items() if k not in _builtin_keys
        }
        
        if ops:
            merged.register_tableOps( ops )
        #
        
        for web in ( a, b ):
            domain_df = _domain_process_df( web.tables[ '__table_processes__' ] )
            if domain_df.is_empty():
                continue
            _max = merged.tables[ '__table_processes__' ][ 'node_id' ].max()
            offset = int( _max + 1 ) if _max is not None else 0
            reindexed = tableProcesses.reindex_process_df( domain_df, offset )
            merged.tables[ '__table_processes__' ] = pl.concat(
                [ merged.tables[ '__table_processes__' ], reindexed ], how = 'vertical',
            )
        #

        for key, pk in (
            ( '__table_init__',    'table_id' ),
            ( '__process_init__',  'op_id'    ),
        ):
            combined = pl.concat(
                [ a.tables[ key ], b.tables[ key ] ],
                how = 'vertical',
            )
            merged.tables[ key ] = combined.unique(
                subset = [ pk ],
                keep = 'last',
            )
        #/for key, pk in (...)

        _system_keys = set( _SYSTEM_SCHEMAS )
        for k, v in a.tables.items():
            if k not in _system_keys:
                merged.tables[ k ] = v
            #/if k not in _system_keys
        #/for k, v in a.tables.items()
        
        for k, v in b.tables.items():
            if k not in _system_keys:
                merged.tables[ k ] = v
            #/if k not in _system_keys
        #/for k, v in b.tables.items()

        return merged
    #/def merge
#/class Web
