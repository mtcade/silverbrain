#
#//  webNu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/11/26.

from . import tableOps, tableProcesses, types
from .tableOps import TableOpSchema
from .tableProcesses import TableProcessRef, TableCheckRef, TableProcessSequence, TableProcessBranch
from .schema import table_schemas, table_schemas_to_df
from .polarsDataTypeStrings import dtype_to_str
from .tableInit import (
    TableInitRef, ProcessInitRef,
    process_init_df_from_dict,
    topological_init_order_from_df,
)

import tomllib
import polars as pl
import numpy as np

import os
import shutil
from pathlib import Path
from typing import Self

# -- System table schemas
_SYSTEM_SCHEMAS: dict[ str, dict ] = {
    '__tables_schema__':    table_schemas[ '__tables_schema__'   ],
    '__table_processes__':  table_schemas[ '__table_processes__' ],
    '__table_op_schema__':  table_schemas[ '__table_op_schema__'  ],
    '__table_op_effects__': table_schemas[ '__table_op_effects__' ],
    '__table_init__':       table_schemas[ '__table_init__'  ],
    '__process_init__':     table_schemas[ '__process_init__'],
    '__routing__':          table_schemas[ '__routing__'     ],
}

class _ComputeOpBindingsOp( tableOps.TableOp ):
    """
        Compute the op_bindings for each tableProcess, from __table_processes__ to __op_bindings__. Gets run in `web.init_data`, once all table processes have been registered
    """
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
        frames: list[ pl.DataFrame ] = []
        for row in dfs[ 0 ].to_dicts():
            if not row.get( 'source' ):
                continue
            frames.append( pl.DataFrame(
                [{
                    'root_op_idn': row[ 'op_idn' ],
                    'op_idn':      row[ 'op_idn' ],
                    'source':      row[ 'source' ] or [],
                    'target':      row[ 'target' ] or [],
                }],
                schema = table_schemas[ 'op_bindings' ],
            ) )
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
    '_compute_op_bindings_op':  _ComputeOpBindingsOp(),
    '_always_true':  lambda dfs, verbose=0, verbose_prefix='': True,
    '_always_false': lambda dfs, verbose=0, verbose_prefix='': False,
    # backend dispatch
    'join_table_backend_op':   tableOps.JoinTableBackendOp(),
    'is_parquet_op':           tableOps.IsParquetBackendOp(),
    'join_parquet_backend_op': tableOps.JoinParquetBackendOp(),
    'write_parquet_op':        tableOps.WriteParquetFragmentOp(),
    'is_sqlite_op':            tableOps.IsSQLiteBackendOp(),
    'join_sqlite_backend_op':  tableOps.JoinSQLiteBackendOp(),
    'write_sqlite_op':         tableOps.WriteSQLiteFragmentOp(),
    'is_postgres_op':          tableOps.IsPostgresBackendOp(),
    'join_postgres_backend_op': tableOps.JoinPostgresBackendOp(),
    'write_postgres_op':       tableOps.WritePostgresFragmentOp(),
}

# -- Built-in process definitions

_COMPUTE_OP_BINDINGS_PROCESS = tableProcesses.TableProcessRef(
    op_idn = 'compute_op_bindings',
    source = ( '__table_processes__', ),
    target = ( 'op_bindings', ),
    terms  = [ '_compute_op_bindings_op' ],
)

_BUILTIN_ROOT_OP_IDS: frozenset[ str ] = frozenset({
    'compute_op_bindings',
})

# -- Table saving and reading; Uses the internal table "_table", which you'll have to bind by wrapping referencing with another TableProcessRef which passes through and renames it to "_table"

# Backend write
_JOIN_TABLE_BACKEND_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'join_table_backend',
    source = ( 'table_ref', '__backend_index__', ),
    target = ( '_table_backend_ref',),
    terms = [ 'join_table_backend_op', ],
)

# Backend write -- parquet
_CHECK_PARQUET_PROCESS: tableProcesses.TableCheckRef = TableCheckRef(
    op_idn = 'is_parquet',
    source = ( '_table_backend_ref',),
    terms = [ 'is_parquet_op', ],
)

_JOIN_PARQUET_BACKEND_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'join_parquet_backend',
    source = ( '_table_backend_ref', '__parquet_backend__',),
    target = ( '_table_backend_ref_written',),
    terms = [ 'join_parquet_backend_op', ],
)

_WRITE_PARQUET_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'write_parquet',
    source = ( '_table', '_table_backend_ref_parquet',),
    target = ( '_table_backend_ref_parquet',),
    terms = [ 'write_parquet_op' ],
)

_WRITE_PARQUET_SEQUENCE: tableProcesses.TableProcessSequence = TableProcessSequence(
    op_idn = 'write_parquet_sequence',
    source = ( '_table', '_table_backend_ref','__parquet_backend__',),
    target = ( '_table_backend_ref_written',),
    terms = [
        'join_parquet_backend',
        'write_parquet',
    ]
)

# Backend write -- sqlite
_CHECK_SQLITE_PROCESS: tableProcesses.TableCheckRef = TableCheckRef(
    op_idn = 'is_sqlite',
    source = ( '_table_backend_ref',),
    terms = [ 'is_sqlite_op', ],
)

_JOIN_SQLITE_BACKEND_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'join_sqlite_backend',
    source = ( '_table_backend_ref','__sqlite_backend__',),
    target = ( '_table_backend_ref_sqlite',),
    terms = [ 'join_sqlite_backend_op', ],
)

_WRITE_SQLITE_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'write_sqlite',
    source = ( '_table', '_table_backend_ref_sqlite',),
    target = ( '_table_backend_ref_written',),
    terms = [ 'write_sqlite_op', ],
)

_WRITE_SQLITE_SEQUENCE: tableProcesses.TableProcessSequence = TableProcessSequence(
    op_idn = 'write_sqlite_sequence',
    source = ( '_table', '_table_backend_ref','__sqlite_backend__',),
    target = ( '_table_backend_ref_written',),
    terms = [
        'join_sqlite_backend',
        'write_sqlite',
    ]
)

# Backend write -- postgres
_CHECK_POSTGRES_PROCESS: tableProcesses.TableCheckRef = TableCheckRef(
    op_idn = 'is_postgres',
    source = ( '_table_backend_ref',),
    terms = [ 'is_postgres_op', ],
)

_JOIN_POSTGRES_BACKEND_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'join_postgres_backend',
    source = ( '_table_backend_ref','__postgres_backend__',),
    target = ( '_table_backend_ref_postgres',),
    terms = [ 'join_postgres_backend_op', ],
)

_WRITE_POSTGRES_PROCESS: tableProcesses.TableProcessRef = TableProcessRef(
    op_idn = 'write_postgres',
    source = ( '_table', '_table_backend_ref_postgres',),
    target = ( '_table_backend_ref_written',),
    terms = [ 'write_postgres_op' ],
)

_WRITE_POSTGRES_SEQUENCE: tableProcesses.TableProcessSequence = TableProcessSequence(
    op_idn = 'write_postgres_sequence',
    source = ( '_table', '_table_backend_ref','__postgres_backend__',),
    target = ( '_table_backend_ref_written',),
    terms = [
        'join_postgres_backend',
        'write_postgres',
    ]
)

# Determine the backend_type, and write accordingly
_WRITE_TABLE_BRANCH_PROCESS: tableProcesses.TableProcessBranch = TableProcessBranch(
    op_idn = 'write_table_branch',
    source = ( '_table', '_table_backend_ref',),
    target = ( '_table_backend_ref_written', ),
    ifs = [
        'is_parquet','is_sqlite','is_postgres',
    ],
    thens = [
        'write_parquet_sequence',
        'write_sqlite_sequence',
        'write_postgres_sequence',
    ],
)

_TABLE_WRITER_FRAGMENT_PROCESS: tableProcesses.TableProcessSequence = TableProcessSequence(
    op_idn = '__table_writer_branch__',
    source = (
        '_table', 'table_ref', '__backend_index__',
        '__parquet_backend__','__sqlite_backend__','__postgres_backend__',
    ),
    target = (),
    terms = [
        'join_table_backend',
        'write_table_branch',
    ]
)


def _domain_process_df( df: pl.DataFrame ) -> pl.DataFrame:
    """Return __table_processes__ rows eligible for reactive firing in run_domain."""
    return df.filter(
        ~pl.col( 'op_idn' ).is_in( list( _BUILTIN_ROOT_OP_IDS ) )
        & ( pl.col( 'type' ) != 'TableCheckRef' )
    )
#/def _domain_process_df


def _collect_op_ids( rows: dict, op_id: str ) -> set[ str ]:
    """Return op_id plus all transitively referenced process op_ids."""
    return tableProcesses.collect_process_deps( rows, op_id )
#/def _collect_op_ids


def _extract_op_subtree( df: pl.DataFrame, op_idn: str ) -> pl.DataFrame:
    """Return all rows needed to execute op_idn (op_idn + all transitive process deps)."""
    rows = { r[ 'op_idn' ]: r for r in df.to_dicts() }
    if op_idn not in rows:
        return df.slice( 0, 0 )
    all_op_idns = tableProcesses.collect_process_deps( rows, op_idn )
    return df.filter( pl.col( 'op_idn' ).is_in( list( all_op_idns ) ) )
#/def _extract_op_subtree


class Cell:
    """
    Fresh implementation of Web with a fixed set of system tables that every
    instance carries.

    System tables (always present, named with dunder convention):
        __tables_schema__    — schema registry; one row per table
                               (table_id, column_names, column_types)
        __table_processes__  — serialized process DAG; dispatched by apply()
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
                                    __table_processes__.
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
            '__routing__':          pl.DataFrame( schema = table_schemas[ '__routing__'       ] ),
            # __tables_schema__ bootstraps itself — its own schema is one of the rows
            '__tables_schema__':    table_schemas_to_df( _SYSTEM_SCHEMAS ),
        }

        # -- Seed __table_processes__ with built-in process definition
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _COMPUTE_OP_BINDINGS_PROCESS.as_polars() ],
            how = 'vertical',
        )

        if ops:
            self.register_tableOps( ops )
        #/if ops
    #/def __init__


    # -- Dispatch

    def run(
        self: Self,
        op_id: str,
        extra: dict[ str, pl.DataFrame ] | None = None,
        dfs: tuple[ pl.DataFrame, ... ] | None = None,
        processes_table_id: str = '__table_processes__',
        verbose: int | None = None,
        verbose_prefix: str = '',
        ) -> None:
        """
        Stateful dispatch: pull all inputs named in source from self.tables merged
        with extra, run the process, and write outputs back to self.tables by target.
        Pass extra to inject tables not yet in self.tables (e.g. staged inputs).
        Pass dfs to supply positional inputs; they are mapped to source names via
        __table_processes__ and merged into extra.
        """
        _verbose = self.verbose if verbose is None else verbose
        if dfs is not None:
            tp_df  = self.tables[ processes_table_id ]
            rows_d = { r[ 'op_idn' ]: r for r in tp_df.to_dicts() }
            source_ids = rows_d[ op_id ].get( 'source' ) or []
            mapped = dict( zip( source_ids, dfs ) )
            extra  = mapped | ( extra or {} )
        all_tables = dict( self.tables ) | { k: v for k, v in dict( extra or {} ).items() if v is not None }
        df         = self.tables[ processes_table_id ]
        rows       = { r[ 'op_idn' ]: r for r in df.to_dicts() }
        row        = rows[ op_id ]
        _dfs       = tuple( all_tables[ tid ] for tid in row[ 'source' ] )
        result     = tableProcesses.run_from_df(
            df             = df,
            root_op_idn    = op_id,
            dfs            = _dfs,
            tableOps       = self.tableOps,
            verbose        = _verbose,
            verbose_prefix = verbose_prefix,
        )
        self.tables |= dict( zip( row[ 'target' ], result ) )
    #/def run

    def run_domain(
        self:           Self,
        inputs:         dict[ str, pl.DataFrame ],
        verbose:        int = 0,
        verbose_prefix: str = '',
    ) -> set[ str ]:
        """
        Merge inputs into self.tables and reactively fire all ready domain ops
        until quiescent.  Returns the set of op_idns that fired.
        """
        if inputs:
            self.tables |= inputs
        domain_df = _domain_process_df( self.tables[ '__table_processes__' ] )
        rows      = { r[ 'op_idn' ]: r for r in domain_df.to_dicts() }
        fired: set[ str ] = set()
        changed = True
        while changed:
            changed = False
            for op_idn, row in rows.items():
                if op_idn in fired:
                    continue
                sources = row.get( 'source' ) or []
                if sources and all( self.tables.get( s ) is not None for s in sources ):
                    self.run(
                        op_id          = op_idn,
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                    fired.add( op_idn )
                    changed = True
        return fired
    #/def run_domain

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

        self.tables[ '__table_op_schema__'  ] = pl.concat(
            [ self.tables[ '__table_op_schema__'  ], _new_schema  ], how = 'vertical',
        )
        self.tables[ '__table_op_effects__' ] = pl.concat(
            [ self.tables[ '__table_op_effects__' ], _new_effects ], how = 'vertical',
        )
    #/def register_tableOps

    def register_process(
        self: Self,
        process: types.TaggedTableProcess,
        validate_children: bool = False,
        ) -> None:
        """
        Serialize `process` and append it to __table_processes__.

        validate_children: if True, raise KeyError if any referenced child
            op_id does not exist in __table_processes__ (for composite types)
            or in self.tableOps (for atomic types).

        NOTE: If you have a process already serialized as a DataFrame, you can call
        run( 'register_process', extra = { '__new_process__': process_df } ) directly.
        """
        _df = process.as_polars()

        if validate_children:
            existing_procs = set( self.tables[ '__table_processes__' ][ 'op_idn' ].to_list() )
            row = _df.row( 0, named = True )
            t   = row[ 'type' ]
            if t in ( 'TableProcessRef', 'TableCheckRef' ):
                for term in ( row.get( 'term_idns' ) or [] ):
                    if term not in self.tableOps:
                        raise KeyError( f"tableOp {term!r} not registered" )
            else:
                for term in ( row.get( 'term_idns' ) or [] ):
                    if term not in existing_procs:
                        raise KeyError( f"process {term!r} not registered" )
                for term in ( row.get( 'ifs' ) or [] ):
                    if term not in existing_procs:
                        raise KeyError( f"check process {term!r} not registered" )
                for term in ( row.get( 'thens' ) or [] ):
                    if term not in existing_procs:
                        raise KeyError( f"then process {term!r} not registered" )
                if cond := row.get( 'condition' ):
                    if cond not in existing_procs:
                        raise KeyError( f"condition process {cond!r} not registered" )

        if isinstance( process, tableProcesses.Alias ):
            aliased_rows = self.tables[ '__table_processes__' ].filter(
                pl.col( 'op_idn' ) == process.aliases
            )
            if aliased_rows.is_empty():
                raise KeyError( f"aliased process {process.aliases!r} not registered" )
            aliased_target = set( aliased_rows.row( 0, named = True )[ 'target' ] or [] )
            if not set( process.target ).issubset( aliased_target ):
                raise ValueError(
                    f"Alias.target {list( process.target )} is not a subset of "
                    f"{process.aliases}.target {sorted( aliased_target )}"
                )

        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df ], how = 'vertical',
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
                    'table_idn':    tid,
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

    def validate_tableProcesses( self: Self ) -> None:
        """
        Validate the __table_processes__ DAG:
          1. All referenced op_ids exist (processes reference processes;
             TableProcessRef/TableCheckRef terms reference tableOps)
          2. No cycles
          3. Terminal nodes (no child process references) are TableProcessRef or TableCheckRef
        """
        df   = self.tables[ '__table_processes__' ]
        rows = { r[ 'op_idn' ]: r for r in df.to_dicts() }
        existing_procs = set( rows.keys() )

        def _process_refs( row: dict ) -> list[ str ]:
            """Return all op_ids this row references that should be in __table_processes__."""
            t = row[ 'type' ]
            if t in ( 'TableProcessRef', 'TableCheckRef' ):
                return []
            refs = list( row.get( 'term_idns' ) or [] )
            refs += list( row.get( 'ifs' ) or [] )
            refs += list( row.get( 'thens' ) or [] )
            if cond := row.get( 'condition' ):
                refs.append( cond )
            return refs

        # 1. Reference check
        for op_id, row in rows.items():
            t = row[ 'type' ]
            if t in ( 'TableProcessRef', 'TableCheckRef' ):
                for term in ( row.get( 'term_idns' ) or [] ):
                    if term not in self.tableOps:
                        raise KeyError( f"Process {op_id!r} references unknown tableOp {term!r}" )
            else:
                for ref in _process_refs( row ):
                    if ref not in existing_procs:
                        raise KeyError( f"Process {op_id!r} references unknown process {ref!r}" )

        # 2. Cycle check (DFS)
        visited: set[ str ] = set()
        in_stack: set[ str ] = set()

        def _has_cycle( op_id: str ) -> bool:
            if op_id in in_stack:
                return True
            if op_id in visited:
                return False
            visited.add( op_id )
            in_stack.add( op_id )
            for ref in _process_refs( rows[ op_id ] ):
                if ref in rows and _has_cycle( ref ):
                    return True
            in_stack.remove( op_id )
            return False

        for op_id in existing_procs:
            if _has_cycle( op_id ):
                raise ValueError( f"__table_processes__ contains a cycle involving {op_id!r}" )

        # 3. Terminal nodes must be TableProcessRef or TableCheckRef
        for op_id, row in rows.items():
            if not _process_refs( row ) and row[ 'type' ] not in ( 'TableProcessRef', 'TableCheckRef' ):
                raise ValueError(
                    f"Terminal process {op_id!r} must be TableProcessRef or TableCheckRef, "
                    f"got {row['type']!r}"
                )
    #/def validate_tableProcesses

    def reset_data(
        self: Self,
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> None:
        if 'paths' not in self.tables:
            return
        rows = self.tables[ 'paths' ].filter( pl.col( 'key' ) == '__data_root__' )
        if rows.is_empty():
            return
        data_root = rows[ 'fp' ][ 0 ]
        if not os.path.isdir( data_root ):
            return
        for entry in os.listdir( data_root ):
            entry_path = os.path.join( data_root, entry )
            if os.path.isfile( entry_path ):
                os.remove( entry_path )
            elif os.path.isdir( entry_path ):
                shutil.rmtree( entry_path )
    #/def reset_data

    def init_data(
        self: Self,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> None:
        """
        Run all registered init ops in topological dependency order.
        For each entry in topological order derived from __table_init__ and
        __process_init__:
          - table:   run( ref.op_idn )   if always_run or table not yet populated
          - process: run( ref.factory_idn ) if process not yet in __table_processes__

        Factory processes are TaggedTableProcesses already registered in
        __table_processes__; when run they produce and register the target process.
        """
        for _type, _init_id in topological_init_order_from_df(
            self.tables[ '__table_init__'   ],
            self.tables[ '__process_init__' ],
        ):
            if _type == 'table':
                row = self.tables[ '__table_init__' ].filter(
                    pl.col( 'table_idn' ) == _init_id
                ).row( 0, named = True )
                if row[ 'always_run' ] or self.tables.get( row[ 'table_idn' ] ) is None:
                    self.run(
                        row[ 'op_idn' ],
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                #
            else:
                row = self.tables[ '__process_init__' ].filter(
                    pl.col( 'op_idn' ) == _init_id
                ).row( 0, named = True )
                if self.tables[ '__table_processes__' ].filter(
                    pl.col( 'op_idn' ) == row[ 'op_idn' ]
                ).is_empty():
                    if row[ 'factory_idn' ] is None:
                        raise RuntimeError(
                            f"Process '{row[ 'op_idn' ]}' has factory_idn=None "
                            f"but is absent from __table_processes__"
                        )
                    self.run(
                        row[ 'factory_idn' ],
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
        Produce a new Cell combining a and b.
        main_id is '{a.main_id}_{b.main_id}'.
        tableOps: non-builtin ops from both (b overwrites a on conflict).
        __table_processes__: non-builtin rows from both; on op_id conflict b wins.
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

        # Merge domain process rows (b wins on op_id conflict)
        domain_a = _domain_process_df( a.tables[ '__table_processes__' ] )
        domain_b = _domain_process_df( b.tables[ '__table_processes__' ] )
        combined = pl.concat( [ domain_a, domain_b ], how = 'vertical' )
        if not combined.is_empty():
            deduped = combined.unique( subset = [ 'op_idn' ], keep = 'last' )
            merged.tables[ '__table_processes__' ] = pl.concat(
                [ merged.tables[ '__table_processes__' ], deduped ], how = 'vertical',
            )
        #

        for key, pk in (
            ( '__table_init__',    'table_idn' ),
            ( '__process_init__',  'op_idn'    ),
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

    def print_tableProcesses(
        self:               Self,
        processes_table_id: str = '__table_processes__',
        verbose_prefix:     str = '',
        ) -> None:
        """
        Print every domain (non-builtin) process, showing each as a node
        with its source, terms/ifs/thens/condition, and target.
        Processes that are only referenced as children of others are indented.
        """
        df   = _domain_process_df( self.tables[ processes_table_id ] )
        rows = { r[ 'op_idn' ]: r for r in df.to_dicts() }

        # Find processes not referenced by any other domain process
        referenced: set[ str ] = set()
        for row in rows.values():
            for t in ( row.get( 'term_idns' ) or [] ):
                if t in rows:
                    referenced.add( t )
            for t in ( row.get( 'ifs' ) or [] ):
                if t in rows:
                    referenced.add( t )
            for t in ( row.get( 'thens' ) or [] ):
                if t in rows:
                    referenced.add( t )
            if cond := row.get( 'condition' ):
                if cond in rows:
                    referenced.add( cond )

        roots = sorted( [ op_id for op_id in rows if op_id not in referenced ] )

        def _print_node( op_id: str, indent: str ) -> None:
            if op_id not in rows:
                print( f'{ indent }[{ op_id }] (external ref)' )
                return
            row          = rows[ op_id ]
            type_        = row[ 'type' ]
            source       = row[ 'source' ] or []
            target       = row[ 'target' ] or []
            child_indent = indent + '  '

            print( f'{ indent }[{ op_id }] { type_ }' )

            if source:
                print( f'{ child_indent }source: { tuple( source ) }' )

            if type_ == 'TableProcessRef':
                terms = row.get( 'term_idns' ) or []
                print( f'{ child_indent }op: { terms[0] if terms else "?" }' )
            elif type_ == 'TableCheckRef':
                terms = row.get( 'term_idns' ) or []
                print( f'{ child_indent }check: { terms[0] if terms else "?" }' )
            elif type_ == 'TableProcessBranch':
                for if_op, then_op in zip( row.get( 'ifs' ) or [], row.get( 'thens' ) or [] ):
                    _print_node( if_op,   child_indent )
                    _print_node( then_op, child_indent )
                if ( terms := row.get( 'term_idns' ) ) and terms:
                    print( f'{ child_indent }otherwise:' )
                    _print_node( terms[ 0 ], child_indent + '  ' )
            elif type_ == 'TableProcessWhile':
                if cond := row.get( 'condition' ):
                    _print_node( cond, child_indent )
                for t in ( row.get( 'term_idns' ) or [] ):
                    _print_node( t, child_indent )
            else:
                for t in ( row.get( 'term_idns' ) or [] ):
                    _print_node( t, child_indent )

            if target:
                print( f'{ child_indent }target: { tuple( target ) }' )
        #/def _print_node

        for op_id in roots:
            _print_node( op_id, verbose_prefix )
            print()
    #/def print_tableProcesses
#/class Cell


def cell_from_toml(
    path: str | Path,
    ops_module,
    main_id: str,
    rng: np.random.Generator | None = None,
    verbose: int = 0,
    paths_dict: dict[ str, str ] | None = None,
) -> Cell:
    """
    Construct a Web from a TOML brain config.

    Parameters
    ----------
    path        : path to the .toml file
    ops_module  : module exposing ``get_ops(keys, rng) -> dict``; used to build
                  the tableOps the processes need
    main_id     : passed to ``Web.__init__``
    rng         : forwarded to ``ops_module.get_ops``
    verbose     : forwarded to ``Web.__init__``

    TOML schema expected
    --------------------
    [cell]
        class     = "..."       # informational only
        input_ids = [...]       # optional; defaults to all process op_idns

    [tables]
        keys = ["X", "X_latent", ...]

    [[processes]]               # one block per process
        op_idn  = "..."
        type    = "TableProcessRef" | "TableCheckRef" | "TableProcessSequence"
                | "TableProcessBranch" | "TableProcessWhile" | "TableProcessCount"
                | "Alias"
        source  = [...]
        target  = [...]         # omit for TableCheckRef
        terms   = [...]         # op keys for Ref/Check; process op_idns for others
        # TableProcessBranch extras:
        ifs     = [...]
        thens   = [...]
        otherwise = "..."       # optional
        # TableProcessWhile extras:
        condition = "..."
        maxIter = 2147483647    # optional
        # TableProcessCount extras:
        count   = N
        # Alias extras:
        aliases = "..."         # op_idn of the process being aliased
    """
    with open( path, 'rb' ) as f:
        data = tomllib.load( f )

    # -- Build process dataclasses from [[processes]]
    processes: list = []
    for entry in data.get( 'processes', [] ):
        t = entry[ 'type' ]
        if t == 'TableProcessRef':
            processes.append( tableProcesses.TableProcessRef(
                op_idn = entry[ 'op_idn' ],
                source = entry[ 'source' ],
                target = entry[ 'target' ],
                terms  = entry[ 'terms' ],
            ) )
        elif t == 'TableCheckRef':
            processes.append( tableProcesses.TableCheckRef(
                op_idn = entry[ 'op_idn' ],
                source = entry[ 'source' ],
                terms  = entry[ 'terms' ],
            ) )
        elif t == 'TableProcessSequence':
            processes.append( tableProcesses.TableProcessSequence(
                op_idn = entry[ 'op_idn' ],
                source = entry[ 'source' ],
                target = entry[ 'target' ],
                terms  = entry[ 'terms' ],
            ) )
        elif t == 'TableProcessBranch':
            processes.append( tableProcesses.TableProcessBranch(
                op_idn    = entry[ 'op_idn' ],
                source    = entry[ 'source' ],
                target    = entry[ 'target' ],
                ifs       = entry[ 'ifs' ],
                thens     = entry[ 'thens' ],
                otherwise = entry.get( 'otherwise' ),
            ) )
        elif t == 'TableProcessWhile':
            processes.append( tableProcesses.TableProcessWhile(
                op_idn    = entry[ 'op_idn' ],
                source    = entry[ 'source' ],
                target    = entry[ 'target' ],
                condition = entry[ 'condition' ],
                terms     = entry[ 'terms' ],
                maxIter   = entry.get( 'maxIter', 2**31 - 1 ),
            ) )
        elif t == 'TableProcessCount':
            processes.append( tableProcesses.TableProcessCount(
                op_idn = entry[ 'op_idn' ],
                source = entry[ 'source' ],
                target = entry[ 'target' ],
                count  = entry[ 'count' ],
                terms  = entry[ 'terms' ],
            ) )
        elif t == 'Alias':
            processes.append( tableProcesses.Alias(
                op_idn  = entry[ 'op_idn' ],
                aliases = entry[ 'aliases' ],
                source  = entry[ 'source' ],
                target  = entry[ 'target' ],
            ) )
        else:
            raise ValueError( f"Unknown process type in TOML: {t!r}" )

    # -- Collect op keys: terms of Ref/Check processes only
    op_keys: set[ str ] = set()
    for entry in data.get( 'processes', [] ):
        if entry[ 'type' ] in ( 'TableProcessRef', 'TableCheckRef' ):
            op_keys.update( entry.get( 'terms', [] ) )

    # -- Build ops from the provided module
    ops: types.TableProcessDict = (
        ops_module.get_ops( list( op_keys ), rng )
        if hasattr( ops_module, 'get_ops' )
        else {}
    )

    # -- Resolve input_ids (empty list → all process op_idns)
    brain_cfg  = data.get( 'cell', data.get( 'brain', {} ) )
    input_ids  = brain_cfg.get( 'input_ids' ) or [ p.op_idn for p in processes ]

    # -- Construct and populate the Web
    cell = Cell( main_id=main_id, input_ids=input_ids, ops=ops, verbose=verbose )

    for process in processes:
        cell.register_process( process )

    for key in data.get( 'tables', {} ).get( 'keys', [] ):
        if key not in cell.tables:
            cell.tables[ key ] = None

    # -- Populate __table_init__ from [[table_init]] entries if present
    table_init_entries = data.get( 'table_init', [] )
    if table_init_entries:
        refs = [
            TableInitRef(
                table_idn  = e[ 'table_idn' ],
                source     = tuple( e.get( 'source', [] ) ),
                op_idn     = e[ 'op_idn' ],
                written_by = tuple( e.get( 'written_by', [] ) ),
                always_run = e.get( 'always_run', False ),
            )
            for e in table_init_entries
        ]
        cell.tables[ '__table_init__' ] = pl.concat(
            [ r.as_polars() for r in refs ], how='vertical',
        )

    # -- Inject paths DataFrame when paths_dict is provided
    if paths_dict is not None and 'paths' in cell.tables:
        from .utilities import isfile_series, isdir_series
        from .schema import table_schemas as _table_schemas
        _paths_df = pl.DataFrame(
            { 'key': list( paths_dict.keys() ), 'fp': list( paths_dict.values() ) },
            schema = { 'key': pl.Utf8, 'fp': pl.Utf8 },
        ).with_columns(
            isfile_series( pl.Series( list( paths_dict.values() ) ), name='isfile' ),
            isdir_series(  pl.Series( list( paths_dict.values() ) ), name='isdir'  ),
        ).select( list( _table_schemas['paths'].keys() ) )
        cell.tables[ 'paths' ] = _paths_df

    return cell
#/def cell_from_toml
