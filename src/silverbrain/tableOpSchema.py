#
#//  tableOpSchema.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

from collections import UserDict
from dataclasses import dataclass, field
from typing import Literal, Self

import polars as pl

from .polarsDataTypeStrings import dtype_to_str, str_to_dtype
from .schema import table_schemas

Extra = Literal['forbidden', 'open', 'transparent']

@dataclass
class DiskWrite:
    path_input: int     # index into inputs — which df holds the path
    path_column: str    # column in that df containing the file path

    def to_polars( self: Self, op_id: str, effect_index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_id': [ op_id ],
                'effect_index': [ effect_index ],
                'type': [ 'DiskWrite' ],
                'path_input': [ self.path_input ],
                'path_column': [ self.path_column ],
                'output': [ None ],
                'format': [ None ],
            },
            schema = table_schemas['__table_op_effects__'],
        )
    #/def to_polars

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DiskWrite',
            'path_input': self.path_input,
            'path_column': self.path_column,
        }
    #/def to_dict

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            path_input = d['path_input'],
            path_column = d['path_column'],
        )
    #/def from_dict
#/class DiskWrite

@dataclass
class DiskRead:
    path_input: int     # index into inputs — which df holds the path
    path_column: str    # column in that df containing the file path
    output: int         # index into outputs — which output receives the loaded data
    format: str         # e.g. 'parquet', 'json'

    def to_polars(
        self: Self,
        op_id: str,
        effect_index: int,
        ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_id': [ op_id ],
                'effect_index': [ effect_index ],
                'type': [ 'DiskRead' ],
                'path_input': [ self.path_input ],
                'path_column': [ self.path_column ],
                'output': [ self.output ],
                'format': [ self.format ],
            },
            schema = table_schemas['__table_op_effects__'],
        )
    #/def to_polars

    def to_dict( self: Self ) -> dict:
        return {
            'type': 'DiskRead',
            'path_input': self.path_input, 'path_column': self.path_column,
            'output': self.output, 'format': self.format,
        }
    #/def to_dict

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            path_input = d['path_input'],
            path_column = d['path_column'],
            output = d['output'],
            format = d['format'],
        )
    #/def from_dict
#/class DiskRead

@dataclass
class Mkdir:
    path_input: int     # index into inputs — which df holds the paths
    path_column: str    # column in that df containing the directory path

    def to_polars(
        self: Self,
        op_id: str,
        effect_index: int,
        ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_id': [ op_id ],
                'effect_index': [ effect_index ],
                'type': [ 'Mkdir' ],
                'path_input': [ self.path_input ],
                'path_column': [ self.path_column ],
                'output': [ None ],
                'format': [ None ],
            },
            schema = table_schemas['__table_op_effects__'],
        )
    #/def to_polars

    def to_dict(
        self: Self
        ) -> dict:
        return {
            'type': 'Mkdir',
            'path_input': self.path_input,
            'path_column': self.path_column,
        }
    #/def to_dict

    @classmethod
    def from_dict(
        cls,
        d: dict,
        ) -> Self:
        return cls(
            path_input = d['path_input'],
            path_column = d['path_column'],
        )
    #/def from_dict
#/class Mkdir

Effect = DiskWrite | DiskRead | Mkdir

_EFFECT_CLASSES: dict[ str, type ] = {
    'DiskWrite': DiskWrite,
    'DiskRead':  DiskRead,
    'Mkdir':     Mkdir,
}

@dataclass
class InputSchema():
    columns: dict[ str, pl.DataType ] = field( default_factory = dict )
    extra: Extra = 'open'
    #
    # 'forbidden'   : no extra columns allowed
    # 'open'        : extra columns allowed, ignored
    # 'transparent' : extra columns allowed, flow to output (see OutputSchema.passthrough_from)
    #

    def to_polars( self: Self, op_id: str, index: int ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'op_id':            [ op_id ],
                'direction':        [ 'input' ],
                'index':            [ index ],
                'column_names':     [ list( self.columns.keys() ) ],
                'column_types':     [
                    [ dtype_to_str( t ) for t in self.columns.values() ]
                ],
                'extra':            [ self.extra ],
                'passthrough_from': [ None ],
            },
            schema = table_schemas['__table_op_schema__'],
        )
    #/def to_polars

    def to_dict( self: Self ) -> dict:
        return {
            'columns': {
                col: dtype_to_str( t )
                for col, t in self.columns.items()
            },
            'extra': self.extra,
        }
    #/def to_dict

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            columns = {
                col: str_to_dtype( s )
                for col, s in d['columns'].items()
            },
            extra = d['extra'],
        )
    #/def from_dict
#/class InputSchema

@dataclass
class OutputSchema():
    columns: dict[ str, pl.DataType ] = field( default_factory = dict )
    passthrough_from: list[ int ] = field( default_factory = list )
    #
    # passthrough_from: indices into TableOpSchema.inputs —
    #   extra columns from each listed input appear here
    # columns: columns explicitly constructed by this op (added on top of any passthrough)
    #

    def to_polars( self: Self, op_id: str, index: int ) -> pl.DataFrame:
        return pl.DataFrame({
            'op_id':            [ op_id ],
            'direction':        [ 'output' ],
            'index':            [ index ],
            'column_names':     [ list( self.columns.keys() ) ],
            'column_types':     [
                [ dtype_to_str( t ) for t in self.columns.values() ]
            ],
            'extra':            [ None ],
            'passthrough_from': [ self.passthrough_from ],
        }, schema = table_schemas['__table_op_schema__'] )
    #/def to_polars

    def to_dict( self: Self ) -> dict:
        return {
            'columns': {
                col: dtype_to_str( t )
                for col, t in self.columns.items()
            },
            'passthrough_from': self.passthrough_from,
        }
    #/def to_dict

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            columns = {
                col: str_to_dtype( s )
                for col, s in d['columns'].items()
            },
            passthrough_from = d.get( 'passthrough_from', [] ),
        )
    #/def from_dict
#/class OutputSchema

@dataclass
class TableOpSchema():
    inputs: list[ InputSchema ]
    outputs: list[ OutputSchema ]
    effects: list[ Effect ] = field( default_factory = list )

    def to_polars_schema( self: Self, op_id: str ) -> pl.DataFrame:
        frames = [
            inp.to_polars( op_id, i ) for i, inp in enumerate( self.inputs )
        ] + [
            out.to_polars( op_id, i ) for i, out in enumerate( self.outputs )
        ]
        return pl.concat( frames, how = 'vertical' )
    #/def to_polars_schema

    def to_polars_effects( self: Self, op_id: str ) -> pl.DataFrame:
        frames = [ eff.to_polars( op_id, i ) for i, eff in enumerate( self.effects ) ]
        return (
            pl.concat( frames, how = 'vertical' ) if frames
            else pl.DataFrame( schema = table_schemas['__table_op_effects__'] )
        )
    #/def to_polars_effects

    def to_polars( self: Self, op_id: str ) -> tuple[ pl.DataFrame, pl.DataFrame ]:
        return ( self.to_polars_schema( op_id ), self.to_polars_effects( op_id ) )
    #/def to_polars

    def to_dict( self: Self ) -> dict:
        return {
            'inputs':  [ inp.to_dict() for inp in self.inputs ],
            'outputs': [ out.to_dict() for out in self.outputs ],
            'effects': [ eff.to_dict() for eff in self.effects ],
        }
    #/def to_dict

    @classmethod
    def from_dict( cls, d: dict ) -> Self:
        return cls(
            inputs  = [
                InputSchema.from_dict( i )
                for i in d['inputs']
            ],
            outputs = [
                OutputSchema.from_dict( o )
                for o in d['outputs']
            ],
            effects = [
                _EFFECT_CLASSES[ e['type'] ].from_dict( e )
                for e in d.get( 'effects', [] )
            ],
        )
    #/def from_dict
#/class TableOpSchema

class TableOpSchemaDict( UserDict[ str, TableOpSchema ] ):
    """
    A typed dict[str, TableOpSchema] mapping opId strings to their schemas.
    """

    def to_dict(
        self: Self,
        ) -> dict[ str, dict ]:
        return {
            op_id: op.to_dict()
            for op_id, op in self.data.items()
        }
    #/def to_dict

    @classmethod
    def from_dict(
        cls,
        d: dict[ str, dict ],
        ) -> Self:
        return cls(
            {
                op_id: TableOpSchema.from_dict( v )
                for op_id, v in d.items()
            }
        )
    #/def from_dict

    def to_polars_schema( self: Self ) -> pl.DataFrame:
        """Return tableOp_schema DataFrame (one row per input/output across all ops)."""
        if not self.data:
            return pl.DataFrame( schema = table_schemas['__table_op_schema__'] )
        return pl.concat(
            [ op.to_polars_schema( op_id ) for op_id, op in self.data.items() ],
            how = 'vertical',
        )
    #/def to_polars_schema

    def to_polars_effects( self: Self ) -> pl.DataFrame:
        """Return tableOp_effects DataFrame (one row per effect across all ops)."""
        if not self.data:
            return pl.DataFrame( schema = table_schemas['__table_op_effects__'] )
        return pl.concat(
            [ op.to_polars_effects( op_id ) for op_id, op in self.data.items() ],
            how = 'vertical',
        )
    #/def to_polars_effects

    def to_polars_tuple(
        self: Self
        ) -> tuple[
            pl.DataFrame, # tableOp_schema
            pl.DataFrame, # tableOp_effects
        ]:
        """
        Serialize all ops into two DataFrames.

        Returns:
            - tableOp_schema: one row per input/output (schema table_schemas['__table_op_schema__'])
            - tableOp_effects: one row per effect (schema table_schemas['__table_op_effects__'])
        """
        return ( self.to_polars_schema(), self.to_polars_effects() )
    #/def to_polars_tuple
#/class TableOpSchemaDict


