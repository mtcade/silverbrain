from . import schema

import polars as pl

from collections import UserDict
from typing import Self, Sequence
from os import path


def apply_template_vars( s: str, template_vars: dict[ str, str ] ) -> str:
    for k, v in template_vars.items():
        s = s.replace( f'{{{k}}}', v )
    return s
#/def apply_template_vars


class TableBuilder( UserDict ):
    """
        A dictionary of columns, used to build a table row by row in a cleaner method than manually initializing a dictionary of lists.

        Has very little additional usage, outside maintaining a `shape` property, and allowing quick conversion to a `polars.DataFrame`.

    """
    def __init__(
        self: Self,
        schema: dict[ str, pl.DataType ] | None = None,
        columns: Sequence[ str ] | None = None,
        ) -> None:
        """
            :param schema: If provided, gives a ready to use polars Schema. If omitted, columns must be provided.
            :param columns: Names for columns if you do not want to provide schema yet. If so, you must provide it when calling `.to_polars(...)`
        """
        if schema is None and columns is None:
            raise ValueError("One of schema or columns must be provided for initialization")
        #

        if schema is not None and columns is not None:
            if not list( columns ) == list( schema.keys ):
                raise ValueError(
                    "Mismatch between schema={}, columns={}".format(schema, columns)
                )
            #/if not list( columns ) == list( schema.keys )
        #/if schema is not None and columns is not None

        if schema is None:
            self.schema = None
            self.columns = tuple( columns )
            self.data = { col: [] for col in self.columns }
        elif columns is None:
            self.schema = dict( schema )
            self.columns = tuple( self.schema.keys() )
            self.data = { col: [] for col in self.columns }
        #

        self.shape = (0,len(self.columns))
    #/def __init__

    def append(
        self: Self,
        item: dict[ str, any ] | Sequence[ any ],
        allow_missing: bool = False,
        ) -> None:
        """
            Add an item to each column. If `isinstance( item, dict )`, do it by column. Otherwise, do it in order of keys.

            :param allow_missing: Adds 'none' if key not present in `item`; only works for appending a dict rather than a sequence
        """

        if allow_missing:
            if isinstance( item, dict ):
                for key in self.columns:
                    if key in item:
                        self.data[key].append( item[key] )
                    #
                    else:
                        self.data[key].append( None )
                    #
                #
            else:
                raise TypeError("Expected dict for `.append` with `allow_missing = True`; got type(item)={}".format(type(item)))
            #/switch type( item )
        #/if allow_missing
        else:
            if isinstance( item, dict ):
                for key in self.columns:
                    self.data[key].append( item[key] )
                #
            else:
                for j in range( self.shape[1] ):
                    self.data[ self.columns[j] ].append( item[j] )
                #
            #/switch type( item )
        #/if allow_none/else

        self.shape = ( self.shape[0]+1, self.shape[1],)

        return
    #/def append

    def add_column(
        self: Self,
        name: str,
        values: Sequence[ any ],
        dtype: pl.DataType | None = None,
        ) -> None:
        assert name not in self.columns
        assert len( values ) == self.shape[0]

        if self.schema is not None:
            assert dtype is not None
            self.schema[ name ] = dtype
        #

        self.columns += (name,)

        self.data[ name ] = list( values )

        self.shape = ( self.shape[0], self.shape[1] + 1, )

        return
    #/def add_column

    def to_polars(
        self: Self,
        schema: dict[ str, pl.DataType ] | None = None,
        columns: Sequence[ str ] | None = None,
        ) -> pl.DataFrame:
        """
            :param schema: Must be provided if `self` was initialized only with column names. Can also be used to cast to new types.
            :param columns: If present, a subset of `self.columns` to use.
        """
        if schema is None:
            schema = self.schema
        #

        if columns is None:
            return pl.DataFrame(
                self.data,
                schema = schema,
            )
        #
        else:
            return pl.DataFrame(
                {
                    col: self.data[col]\
                        for col in columns
                    #
                },
                schema = {
                    col: schema[col]\
                        for col in columns
                    #/
                }
            )
        #/if columns is None
        # EARLY RETURN/
    #/def to_polars
#/class TableBuilder

# -- Tables Data

def isdir_series(
    fp_series: Sequence[ str ],
    fp_start: str | None = None,
    name: str = 'isdir',
    ) -> pl.Series:
    """
        Get a series checking if the given series is a dir; normally used to add an 'isdir' column to another table
    """
    if fp_start is None:
        return pl.Series(
            name = name,
            values = tuple(
                path.isdir(
                    fp
                ) for fp in fp_series
            ),
            dtype = schema.cannonical_schema['isdir'],
        )
    else:
        return pl.Series(
            name = name,
            values = tuple(
                path.isdir(
                    path.join(
                        fp_start, fp
                    )
                ) for fp in fp_series
            ),
            dtype = schema.cannonical_schema['isdir'],
        )
    #/if fp_start is None/else
    # EARLY RETURN/
#/def isdir_series

def isfile_series(
    fp_series: Sequence[ str ],
    fp_start: str | None = None,
    name: str = 'isfile',
    ) -> pl.Series:
    """
        Get a series checking if the given series is a file; normally used to add an 'isfile' column to another table
    """
    if fp_start is None:
        return pl.Series(
            name = name,
            values = tuple(
                path.isfile(
                    fp
                ) for fp in fp_series
            ),
            dtype = schema.cannonical_schema['isfile'],
        )
    else:
        return pl.Series(
            name = name,
            values = tuple(
                path.isfile(
                    path.join(
                        fp_start, fp
                    )
                ) for fp in fp_series
            ),
            dtype = schema.cannonical_schema['isfile'],
        )
    #/if fp_start is None/else
    # EARLY RETURN/
#/def isfile_series
