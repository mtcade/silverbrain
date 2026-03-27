from . import schema

import polars as pl
import numpy as np

from typing import Literal, Sequence
from os import path

# -- Tables Data

def get_uuid7_bytes() -> bytes:
    from uuid_extensions import uuid7
    return uuid7( as_type = 'bytes' )
#/def get_uuid_int

def add_col_uuid7(
    df: pl.DataFrame,
    col: str,
    overwrite: bool = False
    ) -> pl.DataFrame:
    """
        :param pl.DataFrame df: Dataframe for new column
        :param str col: Column name for the new column
        :param bool overwrite: Whether to overwrite `col` if already present in `df.columns`
    """
    if (not overwrite) and\
        col in df.columns:
            raise ValueError(
                "Already have column in df col={}".format( col )
            )
        #/if { column conflict }
    #/
    
    return df.with_columns(
        pl.Series(
            get_uuid7_bytes() for _ in range( df.shape[0] )
        ).alias( col ).cast( pl.Binary )
    )
#/def add_col_uuid7

def add_random_row_indices(
    df: pl.DataFrame,
    col: str,
    rng: np.random.Generator
    ) -> pl.DataFrame:
    return df.with_columns(
        pl.Series(
            rng.permutation(
                df.shape[0]
            )
        ).alias( col )
    )
#/def add_random_row_indices

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

# -- Paths

def paths_df_from_dict(
    paths_dict: dict[ str, str ],
    ) -> pl.DataFrame:
    """
        :param paths_dict: key -> fp
        
        :returns:
            - key: str
            - fp: str
            - isfile: bool
    """
    paths_df: pl.DataFrame = pl.DataFrame(
        {
            'key': tuple( paths_dict.keys() ),
            'fp': tuple( paths_dict.values() ),
        },
        schema = schema.cannonical_schema_for_keys((
            'key','fp',
        )),
    )
    
    return paths_df.with_columns(
        isfile_series(
            paths_df['fp'],
            name = 'isfile',
        ),
        isdir_series(
            paths_df['fp'],
            name = 'isdir',
        ),
    )
#/def paths_df_from_dict

def resolve_paths(
    paths_dict: dict[ str, str ],
    start: str,
    main_root: str | None = None,
    ) -> dict[ str, str ]:
    """
        Turns a paths dict into a series of absolute paths. First, check for a '__main_root__' which must be absolute or start with a '.', as in '.' or '..'. Then, other paths are resolved as:
        
        - '/*': Absolute path
        - '.*': Relative to start
        - otherwise: Relative to main_root
        
        :param paths_dict: Read from disk as a json
        :param start: The directory of the json
        :param main_root: The directory for where unqualified paths should start. If not provided, it needs to be in paths_dict, or there should be only dot/slash paths.
        
        :returns: New dictionary of absolute paths with same keys
    """
    if main_root is None:
        if '__main_root__' in paths_dict:
            main_root = paths_dict['__main_root__']
            if main_root.startswith('.'):
                main_root = path.abspath(
                    path.join(
                        start,
                        main_root,
                    )
                )
            #
            elif main_root.startswith('/'):
                # Already absolute
                ...
            #
            else:
                # Bad main root
                raise ValueError("Require a dot/slash mainroot, got main_root='{}'".format(main_root))
            #/switch main_root/startswith(...)
            return resolve_paths(
                paths_dict = paths_dict,
                start = start,
                main_root = main_root,
            )
        #/if '__main_root__' in paths_dict
        else:
            for key, fp in paths_dict.items():
                if fp.startswith('/'):
                    ...
                #
                elif fp.startswith('.'):
                    paths_dict[key] = path.abspath(
                        path.join(
                            start,
                            fp,
                        )
                    )
                #
                else:
                    raise ValueError(
                        "Require '__main_root__' for non dot/slash fp, found key={}, fp={}".format(
                            key, fp,
                        )
                    )
                #/switch fp.startswith
            #/for key, fp in paths_dict.items()
            return paths_dict
        #/if '__main_root__' in paths_dict/else
    #/if main_root is None
    else:
        return {
            key: (
                main_root if key == '__main_root__'
                else fp if fp.startswith('/')
                else path.abspath(
                    path.join(
                        start,
                        fp,
                    )
                ) if fp.startswith('.')
                else path.abspath(
                    path.join(
                        main_root,
                        fp,
                    )
                )
            ) for key, fp in paths_dict.items()
        }
    #/if main_root is None/else
    # EARLY RETURN/
#/def resolve_paths

def get_paths_for_main_id(
    main_id: str,
    paths_index_fp: str | None = None,
    main_root: str | None = None,
    ) -> dict[ str, str ]:
    """
        Resolve paths_index_fp, get default paths and paths for main id, resolve them to absolute, and return the result, by `default_paths | brain_paths`
        
        Get paths_index_fp by:
            1. Check paths_index_fp
            2. If not paths_index_fp, use
                `path.join( getcwd(), "settings", "paths_index.json", )`
            
        :param paths_index_fp: For paths_index.json, likely in settings. Must have both brainId and 'default' as keys.
        :param main_id: Key in paths_index
        
    """
    import json
    if paths_index_fp:
            ...
    #
    else:
        from os import getcwd
        paths_index_fp = path.join(
            getcwd(),
            "settings",
            "paths_index.json",
        )
    #/{ resolve inputs }

    paths_index_dir: str = path.dirname( paths_index_fp )
    paths_index: dict[ str, str ]
    with open( paths_index_fp ) as f:
        paths_index = json.load( f )
    #
    paths_index = resolve_paths(
        paths_index,
        start = paths_index_dir,
    )

    defaults_fp: str = paths_index['default']
    defaults_dir: str = path.dirname( defaults_fp )
    default_paths: dict[ str, str ]
    with open( defaults_fp ) as f:
        default_paths = json.load( f )
    #
    default_paths = resolve_paths(
        default_paths,
        start = defaults_dir,
    )
    
    # default_paths should have __main_root__
    # Now it is absolute, and when we union it with our brain specific paths, the __main_root__ will flow through
    
    main_paths_fp: str = paths_index[ main_id ]
    main_paths_dir: str = path.dirname( main_paths_fp )
    main_paths: dict[ str, str ]
    with open( main_paths_fp ) as f:
        main_paths = json.load( f )
    #
    main_paths = default_paths | main_paths
    
    # main_paths will now have a __main_root__, and perhaps some relative paths
    main_paths = resolve_paths(
        main_paths,
        start = main_paths_dir,
    )
    
    return main_paths
#/def get_paths_for_main_id
