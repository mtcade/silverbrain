#
#//  configTree.py
#//  silverbrain
#//

"""
Parses config files into a flat, initialization-ready node tree -- pure
parsing, no live Cell construction

The output is a (nodes, parameters, next_node_idx) triple:

- nodes ('config_tree' schema, schema.py): a node_idn/parent_idn pair per
  node, spanning however many files a config transitively references via
  kind='cell' [[nodes]] entries. This is the same node_idn/parent_idn
  convention Cell's own __nodes__ table uses (schema.py), just with real
  multi-level parent_idn chains, since a config's file-reference tree can
  be arbitrarily deep before any live Cell exists.

[[nodes]] -- one block per node this file directly owns, distinguished
by kind (any leaf TableOp/nested Cell/child can, in principle, be swapped
for another at the same node_idn post-construction -- see
Cell.tableOps[node_idn] -- kind here only affects how this node is
*parsed and built*, not whether it can later change):

    [[nodes]]
    node_idn = "..."          # must be unique among this file's own nodes
    kind     = "op"           # "op" | "cell" | "placeholder"
    source   = [...]          # this node's positional inputs, read from
                               # this Cell's own namespace
    target   = [...]          # this node's positional outputs, written
                               # under these names (need not be unique --
                               # see squishy-sleeping-snowflake.md)

    # kind="op" additionally requires nothing else -- node_idn must be a
    # key op_classes resolves (its ctor kwargs, if any, come from
    # [[parameters]] below).

    # kind="cell" additionally requires:
    #   toml = "path/to/child.toml"   # relative to this file

    # kind="placeholder" additionally requires:
    #   columns = { col_name = "DtypeStr", ... }   # e.g. { a = "Int64" }
    #     -- or, for a ColVar-capable interface (exprParse.py's grammar):
    #   columns = "Union(ColVar('C'), 'nu_col_0')"
    # A dict value of "?" or "" (and any bare-name atom in the string
    # form, e.g. 'nu_col_0' above) means "infer this column's dtype from
    # schema.cannonical_schema" -- see exprParse.py's module docstring.
    # Declares the node's interface (an arity-1 input/output pair) with no
    # implementation bound yet -- built into a tableOps.PlaceholderOp,
    # which raises if ever actually called. Swap
    # cell.tableOps[node_idn] for a real TableOp/Cell before this node is
    # reached on a live execution path.
- parameters (ParameterParser's tree schema, parameterParse.py): a forest
  of TableOp construction-parameter trees. Every parameterized op's rows
  share one fixed par_group, parameterParse.TABLE_OPS_PARAMETERS_GROUP
  ('table_ops_parameters') -- individual ops are told apart within it by
  node_idx, via a root-level leaf with par_name == 'op_idn' -- see
  parameterParse.py's "TableOp construction kwargs" section. Populated
  from each file's own [[parameters]] entries (below), either inline or
  via an external .toml/.json reference; every entry gets its own fresh
  ParameterParser (dtypes inferred from the literal values present -- see
  parameter_group_to_tree()), so entries' value-column sets can differ and
  are reconciled with a diagonal concat. node_idx values are kept globally
  unique across the whole forest one config_tree()/toml_config_tree() call
  produces (this is what makes the shared-par_group scheme safe): a
  running counter (node_idx_start in, next_node_idx out -- the same
  dual-return pattern ParameterParser.to_polars()/to_polars_flat() already
  use) is threaded through every [[parameters]] entry at this level and,
  recursively, every kind='cell' [[nodes]] call's own entries. A forest assembled
  some other way without sharing that counter (e.g. Cell.merge()
  concatenating two independently-built forests) can reintroduce colliding
  node_idx across different ops -- see the caveat in parameterParse.py.
- next_node_idx: the counter's value after this whole call (this file plus
  every transitively-loaded child) -- pass back in as a parent's own
  node_idx_start when composing calls manually; cell_from_file's single
  root call doesn't need to, since nothing consumes it past that one call.

[[parameters]] -- one block per parameterized [[nodes]] entry in *this*
file (kind='op' or kind='cell' -- a placeholder's interface is declared
inline on its own [[nodes]] entry instead, see below):

    [[parameters]]
    node_idn = "..."   # which node (in this file) these params belong to
    # then either inline key/value parameters directly:
    threshold = 0.5
    label     = "x"
    # ... or exactly one external reference instead (never both):
    toml = "path/to/params.toml"   # resolved relative to this file
    json = "path/to/params.json"   # ditto

An external .toml/.json file's top-level content IS the ParamGroupDict --
the same par_name -> scalar-or-valueDict (+ optional _or/_and) shape a
hand-written pre-ParameterParser JSON spec already uses (see
parameterParse.py's module docstring), just without the outer
{par_group: ...} wrapping (parameter_group_to_tree() supplies
TABLE_OPS_PARAMETERS_GROUP for that; node_idn supplies the 'op_idn'
identity leaf). Inline keys in the TOML block itself (beyond
node_idn/toml/json) are read the same way.

config_tree() is the format dispatcher, inferring which parser to use from
the file suffix at every node in the walk (not just the root) -- so a
config can reference children in a different format than itself. '.toml'
(toml_config_tree) and '.json' (json_config_tree) are both implemented,
sharing all their parsing logic via _config_tree_from_data() once the file
is loaded to a plain dict -- anything else raises. External [[parameters]]
references are dispatched separately (by their own .toml/.json suffix, via
_load_param_group) since they carry a raw ParamGroupDict, not another node
in the topology.
"""

from . import bdd
from .exprParse import expr_to_text, parse_expr
from .parameterParse import ParameterParser, parameter_group_to_tree
from .schema import table_schemas
from .utilities import apply_template_vars

import json
import polars as pl
import tomllib

from pathlib import Path
from typing import Any, Sequence


def _config_tree_row(
    node_idn:            str,
    parent_idn:          str | None,
    kind:                str,
    source:              Sequence[ str ],
    target:              Sequence[ str ],
    path:                str | None,
    cell_source:         Sequence[ str ] | None = None,
    cell_target:         Sequence[ str ] | None = None,
    placeholder_columns: str | None             = None,
    ) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'node_idn':             [ node_idn ],
            'parent_idn':           [ parent_idn ],
            'kind':                 [ kind ],
            'source':               [ list( source ) ],
            'target':               [ list( target ) ],
            'path':                 [ path ],
            'cell_source':          [ list( cell_source ) if cell_source is not None else None ],
            'cell_target':          [ list( cell_target ) if cell_target is not None else None ],
            'placeholder_columns':  [ placeholder_columns ],
        },
        schema = table_schemas[ 'config_tree' ],
    )
#/def _config_tree_row


def _placeholder_expr( columns: Any ) -> bdd.Expr:
    """
    Normalize a kind='placeholder' [[nodes]] entry's `columns` field to one
    bdd.Expr -- either a TOML/JSON dict ({name = "DtypeStr", ...},
    backward compatible -- "?" or "" as a value now also means "infer",
    same sentinel spelling exprParse.py's text grammar uses) or a plain
    string, parsed via exprParse.parse_expr (the new ColVar-capable form).
    The dict form is normalized by transcribing it to the same text
    grammar and parsing that, rather than duplicating the dtype-inference
    rule here.
    """
    if isinstance( columns, str ):
        return parse_expr( columns )
    if isinstance( columns, dict ):
        atoms_text = ', '.join(
            "Atom('{}', {})".format( name, token if token not in ( '', '?' ) else '?' )
            for name, token in columns.items()
        )
        return parse_expr( f'Union({atoms_text})' )
    raise TypeError(
        f"placeholder 'columns' must be a dict or a string (exprParse grammar), got {type(columns)!r}"
    )
#/def _placeholder_expr


def _empty_parameters() -> pl.DataFrame:
    """A zero-row parameters frame, ParameterParser's own tree schema (no
    dtypes registered -> just the identifier columns, no par_val_* yet)."""
    return pl.DataFrame( schema = ParameterParser().schema )
#/def _empty_parameters


def _load_param_group( path: Path ) -> dict[ str, Any ]:
    """Load an external [[parameters]] reference's raw ParamGroupDict,
    dispatching on suffix -- '.toml' and '.json' both just parse to a plain
    dict, handled identically from here on."""
    suffix = path.suffix.lower()
    if suffix == ".json":
        with open( path, 'r' ) as f:
            return json.load( f )
        #
    #/if suffix == ".json"
    if suffix == ".toml":
        with open( path, 'rb' ) as f:
            return tomllib.load( f )
        #
    #/if suffix == ".toml"
    raise ValueError(
        f"Unsupported external parameters file format {suffix!r} for { path!r } -- "
        f"only '.toml'/'.json' are supported"
    )
#/def _load_param_group


def _parameters_entry_group(
    entry:     dict[ str, Any ],
    base_path: Path,
    tvars:     dict[ str, str ],
    ) -> tuple[ str, dict[ str, Any ] ]:
    """
    Resolve one [[parameters]] TOML entry to (node_idn, ParamGroupDict) --
    either the entry's own inline keys (everything but node_idn/toml/json),
    or the dict loaded from exactly one external 'toml'/'json' reference.
    Mixing inline keys with an external reference, or giving both toml and
    json, is a usage error (ambiguous which parameters actually apply).
    """
    entry      = dict( entry )
    node_idn   = entry.pop( 'node_idn' )
    toml_ref   = entry.pop( 'toml', None )
    json_ref   = entry.pop( 'json', None )

    if toml_ref is not None and json_ref is not None:
        raise ValueError(
            f"[[parameters]] entry for node_idn={ node_idn!r } declares both "
            f"'toml' and 'json' -- only one external reference is allowed"
        )
    if ( toml_ref is not None or json_ref is not None ) and entry:
        raise ValueError(
            f"[[parameters]] entry for node_idn={ node_idn!r } mixes an external "
            f"reference ('toml'/'json') with inline keys { sorted( entry )!r } -- "
            f"use one or the other"
        )

    ref = toml_ref if toml_ref is not None else json_ref
    if ref is not None:
        group = _load_param_group( ( base_path / apply_template_vars( ref, tvars ) ).resolve() )
    else:
        group = entry

    return node_idn, group
#/def _parameters_entry_group


def config_tree(
    path:           str | Path,
    node_idn:       str,
    parent_idn:     str | None      = None,
    source:         Sequence[ str ] = (),
    target:         Sequence[ str ] = (),
    template_vars:  dict[ str, str ] | None = None,
    node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int ]:
    """
    Parse a config file and its transitive child-file references into
    (nodes, parameters, next_node_idx) dataframes, dispatching to a
    format-specific parser by path's suffix. '.toml' and '.json' are both
    implemented (toml_config_tree/json_config_tree); anything else raises.

    node_idx_start/next_node_idx: the same dual-return counter-threading
    pattern ParameterParser.to_polars()/to_polars_flat() already use --
    every [[parameters]] entry parsed anywhere in this file or its
    transitive kind='cell' [[nodes]] entries draws its own parameter tree's node_idx values
    from one running counter, so the whole forest this call produces has
    globally-unique node_idx (not just unique per entry).
    """
    suffix = Path( path ).suffix.lower()
    if suffix == '.toml':
        parser = toml_config_tree
    elif suffix == '.json':
        parser = json_config_tree
    else:
        raise ValueError(
            f"Unsupported config file format {suffix!r} for { path!r } -- "
            f"only '.toml'/'.json' are implemented"
        )
    return parser(
        path           = path,
        node_idn       = node_idn,
        parent_idn     = parent_idn,
        source         = source,
        target         = target,
        template_vars  = template_vars,
        node_idx_start = node_idx_start,
    )
#/def config_tree


def toml_config_tree(
    path:           str | Path,
    node_idn:       str,
    parent_idn:     str | None      = None,
    source:         Sequence[ str ] = (),
    target:         Sequence[ str ] = (),
    template_vars:  dict[ str, str ] | None = None,
    node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int ]:
    """
    Parse a cell TOML file and its transitive kind='cell' [[nodes]] file references
    into (nodes, parameters, next_node_idx) dataframes. See
    _config_tree_from_data() for the format-agnostic parsing logic shared
    with json_config_tree().
    """
    path = Path( path ).resolve()
    with open( path, 'rb' ) as f:
        data = tomllib.load( f )
    return _config_tree_from_data(
        data           = data,
        path           = path,
        node_idn       = node_idn,
        parent_idn     = parent_idn,
        source         = source,
        target         = target,
        template_vars  = template_vars,
        node_idx_start = node_idx_start,
    )
#/def toml_config_tree


def json_config_tree(
    path:           str | Path,
    node_idn:       str,
    parent_idn:     str | None      = None,
    source:         Sequence[ str ] = (),
    target:         Sequence[ str ] = (),
    template_vars:  dict[ str, str ] | None = None,
    node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int ]:
    """
    Parse a cell JSON file and its transitive kind='cell' [[nodes]] file
    references into (nodes, parameters, next_node_idx) dataframes -- the
    JSON equivalent of toml_config_tree(): same {'cell': {...}, 'nodes':
    [...], 'parameters': [...]} shape TOML's [cell]/[[nodes]]/[[parameters]]
    tables parse to, just spelled as JSON. See _config_tree_from_data() for
    the shared parsing logic.
    """
    path = Path( path ).resolve()
    with open( path, 'r' ) as f:
        data = json.load( f )
    return _config_tree_from_data(
        data           = data,
        path           = path,
        node_idn       = node_idn,
        parent_idn     = parent_idn,
        source         = source,
        target         = target,
        template_vars  = template_vars,
        node_idx_start = node_idx_start,
    )
#/def json_config_tree


def _config_tree_from_data(
    data:           dict[ str, Any ],
    path:           Path,
    node_idn:       str,
    parent_idn:     str | None,
    source:         Sequence[ str ],
    target:         Sequence[ str ],
    template_vars:  dict[ str, str ] | None,
    node_idx_start: int,
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int ]:
    """
    Format-agnostic body shared by toml_config_tree()/json_config_tree():
    given an already-parsed {'cell': {...}, 'nodes': [...], 'parameters':
    [...]} dict plus the resolved path it came from, build this file's
    (nodes, parameters, next_node_idx).

    node_idn/parent_idn/source/target describe how *this* file was
    referenced by its parent (root call: parent_idn=None, source/target
    empty -- the root has no parent declaring them) -- these are the
    node-level, parent-assigned names, distinct from this file's own
    [cell].source/[cell].target (this cell's external binding, read here
    and carried as cell_source/cell_target). Children are recursed into via
    config_tree(), not this helper directly, so format is inferred fresh at
    each level.

    node_idx_start/next_node_idx thread one running counter through every
    [[parameters]] entry parsed at this level and, recursively, every
    child's -- see config_tree()'s docstring.
    """
    tvars = template_vars or {}

    cell_header = data.get( 'cell', {} )
    node_rows: list[ pl.DataFrame ] = [ _config_tree_row(
        node_idn    = node_idn,
        parent_idn  = parent_idn,
        kind        = 'cell',
        source      = source,
        target      = target,
        path        = str( path ),
        cell_source = cell_header.get( 'source' ),
        cell_target = cell_header.get( 'target' ),
    ) ]
    param_rows: list[ pl.DataFrame ] = [ _empty_parameters() ]
    node_idx = node_idx_start

    # Two passes over [[nodes]], not one: op/placeholder rows are appended
    # immediately, but kind='cell' entries are only *collected* here and
    # recursed into afterward, once this file's own [[parameters]] have
    # claimed their node_idx range -- preserves the pre-unification
    # ordering (this file's own params before any child's), so node_idx
    # still grows depth-first in file-declaration order rather than a
    # child's params jumping ahead of this file's own.
    cell_entries: list[ dict ] = []

    for entry in data.get( 'nodes', [] ):
        entry_node_idn = entry[ 'node_idn' ]
        kind           = entry[ 'kind' ]

        if kind == 'op':
            node_rows.append( _config_tree_row(
                node_idn   = entry_node_idn,
                parent_idn = node_idn,
                kind       = 'op',
                source     = entry.get( 'source', [] ),
                target     = entry.get( 'target', [] ),
                path       = None,
            ) )
        elif kind == 'placeholder':
            node_rows.append( _config_tree_row(
                node_idn            = entry_node_idn,
                parent_idn          = node_idn,
                kind                = 'placeholder',
                source              = entry.get( 'source', [] ),
                target              = entry.get( 'target', [] ),
                path                = None,
                placeholder_columns = expr_to_text( _placeholder_expr( entry.get( 'columns', {} ) ) ),
            ) )
        elif kind == 'cell':
            cell_entries.append( entry )
        else:
            raise ValueError(
                f"[[nodes]] entry {entry_node_idn!r} has unknown kind {kind!r} -- "
                f"expected 'op', 'cell', or 'placeholder'"
            )

    for entry in data.get( 'parameters', [] ):
        p_node_idn, group = _parameters_entry_group( entry, path.parent, tvars )
        p_tree, node_idx = parameter_group_to_tree( p_node_idn, group, node_idx_start = node_idx )
        param_rows.append( p_tree )

    for entry in cell_entries:
        child_toml_path = ( path.parent / apply_template_vars( entry[ 'toml' ], tvars ) ).resolve()
        child_nodes, child_params, node_idx = config_tree(
            path           = child_toml_path,
            node_idn       = entry[ 'node_idn' ],
            parent_idn     = node_idn,
            source         = entry.get( 'source', () ),
            target         = entry.get( 'target', () ),
            template_vars  = tvars,
            node_idx_start = node_idx,
        )
        node_rows.append( child_nodes )
        param_rows.append( child_params )

    return (
        pl.concat( node_rows,  how = 'vertical' ),
        # diagonal_relaxed, not vertical: each [[parameters]] entry (here or
        # in any recursively-loaded child) registers its own dtypes
        # independently (parameter_group_to_tree), so fragments' value-column
        # sets can differ -- reconcile by filling missing columns with null
        # rather than requiring identical schemas.
        pl.concat( param_rows, how = 'diagonal_relaxed' ),
        node_idx,
    )
#/def _config_tree_from_data
