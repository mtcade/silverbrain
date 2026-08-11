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
    join         = "all"      # optional: "all" (default) | "any" | "at_least_k"
                               # -- this node's firing rule; see schema.py's
                               # __nodes__ column docs / cell.py's
                               # _WiringEngine._scan
    join_k       = 2          # required iff join = "at_least_k" (1 <= join_k
                               # <= len(source)), else omitted

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
    # ... or exactly one external reference instead (never both). '_toml'/
    # '_json' (leading underscore) are reserved/"magic" keys, distinct from
    # an ordinary literal ctor kwarg that happens to be named 'toml'/'json':
    _toml = "path/to/params.toml"   # resolved relative to this file
    _json = "path/to/params.json"   # ditto

An external .toml/.json file's top-level content IS the ParamGroupDict --
the same par_name -> scalar-or-valueDict (+ optional _or/_and) shape a
hand-written pre-ParameterParser JSON spec already uses (see
parameterParse.py's module docstring), just without the outer
{par_group: ...} wrapping (parameter_group_to_tree() supplies
TABLE_OPS_PARAMETERS_GROUP for that; node_idn supplies the 'op_idn'
identity leaf). Inline keys in the TOML block itself (beyond
node_idn/_toml/_json) are read the same way.

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
from .bundle import CELLS_INDEX_FILE_DEFAULT, resolve_cell_fp
from .exprParse import expr_to_text, parse_expr
from .parameterParse import ParameterParser, parameter_group_to_tree
from .schema import table_schemas
from .utilities import apply_template_vars

import json
import polars as pl
import tomllib

from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4


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
    op_type:             str | None             = None,
    join:                str | None             = None,
    join_k:              int | None             = None,
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
            'op_type':              [ op_type ],
            'join':                 [ join ],
            'join_k':               [ join_k ],
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


def _auto_node_idn( entry: dict[ str, Any ] ) -> str:
    """
    A [[nodes]] entry's node_idn is now optional -- when omitted, this
    builds one, so the same op_type/class can be used by any number of
    anonymous nodes (wired purely by source/target) without the TOML author
    having to invent distinct names. Prefixed by whatever "type" identifier
    the entry does declare (op_type for kind='op'; cell_idn or the 'toml'
    path's stem for kind='cell'; 'kind' itself as a last resort) purely for
    legibility in error messages/introspection -- the uuid4 suffix is what
    actually guarantees uniqueness.
    """
    prefix = (
        entry.get( 'op_type' )
        or entry.get( 'cell_idn' )
        or ( Path( entry[ 'toml' ] ).stem if 'toml' in entry else None )
        or entry.get( 'kind', 'node' )
    )
    return f"{prefix}__{uuid4().hex[:8]}"
#/def _auto_node_idn


def _entry_join_fields( entry: dict[ str, Any ], entry_node_idn: str ) -> tuple[ str, int | None ]:
    """
    Extract and validate a [[nodes]] entry's join/join_k -- shared by the
    op/placeholder/cell branches below (see schema.py's __nodes__ column
    docs / _WiringEngine._scan for what these mean). Default ('all', None)
    reproduces today's only behavior (a plain AND-join) when neither is
    declared.
    """
    join = entry.get( 'join', 'all' )
    if join not in ( 'all', 'any', 'at_least_k' ):
        raise ValueError(
            f"[[nodes]] entry {entry_node_idn!r} has join={join!r} -- "
            f"must be 'all', 'any', or 'at_least_k'"
        )
    join_k = entry.get( 'join_k' )
    src_len = len( entry.get( 'source', [] ) )
    if join == 'at_least_k':
        if not isinstance( join_k, int ) or not ( 1 <= join_k <= src_len ):
            raise ValueError(
                f"[[nodes]] entry {entry_node_idn!r} has join='at_least_k' but "
                f"join_k={join_k!r} is not an int with 1 <= join_k <= "
                f"len(source)={src_len}"
            )
    elif join_k is not None:
        raise ValueError(
            f"[[nodes]] entry {entry_node_idn!r} declares join_k={join_k!r} "
            f"but join={join!r} -- join_k is only meaningful when join='at_least_k'"
        )
    return join, join_k
#/def _entry_join_fields


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


def _apply_template_vars_to_value( value: Any, tvars: dict[ str, str ] ) -> Any:
    """
    Recursively resolve '{key}' placeholders (apply_template_vars) through
    a ParamGroupDict's inline literal values -- str values are substituted
    directly; dict/list values (valueDict specs, _or/_and alternatives) are
    walked so a placeholder can appear at any depth; anything else (int,
    float, bool, None) passes through unchanged, same as apply_template_vars
    already does for a str with no matching key.
    """
    if isinstance( value, str ):
        return apply_template_vars( value, tvars )
    if isinstance( value, dict ):
        return { k: _apply_template_vars_to_value( v, tvars ) for k, v in value.items() }
    if isinstance( value, list ):
        return [ _apply_template_vars_to_value( v, tvars ) for v in value ]
    return value
#/def _apply_template_vars_to_value


def _parameters_entry_group(
    entry:     dict[ str, Any ],
    base_path: Path,
    tvars:     dict[ str, str ],
    ) -> tuple[ str, dict[ str, Any ] ]:
    """
    Resolve one [[parameters]] TOML entry to (node_idn, ParamGroupDict) --
    either the entry's own inline keys (everything but node_idn/_toml/_json),
    or the dict loaded from exactly one external '_toml'/'_json' reference.
    Mixing inline keys with an external reference, or giving both _toml and
    _json, is a usage error (ambiguous which parameters actually apply). The
    leading underscore marks these two keys as reserved/"magic" -- an
    ordinary literal ctor kwarg is free to be named 'toml' or 'json' without
    colliding (e.g. a TableOp that itself takes a path to a JSON data file).

    Inline keys get tvars substituted into their literal values (e.g.
    bundle_root = "{bundle_root}"), same {key} substitution kind='cell'
    [[nodes]] entries already get on their 'toml' reference -- values
    loaded from an external _toml/_json reference are used as-is (that file
    has no access to this file's tvars).
    """
    entry      = dict( entry )
    node_idn   = entry.pop( 'node_idn' )
    toml_ref   = entry.pop( '_toml', None )
    json_ref   = entry.pop( '_json', None )

    if toml_ref is not None and json_ref is not None:
        raise ValueError(
            f"[[parameters]] entry for node_idn={ node_idn!r } declares both "
            f"'_toml' and '_json' -- only one external reference is allowed"
        )
    if ( toml_ref is not None or json_ref is not None ) and entry:
        raise ValueError(
            f"[[parameters]] entry for node_idn={ node_idn!r } mixes an external "
            f"reference ('_toml'/'_json') with inline keys { sorted( entry )!r } -- "
            f"use one or the other"
        )

    ref = toml_ref if toml_ref is not None else json_ref
    if ref is not None:
        group = _load_param_group( ( base_path / apply_template_vars( ref, tvars ) ).resolve() )
    else:
        group = { k: _apply_template_vars_to_value( v, tvars ) for k, v in entry.items() }

    return node_idn, group
#/def _parameters_entry_group


def config_tree(
    path:                str | Path,
    node_idn:            str,
    parent_idn:          str | None      = None,
    source:              Sequence[ str ] = (),
    target:              Sequence[ str ] = (),
    template_vars:       dict[ str, str ] | None = None,
    node_idx_start:      int = 0,
    app_name:            str | None = None,
    fallback_data_roots: dict[ str, Path ] | None = None,
    cells_index_file:     Path = CELLS_INDEX_FILE_DEFAULT,
    join:                str  = 'all',
    join_k:              int | None = None,
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

    app_name/fallback_data_roots/cells_index_file: only needed if some
    kind='cell' [[nodes]] entry, anywhere in the transitive walk, uses
    'cell_idn' instead of 'toml' -- resolved via bundle.resolve_cell_fp()'s
    maps_index.toml search. None/omitted (the default) preserves today's
    exact behavior for every caller that only ever uses 'toml'.

    join/join_k: this file's own wiring-edge firing rule, as assigned by
    whoever referenced it (a parent's kind='cell' [[nodes]] entry, or the
    root caller for the top-level file, where they're unused) -- see
    schema.py's __nodes__ column docs. Default ('all', None) reproduces
    today's only behavior (a plain AND-join).
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
        path                = path,
        node_idn            = node_idn,
        parent_idn          = parent_idn,
        source              = source,
        target              = target,
        template_vars       = template_vars,
        node_idx_start      = node_idx_start,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file     = cells_index_file,
        join                = join,
        join_k              = join_k,
    )
#/def config_tree


def toml_config_tree(
    path:                str | Path,
    node_idn:            str,
    parent_idn:          str | None      = None,
    source:              Sequence[ str ] = (),
    target:              Sequence[ str ] = (),
    template_vars:       dict[ str, str ] | None = None,
    node_idx_start:      int = 0,
    app_name:            str | None = None,
    fallback_data_roots: dict[ str, Path ] | None = None,
    cells_index_file:     Path = CELLS_INDEX_FILE_DEFAULT,
    join:                str  = 'all',
    join_k:              int | None = None,
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
        data                = data,
        path                = path,
        node_idn            = node_idn,
        parent_idn          = parent_idn,
        source              = source,
        target              = target,
        template_vars       = template_vars,
        node_idx_start      = node_idx_start,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file     = cells_index_file,
        join                = join,
        join_k              = join_k,
    )
#/def toml_config_tree


def json_config_tree(
    path:                str | Path,
    node_idn:            str,
    parent_idn:          str | None      = None,
    source:              Sequence[ str ] = (),
    target:              Sequence[ str ] = (),
    template_vars:       dict[ str, str ] | None = None,
    node_idx_start:      int = 0,
    app_name:            str | None = None,
    fallback_data_roots: dict[ str, Path ] | None = None,
    cells_index_file:     Path = CELLS_INDEX_FILE_DEFAULT,
    join:                str  = 'all',
    join_k:              int | None = None,
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
        data                = data,
        path                = path,
        node_idn            = node_idn,
        parent_idn          = parent_idn,
        source              = source,
        target              = target,
        template_vars       = template_vars,
        node_idx_start      = node_idx_start,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file     = cells_index_file,
        join                = join,
        join_k              = join_k,
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
    app_name:            str | None = None,
    fallback_data_roots: dict[ str, Path ] | None = None,
    cells_index_file:     Path = CELLS_INDEX_FILE_DEFAULT,
    join:                str  = 'all',
    join_k:              int | None = None,
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
        node_idn     = node_idn,
        parent_idn   = parent_idn,
        kind         = 'cell',
        source       = source,
        target       = target,
        path         = str( path ),
        cell_source  = cell_header.get( 'source' ),
        cell_target  = cell_header.get( 'target' ),
        join         = join,
        join_k       = join_k,
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
    # node_idns that claimed their __table_ops_parameters__ row inline (via
    # a 'parameters' key on their own [[nodes]] entry, below) rather than
    # through a separate top-level [[parameters]] entry -- checked against
    # the [[parameters]] pass further down so the same op_idn can never be
    # given two competing parameter groups (op_ctor_kwargs_forest keys its
    # result dict by op_idn, so a silent second write would just clobber
    # the first with no error).
    inline_param_op_idns: set[ str ] = set()
    # node_idns already claimed by this file's own [[nodes]] entries --
    # checked below so a real (explicit) duplicate is a hard error rather
    # than the silent op-instance-clobbering/cross-contaminated dispatch
    # state a repeated node_idn used to cause. An omitted node_idn is
    # auto-generated (_auto_node_idn) and therefore always unique, so it
    # never collides here.
    seen_node_idns: set[ str ] = set()

    for entry in data.get( 'nodes', [] ):
        entry_node_idn = entry.get( 'node_idn' ) or _auto_node_idn( entry )
        if entry_node_idn in seen_node_idns:
            raise ValueError(
                f"duplicate node_idn={entry_node_idn!r} among this file's own "
                f"[[nodes]] entries -- node_idn must be unique per file (omit "
                f"it entirely for an anonymous, auto-generated id if multiple "
                f"nodes share the same op_type/kind)"
            )
        seen_node_idns.add( entry_node_idn )
        kind = entry[ 'kind' ]

        if kind == 'op':
            join, join_k = _entry_join_fields( entry, entry_node_idn )
            node_rows.append( _config_tree_row(
                node_idn     = entry_node_idn,
                parent_idn   = node_idn,
                kind         = 'op',
                source       = entry.get( 'source', [] ),
                target       = entry.get( 'target', [] ),
                path         = None,
                op_type      = entry.get( 'op_type', entry_node_idn ),
                join         = join,
                join_k       = join_k,
            ) )
            if 'parameters' in entry:
                # Same (inline keys) | (exactly one external '_toml'/'_json'
                # ref) grammar _parameters_entry_group already enforces for
                # top-level [[parameters]] entries -- reused verbatim by
                # wrapping this node's inline table in the same shape
                # ({node_idn, **rest}) that function expects, so both
                # spellings parse into the identical __table_ops_parameters__
                # row (same op_idn, same tree), not a second representation.
                p_node_idn, group = _parameters_entry_group(
                    { 'node_idn': entry_node_idn, **entry[ 'parameters' ] },
                    path.parent, tvars,
                )
                p_tree, node_idx = parameter_group_to_tree( p_node_idn, group, node_idx_start = node_idx )
                param_rows.append( p_tree )
                inline_param_op_idns.add( entry_node_idn )
        elif kind == 'placeholder':
            if 'parameters' in entry:
                raise ValueError(
                    f"[[nodes]] entry {entry_node_idn!r} declares 'parameters' but "
                    f"kind='placeholder' -- placeholders are built directly as "
                    f"PlaceholderOp(columns=...), never through the "
                    f"__table_ops_parameters__ forest, so inline parameters here "
                    f"would silently never be read"
                )
            join, join_k = _entry_join_fields( entry, entry_node_idn )
            node_rows.append( _config_tree_row(
                node_idn            = entry_node_idn,
                parent_idn          = node_idn,
                kind                = 'placeholder',
                source              = entry.get( 'source', [] ),
                target              = entry.get( 'target', [] ),
                path                = None,
                placeholder_columns = expr_to_text( _placeholder_expr( entry.get( 'columns', {} ) ) ),
                join                = join,
                join_k              = join_k,
            ) )
        elif kind == 'cell':
            if 'parameters' in entry:
                raise ValueError(
                    f"[[nodes]] entry {entry_node_idn!r} declares 'parameters' but "
                    f"kind='cell' -- nested Cells are built recursively via "
                    f"config_tree(), never through the __table_ops_parameters__ "
                    f"forest, so inline parameters here would silently never be read"
                )
            cell_entries.append( { **entry, 'node_idn': entry_node_idn } )
        else:
            raise ValueError(
                f"[[nodes]] entry {entry_node_idn!r} has unknown kind {kind!r} -- "
                f"expected 'op', 'cell', or 'placeholder'"
            )

    for entry in data.get( 'parameters', [] ):
        p_node_idn, group = _parameters_entry_group( entry, path.parent, tvars )
        if p_node_idn in inline_param_op_idns:
            raise ValueError(
                f"node_idn={ p_node_idn!r } has both an inline 'parameters' table "
                f"on its own [[nodes]] entry and a separate [[parameters]] entry "
                f"-- only one is allowed per op, since both would independently "
                f"try to populate __table_ops_parameters__ for the same op_idn"
            )
        p_tree, node_idx = parameter_group_to_tree( p_node_idn, group, node_idx_start = node_idx )
        param_rows.append( p_tree )

    for entry in cell_entries:
        entry_node_idn = entry[ 'node_idn' ]
        has_toml     = 'toml' in entry
        has_cell_idn = 'cell_idn' in entry

        if has_toml and has_cell_idn:
            raise ValueError(
                f"[[nodes]] entry {entry_node_idn!r} (kind='cell') has both "
                f"'toml' and 'cell_idn' -- use one or the other"
            )
        if has_cell_idn:
            if app_name is None:
                raise ValueError(
                    f"[[nodes]] entry {entry_node_idn!r} (kind='cell') uses "
                    f"cell_idn={entry['cell_idn']!r} but no app_name was "
                    f"supplied to resolve it via maps_index.toml"
                )
            child_toml_path = resolve_cell_fp(
                entry[ 'cell_idn' ], app_name, fallback_data_roots, cells_index_file,
            )
        elif has_toml:
            child_toml_path = ( path.parent / apply_template_vars( entry[ 'toml' ], tvars ) ).resolve()
        else:
            raise ValueError(
                f"[[nodes]] entry {entry_node_idn!r} (kind='cell') has neither "
                f"'toml' nor 'cell_idn' -- one is required"
            )

        join, join_k = _entry_join_fields( entry, entry_node_idn )
        child_nodes, child_params, node_idx = config_tree(
            path                = child_toml_path,
            node_idn            = entry_node_idn,
            parent_idn          = node_idn,
            source              = entry.get( 'source', () ),
            target              = entry.get( 'target', () ),
            template_vars       = tvars,
            node_idx_start      = node_idx,
            app_name            = app_name,
            fallback_data_roots = fallback_data_roots,
            cells_index_file     = cells_index_file,
            join                = join,
            join_k              = join_k,
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
