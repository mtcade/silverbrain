#
#//  configTree.py
#//  silverbrain
#//

"""
Parses config files into a flat, initialization-ready node tree -- pure
parsing, no live Cell construction

The output is a (nodes, parameters, next_node_idx, declarations) 4-tuple
(see "variables.resources / variables.actors" below for declarations):

- nodes ('config_tree' schema, schema.py): a node_idn/parent_idn pair per
  node, spanning however many files a config transitively references via
  kind='cell' [[nodes]] entries. This is the same node_idn/parent_idn
  convention Cell's own __nodes__ table uses (schema.py), just with real
  multi-level parent_idn chains, since a config's file-reference tree can
  be arbitrarily deep before any live Cell exists. Every row also carries
  its own 'abs_path' -- the dotted chain of node_idns from the ROOT of this
  whole config_tree() call down to this node (e.g. "wrapper.search.
  parameter_pick_next") -- see "variables.resources / variables.actors"
  below for what this is used for.

[[nodes]] -- one block per node this file directly owns, distinguished
by kind (any leaf TableOp/nested Cell/child can, in principle, be swapped
for another at the same node_idn post-construction -- see
Cell.tableOps[node_idn] -- kind here only affects how this node is
*parsed and built*, not whether it can later change):

    [[nodes]]
    node_idn = "..."          # must be unique among this file's own nodes
    kind     = "op"           # "op" | "cell" | "variable"
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
    discard_on_null = true    # optional, default true; only consulted when
                               # join = "all" -- see schema.py's __nodes__
                               # column docs / cell.py's _WiringEngine._scan

    # kind="op" additionally requires nothing else -- node_idn must be a
    # key op_classes resolves (its ctor kwargs, if any, come from
    # [[parameters]] below).

    # kind="cell" additionally requires:
    #   toml = "path/to/child.toml"   # relative to this file

    # kind="variable" additionally requires:
    #   columns = { col_name = "DtypeStr", ... }   # e.g. { a = "Int64" }
    #     -- or, for a ColVar-capable interface (exprParse.py's grammar):
    #   columns = "Union(ColVar('C'), 'nu_col_0')"
    # A dict value of "?" or "" (and any bare-name atom in the string
    # form, e.g. 'nu_col_0' above) means "infer this column's dtype from
    # schema.cannonical_schema" -- see exprParse.py's module docstring.
    # Declares the node's interface (an arity-1 input/output pair) with no
    # implementation bound yet -- built into a tableOps.PlaceholderOp,
    # which raises if ever actually called, UNLESS a variables.actors
    # declaration (see below) binds a real implementation to this node's
    # abs_path.
- parameters (ParameterParser's tree schema, parameterParse.py): a forest
  of TableOp construction-parameter trees. Each parameterized op's own
  dict (its ctor kwargs, plus an injected 'op_idn' leaf) is handed to
  to_polars() directly -- no wrapper key, since to_polars() has no concept
  of a top-level "group" a dict lives under -- individual ops are told
  apart by node_idx, via a root-level leaf with par_name == 'op_idn' -- see
  parameterParse.py's "TableOp construction kwargs" section. Populated
  from each file's own [[parameters]] entries (below), either inline or
  via an external .toml/.json reference; every entry gets its own fresh
  ParameterParser (dtypes inferred from the literal values present -- see
  parameter_group_to_tree()), so entries' value-column sets can differ and
  are reconciled with a diagonal concat. node_idx values are kept globally
  unique across the whole forest one config_tree()/toml_config_tree() call
  produces (this is what makes ops distinguishable purely by node_idx
  safe): a running counter (node_idx_start in, next_node_idx out -- the same
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
file (kind='op' or kind='cell' -- a variable's interface is declared
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
the same par_name -> scalar-or-valueDict (+ at most one of _or/_and) shape
a hand-written pre-ParameterParser JSON spec already uses (see
parameterParse.py's module docstring); node_idn supplies the 'op_idn'
identity leaf parameter_group_to_tree() injects. Inline keys in the TOML
block itself (beyond node_idn/_toml/_json) are read the same way.

config_tree() is the format dispatcher, inferring which parser to use from
the file suffix at every node in the walk (not just the root) -- so a
config can reference children in a different format than itself. '.toml'
(toml_config_tree) and '.json' (json_config_tree) are both implemented,
sharing all their parsing logic via _config_tree_from_data() once the file
is loaded to a plain dict -- anything else raises. External [[parameters]]
references are dispatched separately (by their own .toml/.json suffix, via
_load_param_group) since they carry a raw ParamGroupDict, not another node
in the topology.

variables.resources / variables.actors -- path-addressed declarations, the
mechanism behind BOTH a cell's own resource manifest AND (a variable's)
bound implementation. Each `[[variables.<channel>]]` entry is declared by
SOME node in the tree (any file, not just a direct parent) and names a
`target` -- a dotted node path RELATIVE TO THE DECLARING NODE ITSELF (e.g.
"search.parameter_pick_next"; omit or use "." to target the declaring node
itself). Resolved to an absolute path by prefixing the declaring node's own
abs_path. A declarer can therefore only ever reach INTO ITS OWN SUBTREE --
never sideways to a sibling's internals, never upward -- but may reach
arbitrarily deep, not just its direct children (unlike the old one-hop
[resources]/[nodes.resources] this replaces).

    [[variables.resources]]
    target    = "search"        # optional, defaults to "." (self)
    table_idn = "..."
    kind      = "local"         # optional, defaults "local"
    path      = "..."           # or db_path/table_name/connection_string/
                                 # schema_name, whichever 'kind' needs

    [[variables.actors]]
    target  = "search.parameter_pick_next"
    op_type = "..."              # a key into op_classes, exactly like a
                                  # kind='op' [[nodes]] entry's own op_type
    [variables.actors.parameters]
    some_kwarg = 1                # ctor kwargs for op_type, if any

Every declaration also carries declarer_depth -- how many kind='cell' hops
this DECLARING node sits below the root of the whole config_tree() call
(root = 0). Resolution (resolve_resource_declarations/
resolve_actor_declarations, below) picks, per target abs_path (and, for
resources, per table_idn within that path -- a target's resource manifest
can hold more than one table), the declaration with the SMALLEST
declarer_depth -- i.e. the ancestor closest to the root always wins over
any declaration made further down the tree (including a node's own default
for itself, which has declarer_depth == that node's own depth, the largest
possible value on its chain). This is the deliberate opposite of a normal
override cascade: the outermost assembler is meant to be the final
compositional authority over anything it transitively instantiates, and an
intermediate cell's own declaration (even one written FOR one of its own
descendants, as a reusable default) is only ever a fallback. Two
declarations for the same target can never tie -- every declarer of a
given target is, by construction, an ancestor of that target (relative-
only addressing), so all declarers of one target form a single linear
chain with a total order by depth.

Not flattened across DIFFERENT targets the way [[parameters]] is: two
sibling kind='cell' entries instantiating the same template must be able
to resolve the same table_idn (or the same variable node_idn) to
different concrete bindings -- since abs_path is built from each
INSTANCE's own node_idn chain (locally unique per parent, enforced by
seen_node_idns below), not the template's node_idns, two sibling
instantiations' abs_paths never collide even when they're built from the
exact same child file.

resolve_resource_declarations()/resolve_actor_declarations() do the actual
per-target resolution, once, after a whole config_tree() call returns --
cell.py's _cell_from_tree then just looks each node up by its own abs_path
column, already fully resolved, no further merging needed at construction
time.
"""

from . import bdd
from .bundle import CELLS_INDEX_FILE_DEFAULT, resolve_cell_fp
from .exprParse import expr_to_text, parse_expr
from .parameterParse import ParameterParser, parameter_group_to_tree
from .ReadWriteOps import _table_ref_row
from .schema import cannonical_schema_for_keys, table_schemas
from .utilities import apply_template_vars

import json
import polars as pl
import tomllib

from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4


# -- variables.resources / variables.actors declaration-row schemas (see
# module docstring). Transient/local shapes -- not registered in
# schema.py's table_schemas, since nothing outside this module's own parse
# -> resolve pipeline ever sees a row in this exact shape (resolution
# strips 'abs_path'/'declarer_depth' back off before handing a per-target
# result to cell.py).

_RESOURCE_DECL_SCHEMA: dict[ str, pl.DataType ] = (
    table_schemas[ 'table_index' ]
    | cannonical_schema_for_keys( ( 'abs_path', 'declarer_depth' ) )
)
_ACTOR_DECL_SCHEMA: dict[ str, pl.DataType ] = cannonical_schema_for_keys( (
    'abs_path', 'declarer_depth', 'kind', 'op_type', 'path', 'kwargs_json', 'declaring_path',
) )


def _empty_resource_decls() -> pl.DataFrame:
    return pl.DataFrame( schema = _RESOURCE_DECL_SCHEMA )
#/def _empty_resource_decls


def _empty_actor_decls() -> pl.DataFrame:
    return pl.DataFrame( schema = _ACTOR_DECL_SCHEMA )
#/def _empty_actor_decls


def _empty_declarations() -> dict[ str, pl.DataFrame ]:
    return { 'resources': _empty_resource_decls(), 'actors': _empty_actor_decls() }
#/def _empty_declarations


def resolve_resource_declarations( decls: pl.DataFrame ) -> dict[ str, pl.DataFrame ]:
    """
    Resolve a flat variables.resources declarations frame (abs_path,
    declarer_depth, plus table_index columns) into {abs_path: resolved
    table_index DataFrame}. Root-wins, generalizing the old
    ReadWriteOps.merge_resources 'caller overrides on a clashing table_idn'
    idiom to arbitrary depth: sort by declarer_depth descending (deepest/
    most-local declarations first, root last), then
    unique(subset=[abs_path,table_idn], keep='last') -- the shallowest
    (closest to root) declaration for a given (target, table_idn) pair
    survives. A target with no declarations at all simply has no key in
    the result (equivalent to Cell.resources staying None, same as today).
    """
    if decls.is_empty():
        return {}
    deduped = (
        decls
        .sort( 'declarer_depth', descending = True, maintain_order = True )
        .unique( subset = [ 'abs_path', 'table_idn' ], keep = 'last', maintain_order = True )
    )
    resolved: dict[ str, pl.DataFrame ] = {}
    for abs_path in deduped[ 'abs_path' ].unique( maintain_order = True ):
        resolved[ abs_path ] = (
            deduped
            .filter( pl.col( 'abs_path' ) == abs_path )
            .drop( [ 'abs_path', 'declarer_depth' ] )
        )
    return resolved
#/def resolve_resource_declarations


def resolve_actor_declarations( decls: pl.DataFrame ) -> dict[ str, dict[ str, Any ] ]:
    """
    Resolve a flat variables.actors declarations frame the same way --
    root-wins, one winner per abs_path (a variable has exactly one bound
    implementation, unlike a resource manifest's per-table_idn rows).
    Result: {abs_path: {'kind': 'op'|'cell', 'declaring_path': ..., ...}} --
    'declaring_path' is provenance (which node's [[variables.actors]] entry
    actually won), carried through to __actor_bindings__ (schema.py) for
    inspection. kind='op' additionally carries 'op_type'/'kwargs' (an
    op_classes key + its ctor kwargs); kind='cell' additionally carries
    'toml' (the graft's already-resolved, absolute child file path -- see
    splice_actor_grafts(), which is what actually consumes a kind='cell'
    binding; _cell_from_tree only ever sees kind='op' bindings, since a
    graft is fully spliced into the tree itself before construction).
    """
    if decls.is_empty():
        return {}
    deduped = (
        decls
        .sort( 'declarer_depth', descending = True, maintain_order = True )
        .unique( subset = [ 'abs_path' ], keep = 'last', maintain_order = True )
    )
    resolved: dict[ str, dict[ str, Any ] ] = {}
    for row in deduped.to_dicts():
        binding: dict[ str, Any ] = {
            'kind':           row[ 'kind' ],
            'declaring_path': row[ 'declaring_path' ],
        }
        if row[ 'kind' ] == 'cell':
            binding[ 'toml' ] = row[ 'path' ]
        else:
            binding[ 'op_type' ] = row[ 'op_type' ]
            binding[ 'kwargs' ]  = json.loads( row[ 'kwargs_json' ] )
        resolved[ row[ 'abs_path' ] ] = binding
    return resolved
#/def resolve_actor_declarations


# Hard cap on splice_actor_grafts()'s resolve/splice rounds -- exists only
# to turn a pathological config (e.g. a graft file that, transitively,
# re-declares a graft binding for the very variable it was just spliced
# in to answer) into a clear error instead of an infinite loop. No real
# config should ever need more than a few rounds (one per level of graft
# nesting actually present).
_MAX_GRAFT_SPLICE_ROUNDS = 50


def splice_actor_grafts(
    tree:                pl.DataFrame,
    parameters:          pl.DataFrame,
    node_idx:            int,
    declarations:        dict[ str, pl.DataFrame ],
    template_vars:       dict[ str, str ] | None      = None,
    app_name:            str | None                   = None,
    fallback_data_roots: dict[ str, Path ] | None      = None,
    cells_index_file:     Path                        = CELLS_INDEX_FILE_DEFAULT,
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int, dict[ str, pl.DataFrame ], dict[ str, dict[ str, Any ] ] ]:
    """
    Resolve declarations and splice in every variables.actors binding whose
    kind='cell' (a graft -- see module docstring's variables.actors
    section), repeatedly, until a fixed point: a grafted subtree can itself
    contain further variables with their own variables.actors/
    variables.resources declarations (its own defaults, or a still-pending
    ancestor declaration reaching further in through it), so one resolve
    pass isn't always enough. Called once by cell_from_file(), after
    config_tree() has produced the whole (pre-graft) tree/parameters/
    declarations -- NOT threaded through _cell_from_tree, since by the
    time construction starts every graft is already a plain kind='cell'
    row in `tree`, indistinguishable from one hand-authored in TOML.

    Splicing a target abs_path: find its (currently kind='variable')
    row, parse the graft's toml (config_tree(), same recursive machinery
    an ordinary kind='cell' [[nodes]] entry uses) with that row's OWN
    node_idn/parent_idn/source/target/join/discard_on_null carried over
    unchanged (only WHICH implementation answers this node_idn is
    changing, not how it's wired -- same principle as cell.py's
    Cell._replace_actor) and ancestor_path set so the grafted subtree's
    own abs_path comes out identical to the variable's -- then replace
    the variable row with the graft's own root row (now kind='cell')
    plus every row underneath it, and fold the graft's own parameters/
    declarations into the running totals for the next round.

    Returns (tree, parameters, node_idx, resolved_resources,
    resolved_actors) -- fully resolved and fully spliced; resolved_actors
    still contains an entry for every spliced kind='cell' target too (not
    consumed by _cell_from_tree, which only ever looks up variable rows
    -- none remain at that abs_path after splicing -- but kept for
    cell.py's __actor_bindings__ provenance row).
    """
    tvars = template_vars or {}
    for _ in range( _MAX_GRAFT_SPLICE_ROUNDS ):
        resolved_actors = resolve_actor_declarations( declarations[ 'actors' ] )
        pending = {
            abs_path: binding for abs_path, binding in resolved_actors.items()
            if binding[ 'kind' ] == 'cell'
            and not tree.filter(
                ( pl.col( 'abs_path' ) == abs_path ) & ( pl.col( 'kind' ) == 'cell' )
            ).height
        }
        if not pending:
            break
        for abs_path, binding in pending.items():
            target_rows = tree.filter(
                ( pl.col( 'abs_path' ) == abs_path ) & ( pl.col( 'kind' ) == 'variable' )
            )
            if target_rows.is_empty():
                raise ValueError(
                    f"variables.actors graft binding targeting {abs_path!r} "
                    f"(declared by {binding[ 'declaring_path' ]!r}) does not "
                    f"resolve to any kind='variable' node"
                )
            target_row = target_rows.row( 0, named = True )
            ancestor_path = tuple( abs_path.split( '.' )[ :-1 ] )
            graft_nodes, graft_params, node_idx, graft_decls = config_tree(
                path                = binding[ 'toml' ],
                node_idn            = target_row[ 'node_idn' ],
                parent_idn          = target_row[ 'parent_idn' ],
                source              = target_row[ 'source' ] or (),
                target              = target_row[ 'target' ] or (),
                template_vars       = tvars,
                node_idx_start      = node_idx,
                app_name            = app_name,
                fallback_data_roots = fallback_data_roots,
                cells_index_file     = cells_index_file,
                join                = target_row[ 'join' ] or 'all',
                join_k              = target_row[ 'join_k' ],
                discard_on_null     = True if target_row[ 'discard_on_null' ] is None else target_row[ 'discard_on_null' ],
                ancestor_path       = ancestor_path,
            )
            tree = pl.concat(
                [
                    tree.filter( ~( ( pl.col( 'abs_path' ) == abs_path ) & ( pl.col( 'kind' ) == 'variable' ) ) ),
                    graft_nodes,
                ],
                how = 'vertical',
            )
            parameters = pl.concat( [ parameters, graft_params ], how = 'diagonal_relaxed' )
            declarations = {
                'resources': pl.concat( [ declarations[ 'resources' ], graft_decls[ 'resources' ] ], how = 'vertical' ),
                'actors':    pl.concat( [ declarations[ 'actors' ],    graft_decls[ 'actors' ]    ], how = 'vertical' ),
            }
    else:
        raise ValueError(
            f"variables.actors graft splicing did not converge after "
            f"{_MAX_GRAFT_SPLICE_ROUNDS} rounds -- check for a cyclic graft "
            f"reference (a grafted cell whose own variables.actors "
            f"re-targets the variable it was just spliced in to answer)"
        )

    # A kind='cell' node_idn (which every spliced graft's own root row now
    # is) must be globally unique across the whole tree -- the same
    # pre-existing assumption _cell_from_tree already leans on for
    # hand-authored kind='cell' [[nodes]] entries (tree.filter(node_idn ==
    # ...).row(0)). A variable's own node_idn is typically far LESS
    # deliberately unique than a hand-chosen cell instantiation id (e.g.
    # "parameter_pick_next" reused verbatim across every sampler-swappable
    # cell in a bundle), so grafting makes a collision meaningfully more
    # likely -- checked explicitly here so a collision fails loudly at
    # parse time instead of silently building the wrong cell for one of
    # the two (tree.filter(...).row(0) would just pick one arbitrarily).
    cell_node_idns = tree.filter( pl.col( 'kind' ) == 'cell' )[ 'node_idn' ]
    dupes = cell_node_idns.filter( cell_node_idns.is_duplicated() ).unique().to_list()
    if dupes:
        raise ValueError(
            f"node_idn(s) {dupes!r} identify more than one kind='cell' node "
            f"in the assembled tree after graft splicing -- every kind='cell' "
            f"node_idn (including a variable's own node_idn, once "
            f"grafted) must be unique across the WHOLE tree, not just "
            f"locally; give the variable(s) an explicit, distinct "
            f"node_idn"
        )

    resolved_resources = resolve_resource_declarations( declarations[ 'resources' ] )
    resolved_actors    = resolve_actor_declarations( declarations[ 'actors' ] )
    return tree, parameters, node_idx, resolved_resources, resolved_actors
#/def splice_actor_grafts


def _config_tree_row(
    node_idn:            str,
    parent_idn:          str | None,
    kind:                str,
    source:              Sequence[ str ],
    target:              Sequence[ str ],
    path:                str | None,
    abs_path:             str,
    cell_source:         Sequence[ str ] | None = None,
    cell_target:         Sequence[ str ] | None = None,
    variable_columns: str | None             = None,
    op_type:             str | None             = None,
    join:                str | None             = None,
    join_k:              int | None             = None,
    discard_on_null:     bool | None            = None,
    ) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'node_idn':             [ node_idn ],
            'parent_idn':           [ parent_idn ],
            'kind':                 [ kind ],
            'source':               [ list( source ) ],
            'target':               [ list( target ) ],
            'path':                 [ path ],
            'abs_path':             [ abs_path ],
            'cell_source':          [ list( cell_source ) if cell_source is not None else None ],
            'cell_target':          [ list( cell_target ) if cell_target is not None else None ],
            'variable_columns':  [ variable_columns ],
            'op_type':              [ op_type ],
            'join':                 [ join ],
            'join_k':               [ join_k ],
            'discard_on_null':      [ discard_on_null ],
        },
        schema = table_schemas[ 'config_tree' ],
    )
#/def _config_tree_row


def _variable_expr( columns: Any ) -> bdd.Expr:
    """
    Normalize a kind='variable' [[nodes]] entry's `columns` field to one
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
        f"variable 'columns' must be a dict or a string (exprParse grammar), got {type(columns)!r}"
    )
#/def _variable_expr


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
    op/variable/cell branches below (see schema.py's __nodes__ column
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


def _entry_discard_on_null_field( entry: dict[ str, Any ] ) -> bool:
    """Extract a [[nodes]] entry's discard_on_null -- shared by the
    op/variable/cell branches below, same as _entry_join_fields.
    Default True. No join-compatibility validation needed -- it's simply
    inert when join != 'all' (see schema.py's __nodes__ column docs)."""
    return bool( entry.get( 'discard_on_null', True ) )
#/def _entry_discard_on_null_field


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


def _resolve_target_path( target: str | None, own_abs_path: str ) -> str:
    """
    An variables.<channel> entry's `target` is relative to the declaring
    node itself -- omitted, "", or "." all mean "target myself"; anything
    else is dotted-joined onto own_abs_path (see module docstring).
    """
    if target in ( None, '', '.' ):
        return own_abs_path
    return f"{own_abs_path}.{target}"
#/def _resolve_target_path


def _resource_variable_entries(
    entries:      list[ dict[ str, Any ] ],
    own_abs_path: str,
    own_depth:    int,
    tvars:        dict[ str, str ],
    ) -> pl.DataFrame:
    """Parse this file's own [[variables.resources]] entries into declaration
    rows -- see module docstring. Each entry is one table_idn binding
    (table_index's own shape, via ReadWriteOps._table_ref_row) plus a
    'target' routing key."""
    rows: list[ dict[ str, Any ] ] = []
    for entry in entries:
        entry = dict( entry )
        target = entry.pop( 'target', None )
        row = _table_ref_row( _apply_template_vars_to_value( entry, tvars ) )
        row[ 'abs_path' ]        = _resolve_target_path( target, own_abs_path )
        row[ 'declarer_depth' ]  = own_depth
        rows.append( row )
    if not rows:
        return _empty_resource_decls()
    return pl.DataFrame( rows, schema = _RESOURCE_DECL_SCHEMA )
#/def _resource_variable_entries


def _actor_variable_entries(
    entries:      list[ dict[ str, Any ] ],
    own_abs_path: str,
    own_depth:    int,
    base_path:    Path,
    tvars:        dict[ str, str ],
    ) -> pl.DataFrame:
    """Parse this file's own [[variables.actors]] entries into declaration
    rows -- see module docstring. Each entry binds exactly one of:

    - op_type (+ optional 'parameters' kwargs, JSON-encoded here rather
      than routed through the __table_ops_parameters__ forest, to avoid
      that forest's op_idn-uniqueness assumption -- see the plan notes on
      op_ctor_kwargs_forest's flat keying), or
    - toml -- a whole-cell graft, resolved to an absolute path relative to
      THIS file (base_path, same {key} tvars convention a kind='cell'
      [[nodes]] entry's own 'toml' already gets) right here, at parse
      time, since this is the last point this declaration still knows
      which file it came from. The actual splicing (parsing that file and
      grafting its subtree in place of the target variable) happens
      later, in splice_actor_grafts() -- only after every declaration in
      the whole tree is collected and root-wins resolution has picked a
      winner, since only the RESOLVED binding is the one that should
      actually be spliced in.
    """
    rows: list[ dict[ str, Any ] ] = []
    for entry in entries:
        entry = dict( entry )
        target = entry.pop( 'target', None )
        target_path = _resolve_target_path( target, own_abs_path )
        has_op_type = 'op_type' in entry
        has_toml    = 'toml' in entry
        if has_op_type == has_toml:
            raise ValueError(
                f"[[variables.actors]] entry targeting {target_path!r} must have "
                f"exactly one of 'op_type' or 'toml'"
            )
        if has_toml:
            toml_ref = entry.pop( 'toml' )
            if entry:
                raise ValueError(
                    f"[[variables.actors]] entry targeting {target_path!r} has "
                    f"unrecognized key(s) {sorted(entry)!r} -- a 'toml' graft binding "
                    f"takes no other keys (the grafted cell's own construction, "
                    f"including its own ctor kwargs, is entirely its own file's concern)"
                )
            resolved_toml_path = str( ( base_path / apply_template_vars( toml_ref, tvars ) ).resolve() )
            rows.append( {
                'abs_path':        target_path,
                'declarer_depth':  own_depth,
                'kind':            'cell',
                'op_type':         None,
                'path':            resolved_toml_path,
                'kwargs_json':     None,
                'declaring_path':  own_abs_path,
            } )
            continue
        op_type = entry.pop( 'op_type' )
        kwargs  = _apply_template_vars_to_value( entry.pop( 'parameters', {} ), tvars )
        if entry:
            raise ValueError(
                f"[[variables.actors]] entry targeting {target_path!r} has "
                f"unrecognized key(s) {sorted(entry)!r}"
            )
        rows.append( {
            'abs_path':        target_path,
            'declarer_depth':  own_depth,
            'kind':            'op',
            'op_type':         op_type,
            'path':            None,
            'kwargs_json':     json.dumps( kwargs ),
            'declaring_path':  own_abs_path,
        } )
    if not rows:
        return _empty_actor_decls()
    return pl.DataFrame( rows, schema = _ACTOR_DECL_SCHEMA )
#/def _actor_variable_entries


def bundle_variable_declarations(
    data:          dict[ str, Any ],
    root_node_idn: str,
    base_path:     Path,
    tvars:         dict[ str, str ] | None = None,
    ) -> dict[ str, pl.DataFrame ]:
    """
    Parse a bundle.toml's own [[variables.resources]]/[[variables.actors]]
    entries (see bundle.py's cell_from_bundle) into declaration rows,
    addressed at root_node_idn's own subtree using the same target-routing
    convention an ordinary cell TOML's own variables header uses -- 'target'
    is a dotted path relative to root_node_idn itself, omitted/''/'.'
    meaning root_node_idn itself (see module docstring above).

    declarer_depth is fixed at -1: a bundle.toml is never itself a node in
    the config_tree() it links to -- it sits entirely outside that tree --
    so its own bindings must be senior to anything the linked cell (or any
    of its own descendants) declares for itself by default. Same precedent
    as cell_from_file()'s own `resources` param (an outside-the-tree
    caller override, also declarer_depth=-1).
    """
    tvars = tvars or {}
    variables_header = data.get( 'variables', {} )
    return {
        'resources': _resource_variable_entries(
            variables_header.get( 'resources', [] ), root_node_idn, -1, tvars,
        ),
        'actors': _actor_variable_entries(
            variables_header.get( 'actors', [] ), root_node_idn, -1, base_path, tvars,
        ),
    }
#/def bundle_variable_declarations


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
    discard_on_null:     bool = True,
    ancestor_path:       tuple[ str, ... ] = (),
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int, dict[ str, pl.DataFrame ] ]:
    """
    Parse a config file and its transitive child-file references into
    (nodes, parameters, next_node_idx, declarations), dispatching to a
    format-specific parser by path's suffix. '.toml' and '.json' are both
    implemented (toml_config_tree/json_config_tree); anything else raises.

    node_idx_start/next_node_idx: the same dual-return counter-threading
    pattern ParameterParser.to_polars()/to_polars_flat() already use --
    every [[parameters]] entry parsed anywhere in this file or its
    transitive kind='cell' [[nodes]] entries draws its own parameter tree's node_idx values
    from one running counter, so the whole forest this call produces has
    globally-unique node_idx (not just unique per entry).

    ancestor_path: the dotted chain of node_idns from the OUTERMOST caller's
    own root down to (but not including) this file's own node_idn -- empty
    for a root call. Threaded one level deeper on every recursive kind='cell'
    call so every row this call (and its children) produce carries a
    correct abs_path -- see module docstring's variables.resources/
    variables.actors section.

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
        discard_on_null     = discard_on_null,
        ancestor_path       = ancestor_path,
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
    discard_on_null:     bool = True,
    ancestor_path:       tuple[ str, ... ] = (),
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int, dict[ str, pl.DataFrame ] ]:
    """
    Parse a cell TOML file and its transitive kind='cell' [[nodes]] file references
    into (nodes, parameters, next_node_idx, declarations). See
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
        discard_on_null     = discard_on_null,
        ancestor_path       = ancestor_path,
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
    discard_on_null:     bool = True,
    ancestor_path:       tuple[ str, ... ] = (),
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int, dict[ str, pl.DataFrame ] ]:
    """
    Parse a cell JSON file and its transitive kind='cell' [[nodes]] file
    references into (nodes, parameters, next_node_idx, declarations) -- the
    JSON equivalent of toml_config_tree(): same {'cell': {...}, 'nodes':
    [...], 'parameters': [...], 'variables': {...}} shape TOML's
    [cell]/[[nodes]]/[[parameters]]/[[variables.resources]]/
    [[variables.actors]] tables parse to, just spelled as JSON. See
    _config_tree_from_data() for the shared parsing logic.
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
        discard_on_null     = discard_on_null,
        ancestor_path       = ancestor_path,
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
    discard_on_null:     bool = True,
    ancestor_path:       tuple[ str, ... ] = (),
    ) -> tuple[ pl.DataFrame, pl.DataFrame, int, dict[ str, pl.DataFrame ] ]:
    """
    Format-agnostic body shared by toml_config_tree()/json_config_tree():
    given an already-parsed {'cell': {...}, 'nodes': [...], 'parameters':
    [...], 'variables': {...}} dict plus the resolved path it came from,
    build this file's (nodes, parameters, next_node_idx, declarations).

    declarations is {'resources': DataFrame, 'actors': DataFrame} -- every
    variables.resources/variables.actors entry parsed anywhere in this
    file or its transitive kind='cell' children, flattened into two frames
    (NOT resolved yet -- resolve_resource_declarations()/
    resolve_actor_declarations() do that, once, over the whole tree this
    top-level config_tree() call produces). See module docstring.

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

    own_path     = ancestor_path + ( node_idn, )
    own_abs_path = '.'.join( own_path )
    own_depth    = len( ancestor_path )

    cell_header = data.get( 'cell', {} )
    node_rows: list[ pl.DataFrame ] = [ _config_tree_row(
        node_idn     = node_idn,
        parent_idn   = parent_idn,
        kind         = 'cell',
        source       = source,
        target       = target,
        path         = str( path ),
        abs_path         = own_abs_path,
        cell_source      = cell_header.get( 'source' ),
        cell_target      = cell_header.get( 'target' ),
        join             = join,
        join_k           = join_k,
        discard_on_null  = discard_on_null,
    ) ]
    param_rows: list[ pl.DataFrame ] = [ _empty_parameters() ]
    node_idx = node_idx_start

    variables_header = data.get( 'variables', {} )
    resource_decl_frames: list[ pl.DataFrame ] = [ _resource_variable_entries(
        variables_header.get( 'resources', [] ), own_abs_path, own_depth, tvars,
    ) ]
    actor_decl_frames: list[ pl.DataFrame ] = [ _actor_variable_entries(
        variables_header.get( 'actors', [] ), own_abs_path, own_depth, path.parent, tvars,
    ) ]

    # Two passes over [[nodes]], not one: op/variable rows are appended
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
        entry_abs_path = f"{own_abs_path}.{entry_node_idn}"

        if kind == 'op':
            join, join_k = _entry_join_fields( entry, entry_node_idn )
            discard_on_null_entry = _entry_discard_on_null_field( entry )
            node_rows.append( _config_tree_row(
                node_idn         = entry_node_idn,
                parent_idn       = node_idn,
                kind             = 'op',
                source           = entry.get( 'source', [] ),
                target           = entry.get( 'target', [] ),
                path             = None,
                abs_path          = entry_abs_path,
                op_type          = entry.get( 'op_type', entry_node_idn ),
                join             = join,
                join_k           = join_k,
                discard_on_null  = discard_on_null_entry,
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
        elif kind == 'variable':
            if 'parameters' in entry:
                raise ValueError(
                    f"[[nodes]] entry {entry_node_idn!r} declares 'parameters' but "
                    f"kind='variable' -- variables are built directly as "
                    f"PlaceholderOp(columns=...), never through the "
                    f"__table_ops_parameters__ forest, so inline parameters here "
                    f"would silently never be read"
                )
            join, join_k = _entry_join_fields( entry, entry_node_idn )
            discard_on_null_entry = _entry_discard_on_null_field( entry )
            node_rows.append( _config_tree_row(
                node_idn             = entry_node_idn,
                parent_idn           = node_idn,
                kind                 = 'variable',
                source               = entry.get( 'source', [] ),
                target               = entry.get( 'target', [] ),
                path                 = None,
                abs_path              = entry_abs_path,
                variable_columns  = expr_to_text( _variable_expr( entry.get( 'columns', {} ) ) ),
                join                 = join,
                join_k               = join_k,
                discard_on_null      = discard_on_null_entry,
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
                f"expected 'op', 'cell', or 'variable'"
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
        discard_on_null_entry = _entry_discard_on_null_field( entry )
        child_nodes, child_params, node_idx, child_decls = config_tree(
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
            discard_on_null     = discard_on_null_entry,
            ancestor_path       = own_path,
        )
        node_rows.append( child_nodes )
        param_rows.append( child_params )
        resource_decl_frames.append( child_decls[ 'resources' ] )
        actor_decl_frames.append( child_decls[ 'actors' ] )

    return (
        pl.concat( node_rows,  how = 'vertical' ),
        # diagonal_relaxed, not vertical: each [[parameters]] entry (here or
        # in any recursively-loaded child) registers its own dtypes
        # independently (parameter_group_to_tree), so fragments' value-column
        # sets can differ -- reconcile by filling missing columns with null
        # rather than requiring identical schemas.
        pl.concat( param_rows, how = 'diagonal_relaxed' ),
        node_idx,
        {
            'resources': pl.concat( resource_decl_frames, how = 'vertical' ),
            'actors':    pl.concat( actor_decl_frames,    how = 'vertical' ),
        },
    )
#/def _config_tree_from_data
