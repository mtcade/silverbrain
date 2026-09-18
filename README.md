# silverbrain

Python implementation of a local silverbrain node and cluster

## Cell TOML map

A `Cell` (`src/silverbrain/cell.py`) is a stateful compute unit built from
named children -- leaf `TableOp`s and/or nested `Cell`s -- wired together
declaratively. `cell_from_file(path, op_classes, main_id, ...)` builds a
live `Cell` (and any children it references) from a `.toml` file. This
section documents that TOML shape.

### `[cell]`

```toml
[cell]
class  = "..."     # informational only, not read by cell_from_file
source = ["a"]     # this Cell's own external input binding
target = ["z"]     # this Cell's own external output binding
```

`source`/`target` are this Cell's own contract with whoever calls it:
`source` names the positions of the `dfs` tuple passed into `Cell.__call__`
(seeded under these names before anything runs); `target` names which
resolved values -- in this order -- make up the tuple `__call__`'s Future
resolves to. Both are optional; a `Cell` with neither declared is still
constructible and still reachable through `bind(node_idn)` from whatever
registered it, it just has no wiring graph of its own to run through
`__call__`.

These names are **private to this Cell**. A parent that nests this Cell as
one of its own children (a `kind = "cell"` `[[nodes]]` entry, below)
assigns its own, independent parent-level `source`/`target` names for it --
only the *arity* (tuple length) needs to agree across that boundary, never
the names themselves.

### `[[nodes]]` -- every child this Cell owns

```toml
[[nodes]]
node_idn = "double"     # unique among this file's own nodes
kind     = "op"          # "op" | "cell" | "placeholder"
source   = ["a"]        # names this node's positional inputs are read from
target   = ["b"]        # names this node's positional outputs are written under
```

One block per leaf `TableOp`, nested `Cell`, or placeholder this Cell wires
in -- `source`/`target` are names in *this Cell's own* namespace, assigned
here regardless of `kind`. `node_idn` must be unique within one file (it's
a dict key); it doubles as `self.tableOps[node_idn]`'s lookup key once the
`Cell` is live, so any node -- whichever `kind` built it -- can later be
swapped for a different `TableOp`/`Cell` instance post-construction without
touching this file. `kind` only governs how the node is *parsed and built*:

- **`kind = "op"`**: `node_idn` is the key looked up in `op_classes` (a
  `dict[str, type[TableOp]]`); the instance is built via `build_tableOps()`
  as `op_classes[node_idn](rng=rng, **kwargs)`, where `kwargs` comes from
  that op's own `[[parameters]]` entry, if any (below) -- so an op with no
  declared parameters is just `cls(rng=rng)`, relying on its own defaults
  (or a `TypeError` for a missing required kwarg).

- **`kind = "cell"`**: also requires `toml = "path/to/child.toml"`
  (resolved relative to this file). Loaded recursively (`config_tree`/
  `cell_from_file` infer file format from suffix at every level, so a child
  can in principle use a different format than its parent -- only `.toml`
  is implemented today). If `source`/`target` are both omitted, the child
  is still constructed and registered in `tableOps` (reachable via
  `bind(node_idn)`), just not wired into this Cell's own execution graph.

- **`kind = "placeholder"`**: also requires `columns = { a = "Int64" }` (a
  `{col_name = "DtypeStr"}` table). Its interface (an arity-1 input/output
  pair matching `columns`) is declared up front, but no implementation is
  bound -- it builds a `tableOps.PlaceholderOp`, which raises if ever
  actually called. Wiring, schema checks, and `__table_op_schema__` all see
  a real node from the start; swap `cell.tableOps[node_idn]` for a real
  `TableOp`/`Cell` before this node is reached on a live execution path.

A `target` name of `"_"` is reserved: it discards that result slot rather
than wiring it anywhere -- the node still runs (side effects included), the
value just never enters this Cell's resolved values and nothing can depend
on it. Useful for a node that always returns something you don't need
downstream (e.g. a write op that echoes the table it wrote, per its own
convention -- see the op's own docs). Any number of nodes may declare
`target = ["_"]`; unlike real names, it's never ambiguous who "owns" it.

### `[[parameters]]` -- TableOp construction kwargs

```toml
[[parameters]]
node_idn  = "double"    # which [[nodes]] node_idn (in this file, kind="op")
threshold = 0.5         # inline kwargs, straight to op_classes[node_idn](rng=rng, **these)
label     = "x"
```

One block per parameterized node. Either give kwargs inline (as above), or
point to exactly one external file instead (never both, and never mixed
with inline keys):

```toml
[[parameters]]
node_idn = "double"
toml     = "path/to/double_params.toml"   # or:
json     = "path/to/double_params.json"
```

An external file's top-level content *is* the kwargs dict -- no `node_idn`
key inside it, no extra wrapping:

```toml
# double_params.toml
threshold = 0.5
label     = "x"
```

A value may also be a search/random spec dict (the same shape
`ParameterParser`'s JSON specs use elsewhere, e.g.
`threshold = { values_search = [0.1, 0.5] }`), since `[[parameters]]`
parses into the same `__table_ops_parameters__` forest other parameter
consumers read -- but `build_tableOps()` itself only accepts concrete
(`fixed`) values; a search/random spec on a node `op_classes` will actually
construct raises at construction time.

### Wiring, not a DAG

`node_idn` must be unique within one file (it's a dict key), but `source`/
`target` **names may repeat** across `[[nodes]]` entries -- the wiring
graph is not required to be a DAG. A name may legitimately
feed more than one node (fan-out), and a node whose `source` lists more
than one name (fan-in) only fires once every one of those names holds a
value produced *since that node last consumed it* -- so four independent
producers can, in pairs, each feed a different two-way fan-in without
cross-talk.

Calling a `Cell` never blocks: `__call__` returns a
`concurrent.futures.Future`, resolved once every name in this Cell's own
`target` has a value. Internally it runs a small wiring engine
(`_WiringEngine` in `cell.py`): whichever registered node has all its
`source` names available and at least one *new* (non-null) value since its
last firing gets dispatched on a shared thread pool; its result is written
back under its `target` names, and the process repeats until this Cell's
own `target` is fully resolved. If nothing is fireable and nothing is still
running, but the target isn't resolved, `__call__`'s Future raises a
`RuntimeError` naming the unresolved names -- there's no silent hang.

### Loops

The same node can fire more than once per outer call. A stateless
loop-controller node looks at its own input and decides, purely from that,
whether the loop continues: while active it emits a real value to the loop
body and `None` to the escape branch; once done, it flips -- `None` to the
loop body (which stops that branch's consumers from firing again) and a
real value to the escape branch. Each firing gets its own correlation id
segment, so nothing needs to track "which iteration" separately from that.

One wrinkle: a `Cell`'s own declared `target` resolves as soon as its name
is merely *present* among the collected values, not once it's *non-null*.
So a loop's escape branch should never write directly to the Cell's own
`target` across multiple rounds -- route it through a small passthrough
node instead, which (thanks to gen-tracking) only fires once a real,
non-null value actually arrives:

```toml
[[nodes]]
node_idn = "controller"
kind     = "op"
source   = ["n"]
target   = ["n", "escape"]     # loops back to "n"; "escape" stays null while looping

[[nodes]]
node_idn = "passthrough"
kind     = "op"
source   = ["escape"]
target   = ["done"]            # only fires once "escape" goes non-null
```

with `[cell] target = ["done"]`, not `["escape"]`.

### Correlation ids

Every `Cell.__call__` mints (or extends) a `corr_id: tuple[str, ...]` --
a fresh root segment (from `uuid7()`) if none was given, otherwise its own
extra segment appended to whatever it received. Each node firing gets its
own further segment derived from a per-node firing counter. This is what
lets concurrent invocations of the same `Cell`, and separate iterations of
the same loop, resolve fan-in joins without cross-talk. `TableOp.__call__`
doesn't accept `corr_id` (it's atomic, nothing to route between
internally) -- the dispatch layer detects this and calls it without one.

## Bundles

A bundle is a named, resolvable *instance* of a Cell TOML tree --
`bundle_name` (e.g. `"train_0"`) plus everything needed to run it: which
cell file it links, and any settings/data of its own. `src/silverbrain/
bundle.py`'s `cell_from_bundle(bundle_name, op_classes, ...)` resolves a
`bundle_name` all the way to a live `Cell`, the bundle-level counterpart to
`cell.py`'s `cell_from_file`.

### `bundle_index.toml` -- the global registry

A user-global TOML (`PlatformDirs(app_name).user_data_path/
bundle_index.toml`) mapping `bundle_name -> {cell_idn, bundle_root?}`:

```toml
[[bundles]]
bundle_name = 'train_0'
cell_idn    = 'train_0'
```

`register_bundle()`/`resolve_bundle()` read and write it. `bundle_root`
defaults to the `BundleDirs` convention below when omitted. **`cell_idn`
here is only a fallback hint for `BundleDirs`, not the linked-cell
resolution path** -- resolution always goes through this bundle's own
`bundle.toml` (below), never straight from this index.

### `bundle.toml` -- the linked cell + its own bindings

Every registered bundle must have an on-disk `bundle.toml` at its
`bundle_root` (`cell_from_bundle` raises `FileNotFoundError` if it's
missing). Two independent top-level tables can share the file:

- `[bundle]` -- self-description (`default_cell_idn`, `required_cell_idns`)
  read once at pull-from-hub time to seed `bundle_index.toml`/
  `cells_index.toml`. Not consulted at ordinary resolution time.
- `[cell]` -- the LINKED cell this bundle wraps, resolved with the same
  has-exactly-one-of grammar a `kind = "cell"` `[[nodes]]` entry uses
  (`_bundle_cell_link()`):

  ```toml
  [cell]
  cell_idn = "train_0"      # resolve_cell_fp(), same as [[nodes]] cell_idn
  # ... or:
  toml = "path/to/cell.toml"  # relative to this bundle.toml, template vars applied
  ```

  plus, optionally, `[[variables.resources]]`/`[[variables.actors]]`
  entries (same shape a cell TOML's own `variables` header uses) that bind
  into the linked cell's *own* subtree from outside its file entirely --
  parsed by `configTree.bundle_variable_declarations()` at
  `declarer_depth=-1`, so they're senior to any declaration the linked
  cell (or any node inside it) makes for itself. A `bundle.toml` is never
  parsed as an ordinary cell TOML: no `[[nodes]]`/`[[parameters]]` of its
  own, just these two responsibilities.

`main_id` for the resulting `Cell` is `cell_idn` when given, else the
`toml` path's own stem.

### Directory layout (`BundleDirs`)

```
user_data_path/<app_name>/
    bundle_index.toml
    cells_index.toml
    bundles/<bundle_name>/
        bundle.toml     # [bundle] + [cell], see above
        cells/          # bundle-specific cell files (optional)
        data/           # this bundle's own parquet/sqlite outputs
        notebooks/      # Marimo .py notebooks
    settings/<bundle_name>/
                        # parameter JSON/TSV files, resources.json
```

`settings/` is the one piece kept OUTSIDE `bundles/<bundle_name>/` --
still keyed by `bundle_name` (one subdirectory per bundle), a sibling of
`bundles/` itself. That's what lets one bundle's `bundle.toml` point at
*another* bundle's settings tree when it intentionally reuses fixtures
(e.g. an `observe_0` bundle reading `train_0`'s settings) without also
reusing that other bundle's `data/`.

Two template vars are available to a linked cell's own TOML (and to
`bundle.toml`'s `toml =`/`[[variables.resources]]` paths):

- `{bundle_root}` -- this bundle's own `bundles/<bundle_name>/` directory.
  What a reusable cell file uses for on-disk state that should resolve
  differently per bundle that loads it (e.g. a random-sampler's
  best-so-far cache at `"{bundle_root}/data/search_state.db"`).
- `{app_root}` -- the platformdirs top-level root itself
  (`user_data_path`), the directory `bundle_index.toml`/`cells_index.toml`/
  every `bundles/<name>/`, `settings/<name>/` live directly under. What a
  `bundle.toml` uses to spell out explicitly which *other* bundle's
  settings tree it means when reusing one (`"{app_root}/settings/
  train_0/..."`).

These are deliberately the only bundle-layout shorthand offered -- no
`{settings_root}`/`{data_root}`-style var whose target could silently
depend on some other key elsewhere in the same file.

### Resolution flow

`cell_from_bundle(bundle_name, ...)`:

1. `resolve_bundle()` -- `bundle_name` -> `{bundle_root, cell_idn}` via
   `bundle_index.toml` (`cell_idn` here unused past this point).
2. Read `{bundle_root}/bundle.toml`'s own `[cell]` table --
   `_bundle_cell_link()` resolves it to `(cell_fp, main_id)`.
3. Parse `[[variables.resources]]`/`[[variables.actors]]` from that same
   `bundle.toml` (`configTree.bundle_variable_declarations()`).
4. `cell_from_file(cell_fp, ..., main_id=main_id, extra_declarations=...)`
   -- the bundle.toml declarations are folded in ahead of the linked
   cell's own, same precedence as `cell_from_file`'s `resources` param (an
   outside-the-tree caller override).

`resolve_bundle_target()` follows the same bundle_name -> bundle.toml ->
`_bundle_cell_link()` path for tooling that only wants the resolved cell
FILE (e.g. `sf-viz`), not a live `Cell` -- it does not apply
`bundle.toml`'s own `variables.resources`/`variables.actors`.
