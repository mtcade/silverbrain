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
one of its own children (via `[[children]]`, below) assigns its own,
independent parent-level `source`/`target` names for it -- only the *arity*
(tuple length) needs to agree across that boundary, never the names
themselves.

### `[[processes]]` -- leaf ops

```toml
[[processes]]
node_idn = "double"     # key op_classes must resolve
source   = ["a"]        # names this op's positional inputs are read from
target   = ["b"]        # names this op's positional outputs are written under
```

One block per `TableOp` this Cell wires in. `node_idn` is the key looked up
in `op_classes` (a `dict[str, type[TableOp]]`); the instance is built via
`build_tableOps()` as `op_classes[node_idn](rng=rng, **kwargs)`, where
`kwargs` comes from that op's own `[[parameters]]` entry, if any (below) --
so an op with no declared parameters is just `cls(rng=rng)`, relying on its
own defaults (or a `TypeError` for a missing required kwarg). `source`/
`target` are names in *this Cell's own* namespace -- assigned here, by this
Cell, not by the op itself.

### `[[parameters]]` -- TableOp construction kwargs

```toml
[[parameters]]
node_idn  = "double"    # which [[processes]]/[[children]] node_idn (in this file)
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

### `[[children]]` -- nested cells

```toml
[[children]]
node_idn = "kid"                # key this child is registered under
toml     = "path/to/child.toml" # resolved relative to this file
source   = ["a"]                # optional
target   = ["b"]                # optional
```

One block per nested `Cell`, loaded recursively (`config_tree`/
`cell_from_file` infer file format from suffix at every level, so a child
can in principle use a different format than its parent -- only `.toml` is
implemented today). `source`/`target` here are assigned by *this* file, in
its own namespace -- independent of the child's own `[cell].source`/
`[cell].target`. If omitted, the child is still constructed and registered
in `tableOps` (reachable via `bind(node_idn)`), just not wired into this
Cell's own execution graph.

### Wiring, not a DAG

`node_idn` must be unique within one file (it's a dict key), but `source`/
`target` **names may repeat** across `[[processes]]`/`[[children]]` entries
-- the wiring graph is not required to be a DAG. A name may legitimately
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
[[processes]]
node_idn = "controller"
source   = ["n"]
target   = ["n", "escape"]     # loops back to "n"; "escape" stays null while looping

[[processes]]
node_idn = "passthrough"
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
