import os
import tarfile
import tomllib
from contextlib import ExitStack
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

from platformdirs import PlatformDirs

from .utilities import apply_template_vars

CELLS_INDEX_FILE_DEFAULT:  Path = Path('cells_index.toml')
BUNDLE_INDEX_FILE_DEFAULT: Path = Path('bundle_index.toml')
BUNDLE_TOML_DEFAULT:       Path = Path('bundle.toml')

# Backs package_data_root()'s own use of importlib.resources.as_file() --
# see that function's docstring for why this process-lifetime ExitStack
# (never explicitly closed) is the right call here, not a per-call `with`.
_resource_stack = ExitStack()


# -- App auto-discovery: every function below that needs an `app_name` (to
# key a platformdirs lookup) now accepts app_name=None, meaning "look it up
# from the current Python environment" rather than "require the caller to
# say" -- see _resolve_app_config()'s own docstring for the exact rule.
# Discovery is importlib.metadata entry points, not cwd/PATH-based: it
# reflects only what's actually INSTALLED in the interpreter currently
# running (identical whether launched via `uv run`, an activated venv, a
# global/pipx install, or a container image), so it behaves the same
# regardless of how this process was invoked or what directory it started
# in -- unlike e.g. walking up from cwd for a project marker file.

APP_CONFIG_ENTRY_POINT_GROUP: str = 'silverbrain.apps'


@dataclass( frozen = True )
class AppConfig:
    """
    What an app registers under the 'silverbrain.apps' entry-point group to
    make itself auto-discoverable -- see _resolve_app_config(). An app opts
    in by adding, to its own pyproject.toml:

        [project.entry-points."silverbrain.apps"]
        <app_name> = "<mypackage>:SOME_ATTR"

    where <mypackage>.SOME_ATTR is an AppConfig instance (built the normal
    way, e.g. AppConfig(app_name=APP_NAME, fallback_data_roots=FALLBACK_DATA_ROOTS)
    reusing constants the app already exposes for its own CLI). Requires a
    `uv sync`/reinstall after adding the entry so the distribution metadata
    actually carries it -- editable installs don't pick up a new
    entry-point declaration until reinstalled.
    """
    app_name:            str
    fallback_data_roots: dict[ str, Path ] = field( default_factory = dict )
#/class AppConfig


# Default fallback_data_roots key for a src-layout package's OWN data/ dir
# -- matches every cell TOML already written against a literal
# '{impl_data_root}' template var (see e.g. duckpond's seed_0/simpleEval_0
# wiring); autoconfigure_app() defaults to this so those files keep
# working unchanged, regardless of which app_name actually supplies it.
IMPL_DATA_ROOT_KEY: str = 'impl_data_root'


def package_data_root( package: str ) -> Path:
    """
    <package>/data -- a package's own bundled data/ dir (cell TOMLs,
    schema JSON, etc.), resolved via importlib.resources rather than
    hand-derived Path(__file__).resolve().parents[N] arithmetic. That
    arithmetic only ever works for an editable/src-layout install
    (site-packages just points back at the real source tree, so walking
    up from __file__ lands on the real repo root); it silently breaks for
    a genuinely installed wheel, where there's no such parent-relative
    relationship at all.

    importlib.resources.files(package) finds `package`'s own installed
    location correctly regardless of install method (editable, a normal
    wheel, or even a zipped one) -- but for that to see anything, data/
    must live INSIDE the package's own tree (src/<package>/data/), not a
    sibling top-level data/ next to src/; a wheel's separate `.data`
    category installs to a completely different, unrelated location (e.g.
    sys.prefix/share/...), not reachable this way at all.

    Callers here need a real Path (fallback_data_roots values end up
    substituted as literal filesystem path strings into cell TOML
    {template_var}s, later opened by configTree.py) rather than the
    resource-Traversable files() itself returns, so this goes through
    as_file() -- a zero-cost passthrough to the real path for the common
    directory-install case, a transparent temp-extraction for a zipped
    one. The as_file() context is intentionally never exited (entered on
    a module-level ExitStack instead, see below): the directory case has
    nothing to clean up, and this Path is meant to live for the rest of
    the process, not one `with` block.
    """
    from importlib.resources import as_file, files
    return _resource_stack.enter_context( as_file( files( package ) / 'data' ) )
#/def package_data_root


def autoconfigure_app(
    app_name:                  str,
    *,
    own_root_key:              str = IMPL_DATA_ROOT_KEY,
    extra_fallback_data_roots: dict[ str, Path ] | None = None,
    ) -> AppConfig:
    """
    Build an AppConfig for `app_name` with its OWN fallback_data_roots
    entry (own_root_key, default 'impl_data_root') filled in automatically
    via package_data_root(app_name) -- app_name doubles as the
    package to introspect, which holds for every app in this workspace
    today (its platformdirs identity and its importable package name are
    the same string).

    extra_fallback_data_roots is still explicit and required from the
    caller for anything beyond the app's own data/ dir -- e.g. a shared
    library's own shipped-default cells (duckpond's own
    'silverflow_data_root': package_data_root('silverflow')). This is
    deliberate, not an oversight: auto-including every installed
    dependency's own data/ dir (walking importlib.metadata.requires(),
    say) would silently grow a bundle's cell search path based on
    whatever else happens to be installed in the environment, including
    packages that ship a data/ dir for reasons having nothing to do with
    silverbrain cells at all -- a much larger, riskier inference than
    "this one app's own data/ dir lives at the conventional src-layout
    spot", which is all this function actually automates. Pass
    extra_fallback_data_roots={'x': package_data_root('x'), ...} to add
    more, using the same helper for each.
    """
    roots: dict[ str, Path ] = { own_root_key: package_data_root( app_name ) }
    roots.update( extra_fallback_data_roots or {} )
    return AppConfig( app_name = app_name, fallback_data_roots = roots )
#/def autoconfigure_app


def discover_app_configs( group: str = APP_CONFIG_ENTRY_POINT_GROUP ) -> dict[ str, AppConfig ]:
    """
    {entry-point name: AppConfig} for every app registered under `group` in
    the CURRENT Python environment (importlib.metadata.entry_points --
    environment-scoped, not cwd-scoped; see module-level comment above).
    Raises TypeError if some registered entry doesn't resolve to an
    AppConfig (or an equivalent plain dict) -- a misconfigured
    pyproject.toml entry should fail loudly, not be silently skipped.
    """
    found: dict[ str, AppConfig ] = {}
    for ep in entry_points( group = group ):
        obj = ep.load()
        if isinstance( obj, AppConfig ):
            cfg = obj
        elif isinstance( obj, dict ):
            cfg = AppConfig( **obj )
        else:
            dist = ep.dist.name if ep.dist is not None else '?'
            raise TypeError(
                f"entry point {ep.name!r} (group={group!r}, from distribution "
                f"{dist!r}) must resolve to a bundle.AppConfig (or an "
                f"equivalent {{'app_name': ..., 'fallback_data_roots': ...}} "
                f"dict), got {type(obj)!r}"
            )
        found[ ep.name ] = cfg
    #/for ep in entry_points( group = group )
    return found
#/def discover_app_configs


def _resolve_app_config(
    app_name:            str | None,
    fallback_data_roots: dict[ str, Path ] | None,
    ) -> tuple[ str, dict[ str, Path ] ]:
    """
    The one rule every app_name=None-accepting function below applies:
    app_name is the sole auto-discovery trigger, never fallback_data_roots
    on its own --

      - app_name given (any string): returned as-is, fallback_data_roots
        given-or-{} -- IDENTICAL to this module's behavior before
        auto-discovery existed; discover_app_configs() is never even
        called, so an explicit caller pays no cost and sees no new
        behavior.
      - app_name=None: exactly one app must be registered under
        APP_CONFIG_ENTRY_POINT_GROUP in the current environment (zero or
        more than one both raise a clear, actionable ValueError rather
        than guessing) -- its app_name is used, and its
        fallback_data_roots is used too UNLESS the caller already passed
        an explicit (non-None) fallback_data_roots, which wins.

    This means an explicit fallback_data_roots can never "partially"
    trigger discovery on its own -- only app_name=None does, and when it
    does, app_name/fallback_data_roots are treated as one atomic unit from
    a single registered app (never mixed across two different apps).
    """
    if app_name is not None:
        return app_name, ( fallback_data_roots or {} )

    configs = discover_app_configs()
    if len( configs ) == 0:
        raise ValueError(
            "app_name was not given and no app is registered under the "
            f"{APP_CONFIG_ENTRY_POINT_GROUP!r} entry-point group in the "
            "current Python environment -- pass app_name explicitly, or "
            "add a [project.entry-points.\"silverbrain.apps\"] entry "
            "(pointing at a silverbrain.bundle.AppConfig) to your app's "
            "own pyproject.toml"
        )
    if len( configs ) > 1:
        names = ', '.join( sorted( configs ) )
        raise ValueError(
            "app_name was not given and more than one app is registered "
            f"under {APP_CONFIG_ENTRY_POINT_GROUP!r} in the current Python "
            f"environment ({names}) -- pass app_name explicitly to "
            "disambiguate"
        )
    [ only ] = configs.values()
    resolved_fallback = fallback_data_roots if fallback_data_roots is not None else only.fallback_data_roots
    return only.app_name, resolved_fallback
#/def _resolve_app_config

# -- Generic idn -> path TOML index. cells_index.toml (cell_idn -> its file's
# location) is the only user of this shape now -- an idn/toml pair per
# [[<array_key>]] block ('toml', not 'path' -- same key a kind='cell'
# [[nodes]] entry itself uses for its own child file, so an index entry and
# a hand-authored graft/node read the same way), path always stored
# RELATIVE to fp's own directory (both directions: read already resolved
# relative to fp.parent; write now uses os.path.relpath rather than an
# absolute path, so the file + whatever it points at stay portable
# together, and so does bundle_index.toml's own optional bundle_root,
# written the same way separately below).

def _read_idn_index( fp: Path, array_key: str, idn_key: str ) -> dict[ str, Path ]:
    """
        Return {idn: absolute_path} from a TOML idn->path index file:
        [[<array_key>]] blocks each carrying <idn_key> and 'toml' (path
        resolved relative to fp's own directory).
    """
    with open( fp, 'rb' ) as f:
        data = tomllib.load( f )
    #/with open( fp, 'rb' ) as f

    result: dict[ str, Path ] = {}
    base = fp.parent
    for entry in data.get( array_key, [] ):
        result[ entry[ idn_key ] ] = ( base / entry[ 'toml' ] ).resolve()
    #/for entry in data.get( array_key, [] )
    return result
#/def _read_idn_index

def _write_idn_index(
    fp:        Path,
    entries:   dict[ str, Path ],
    array_key: str,
    idn_key:   str,
) -> None:
    fp.parent.mkdir( parents=True, exist_ok=True )
    lines = []
    for idn, path in entries.items():
        rel = os.path.relpath( str( Path( path ).resolve() ), start=str( fp.parent ) )
        lines.append( f'[[{array_key}]]' )
        lines.append( f'{idn_key} = {idn!r}' )
        lines.append( f'toml = {rel!r}' )
        lines.append( '' )
    #/for idn, path in entries.items()
    fp.write_text( '\n'.join( lines ) )
#/def _write_idn_index


# -- Cells index (cell_idn -> its file's location) -- global, searched via
# platformdirs then fallback_data_roots (see resolve_cell_fp). Pure file
# lookup, no bundle semantics at all: a library cell (never attached to any
# bundle, e.g. a shared parameterSearch_0.toml) and a bundle's own top-level
# cell are registered here exactly the same way -- "the index is just to
# tell where to find things" (see bundle_index.toml below for the separate,
# bundle-specific concept of bundle_root/which cell_idn is a bundle's own).

def _read_cells_index( fp: Path ) -> dict[ str, Path ]:
    return _read_idn_index( fp, array_key='cells', idn_key='cell_idn' )
#/def _read_cells_index

def _write_cells_index( fp: Path, cells: dict[ str, Path ] ) -> None:
    _write_idn_index( fp, cells, array_key='cells', idn_key='cell_idn' )
#/def _write_cells_index

def resolve_cell_fp(
    cell_idn:               str,
    app_name:               str | None                = None,
    fallback_data_roots:    dict[ str, Path ] | None = None,
    cells_index_file:       Path = CELLS_INDEX_FILE_DEFAULT,
    ) -> Path:
    """
    Find a cell's file location for a given cell_idn -- no bundle
    involvement whatsoever (see module docstring above cells_index).

    app_name=None (the default) auto-discovers via _resolve_app_config()
    -- exactly one app must be registered in the current environment (see
    that function's docstring); pass app_name explicitly to skip
    discovery entirely (unchanged pre-discovery behavior).

    Search order (first match wins):
      1. User-global cells_index.toml (platformdirs) — installed bundles'
         own cells, plus any globally-registered library cell
      2. Each directory in fallback_data_roots, in insertion order — e.g.
         {'impl_data_root': <some implementation repo>/data,
          'silverflow_data_root': <silverflow repo>/data}: an
         implementation's own local/dev cells_index.toml, checked before a
         shared library's shipped-defaults one. This function has no
         opinion on what those names/directories represent -- naming them
         meaningfully is what lets a bundle's template_vars turn the
         identical dict into {impl_data_root}/etc. substitutions for cell
         TOML files; resolve_cell_fp() itself only ever reads the dict's
         values, in order.
      3. silverbrain-arrow's own package_data_root('silverbrain') -- the
         lowest-priority fallback, checked last, after every caller-
         supplied fallback_data_roots entry. Exists so this framework
         package can itself ship default cells one day without silently
         out-ranking any implementation's or silverflow's own; today
         src/silverbrain/data/ doesn't exist at all, so this stop is
         inert (never .exists()) until something is deliberately put
         there.
    """
    app_name, fallback_data_roots = _resolve_app_config( app_name, fallback_data_roots )
    global_index = PlatformDirs( app_name ).user_data_path / cells_index_file
    checked: list[ Path ] = [ global_index ]
    if global_index.exists():
        global_cells = _read_cells_index( global_index )
        if cell_idn in global_cells:
            return global_cells[ cell_idn ]
        #/if cell_idn in global_cells
    #/if global_index.exists()

    for root in ( fallback_data_roots or {} ).values():
        fallback_fp = Path( root ) / cells_index_file
        checked.append( fallback_fp )
        if fallback_fp.exists():
            fallback_cells = _read_cells_index( fallback_fp )
            if cell_idn in fallback_cells:
                return fallback_cells[ cell_idn ]
            #/if cell_idn in fallback_cells
        #/if fallback_fp.exists()
    #/for root in ( fallback_data_roots or {} ).values()

    own_fp = package_data_root( 'silverbrain' ) / cells_index_file
    checked.append( own_fp )
    if own_fp.exists():
        own_cells = _read_cells_index( own_fp )
        if cell_idn in own_cells:
            return own_cells[ cell_idn ]
        #/if cell_idn in own_cells
    #/if own_fp.exists()

    raise ValueError(
        f"cell_idn={cell_idn!r} not found in any of {checked!r}"
    )
#/def resolve_cell_fp

def register_cell(
    cell_idn:          str,
    path:               Path,
    app_name:           str | None = None,
    cells_index_file:   Path = CELLS_INDEX_FILE_DEFAULT,
) -> None:
    """Add or update a cell_idn -> path entry in the user-global
    cells_index.toml. app_name=None auto-discovers -- see resolve_cell_fp."""
    app_name, _ = _resolve_app_config( app_name, None )
    global_index = PlatformDirs( app_name ).user_data_path / cells_index_file

    existing: dict[ str, Path ] = {}
    if global_index.exists():
        existing = _read_cells_index( global_index )
    #/if global_index.exists()

    existing[ cell_idn ] = Path( path ).resolve()
    _write_cells_index( global_index, existing )
#/def register_cell


# -- Bundle index (bundle_name -> {bundle_root, cell_idn}) -- global only,
# never searched via fallback_data_roots (a bundle is always a real,
# locally-registered thing, never a library default the way a cell can be).
# Deliberately NOT built on _read_idn_index/_write_idn_index above: a
# bundle entry has no single required 'path' the way a cell entry does --
# bundle_root is optional (falls back to the BundleDirs convention) and
# cell_idn isn't path-like at all.

@dataclass( frozen = True )
class BundleEntry:
    bundle_root: Path
    cell_idn:    str
#/class BundleEntry

def _read_bundle_index( fp: Path ) -> dict[ str, dict[ str, Any ] ]:
    """Return {bundle_name: {'cell_idn': str, 'bundle_root': Path | absent}}
    -- 'bundle_root', when present, is resolved relative to fp's own
    directory, same convention as cells_index.toml's 'path'."""
    with open( fp, 'rb' ) as f:
        data = tomllib.load( f )
    #/with open( fp, 'rb' ) as f

    base = fp.parent
    result: dict[ str, dict[ str, Any ] ] = {}
    for entry in data.get( 'bundles', [] ):
        item: dict[ str, Any ] = { 'cell_idn': entry[ 'cell_idn' ] }
        if 'bundle_root' in entry:
            item[ 'bundle_root' ] = ( base / entry[ 'bundle_root' ] ).resolve()
        #/if 'bundle_root' in entry
        result[ entry[ 'bundle_name' ] ] = item
    #/for entry in data.get( 'bundles', [] )
    return result
#/def _read_bundle_index

def _write_bundle_index( fp: Path, bundles: dict[ str, dict[ str, Any ] ] ) -> None:
    fp.parent.mkdir( parents=True, exist_ok=True )
    lines = []
    for bundle_name, entry in bundles.items():
        lines.append( '[[bundles]]' )
        lines.append( f'bundle_name = {bundle_name!r}' )
        lines.append( f"cell_idn = {entry[ 'cell_idn' ]!r}" )
        bundle_root = entry.get( 'bundle_root' )
        if bundle_root is not None:
            rel = os.path.relpath( str( Path( bundle_root ).resolve() ), start=str( fp.parent ) )
            lines.append( f'bundle_root = {rel!r}' )
        #/if bundle_root is not None
        lines.append( '' )
    #/for bundle_name, entry in bundles.items()
    fp.write_text( '\n'.join( lines ) )
#/def _write_bundle_index

def resolve_bundle(
    bundle_name:        str,
    app_name:            str | None = None,
    bundle_index_file:   Path = BUNDLE_INDEX_FILE_DEFAULT,
    ) -> BundleEntry:
    """
    Resolve bundle_name to its BundleEntry (bundle_root, cell_idn) via the
    user-global bundle_index.toml only. bundle_root falls back to today's
    platformdirs convention (BundleDirs(bundle_name, app_name).root) when
    the entry doesn't declare one explicitly -- most bundles (installed
    the normal way) already live exactly there and need nothing extra.
    Raises ValueError if bundle_name isn't registered, or its entry has no
    cell_idn. app_name=None auto-discovers -- see resolve_cell_fp.
    """
    app_name, _ = _resolve_app_config( app_name, None )
    global_index = PlatformDirs( app_name ).user_data_path / bundle_index_file
    if not global_index.exists():
        raise ValueError( f"bundle_name={bundle_name!r} not found -- no {global_index!r}" )
    #/if not global_index.exists()

    bundles = _read_bundle_index( global_index )
    if bundle_name not in bundles:
        raise ValueError( f"bundle_name={bundle_name!r} not found in {global_index!r}" )
    #/if bundle_name not in bundles

    entry = bundles[ bundle_name ]
    bundle_root = entry.get( 'bundle_root' ) or BundleDirs( bundle_name, app_name ).root
    return BundleEntry( bundle_root=bundle_root, cell_idn=entry[ 'cell_idn' ] )
#/def resolve_bundle

def register_bundle(
    bundle_name:        str,
    cell_idn:            str,
    app_name:            str | None = None,
    bundle_root:         Path | None = None,
    bundle_index_file:   Path = BUNDLE_INDEX_FILE_DEFAULT,
) -> None:
    """
    Add or update a bundle_name entry in the user-global bundle_index.toml
    -- bundle_root omitted (the common case) falls back to the platformdirs
    convention at resolve time (see resolve_bundle), not written explicitly
    here. Registering the bundle's own top-level cell_idn's file location
    is a separate step (register_cell) -- a bundle's cell_idn still needs
    an ordinary cells_index.toml entry pointing at its actual file, same as
    any other cell. app_name=None auto-discovers -- see resolve_cell_fp.
    """
    app_name, _ = _resolve_app_config( app_name, None )
    global_index = PlatformDirs( app_name ).user_data_path / bundle_index_file

    existing: dict[ str, dict[ str, Any ] ] = {}
    if global_index.exists():
        existing = _read_bundle_index( global_index )
    #/if global_index.exists()

    entry: dict[ str, Any ] = { 'cell_idn': cell_idn }
    if bundle_root is not None:
        entry[ 'bundle_root' ] = Path( bundle_root ).resolve()
    #/if bundle_root is not None
    existing[ bundle_name ] = entry
    _write_bundle_index( global_index, existing )
#/def register_bundle


def _template_vars(
    bundle_root:              Path,
    app_name:                 str,
    fallback_data_roots:      dict[ str, Path ] | None = None,
    ) -> dict[ str, str ]:
    """{bundle_root}/{app_root}, plus one {name} entry per
    fallback_data_roots key (e.g. {impl_data_root}/{silverflow_data_root})
    -- for cell TOML files loaded via cell_from_bundle()/resolve_cell_fp().
    bundle_root is whatever resolve_bundle() already resolved (convention
    or explicit override) -- this function no longer recomputes it itself.

    {app_root} is the platformdirs top-level root itself (PlatformDirs(
    app_name).user_data_path -- the same directory bundle_index.toml/
    cells_index.toml/every bundles/<name>/, settings/<name>/ live directly
    under). {bundle_root}/{app_root} are deliberately the ONLY bundle-
    layout shorthand offered here -- no {settings_root}/{data_root}-style
    var whose target could silently depend on some other key elsewhere in
    the same file. A bundle.toml spells out explicitly which OTHER
    bundle's settings tree it means when reusing one (e.g.
    "{app_root}/settings/train_0/..."), while {bundle_root} is what a
    reusable CELL file (not bundle.toml) uses for its own on-disk state
    that should resolve differently per bundle that loads it -- e.g.
    silverflow's randomSampler_0.toml points its own best-so-far cache at
    "{bundle_root}/data/search_state.db" -- since every bundle_root now has
    its own data/ dir directly underneath it (BundleDirs.data), that
    resolves correctly with no bundle.toml override needed at all."""
    tvars: dict[ str, str ] = {
        'bundle_root': str( bundle_root ),
        'app_root':    str( PlatformDirs( app_name ).user_data_path ),
    }
    for name, root in ( fallback_data_roots or {} ).items():
        tvars[ name ] = str( Path( root ) )
    #/for name, root in ( fallback_data_roots or {} ).items()
    return tvars
#/def _template_vars


def _bundle_cell_link(
    bundle_data:         dict[ str, Any ],
    bundle_toml_fp:      Path,
    app_name:            str,
    fallback_data_roots: dict[ str, Path ] | None,
    cells_index_file:    Path,
    template_vars:       dict[ str, str ],
    ) -> tuple[ Path, str ]:
    """
    Resolve a bundle.toml's own [cell] table -- the LINKED cell this bundle
    wraps -- to (cell_fp, main_id). Exactly one of:

        [cell]
        toml = "path/to/cell.toml"   # resolved relative to bundle.toml's
                                       # own directory, same {key} template-
                                       # var substitution a kind='cell'
                                       # [[nodes]] entry's own 'toml' gets
        # ... or:
        [cell]
        cell_idn = "some-cell-idn"    # resolved via resolve_cell_fp(),
                                       # same as a kind='cell' [[nodes]]
                                       # entry using 'cell_idn' instead of
                                       # 'toml'

    is required -- same has-exactly-one-of grammar
    configTree._config_tree_from_data's own kind='cell' [[nodes]] entries
    already enforce, reused here since a bundle.toml's [cell] table plays
    the identical role for its own single linked cell. main_id is cell_idn
    when given, else the toml path's own stem (e.g. 'train_0.toml' ->
    'train_0') -- this becomes the linked cell's own root node_idn/abs_path,
    exactly as if bundle.toml's variables.resources/variables.actors
    entries had been declared inside the linked cell's own file (see
    cell_from_bundle).
    """
    cell_header  = bundle_data.get( 'cell', {} )
    has_toml     = 'toml' in cell_header
    has_cell_idn = 'cell_idn' in cell_header
    if has_toml == has_cell_idn:
        raise ValueError(
            f"bundle.toml at {bundle_toml_fp!r} must declare exactly one of "
            f"[cell].toml or [cell].cell_idn -- the linked cell this bundle wraps"
        )
    if has_cell_idn:
        cell_idn = cell_header[ 'cell_idn' ]
        cell_fp  = resolve_cell_fp( cell_idn, app_name, fallback_data_roots, cells_index_file )
        return cell_fp, cell_idn
    toml_ref = cell_header[ 'toml' ]
    cell_fp  = ( bundle_toml_fp.parent / apply_template_vars( toml_ref, template_vars ) ).resolve()
    return cell_fp, Path( toml_ref ).stem
#/def _bundle_cell_link


def cell_from_bundle(
    bundle_name:             str,
    op_classes:              dict[ str, type ],
    app_name:                str | None = None,
    fallback_data_roots:     dict[ str, Path ] | None = None,
    cells_index_file:        Path = CELLS_INDEX_FILE_DEFAULT,
    bundle_index_file:       Path = BUNDLE_INDEX_FILE_DEFAULT,
    bundle_toml_file:        Path = BUNDLE_TOML_DEFAULT,
    rng                           = None,
    verbose:                 int  = 0,
    resources                     = None,
    ):
    """
    Load the top-level Cell for a registered bundle_name.

    Resolution: resolve_bundle() finds this bundle's bundle_root via
    bundle_index.toml; {bundle_root}/bundle_toml_file (default
    'bundle.toml') is then read for its own [cell] table -- naming the
    LINKED cell this bundle wraps, resolved exactly like an ordinary
    kind='cell' [[nodes]] entry (one of 'toml' or 'cell_idn' -- see
    _bundle_cell_link()) -- plus whatever [[variables.resources]]/
    [[variables.actors]] entries it declares (configTree.
    bundle_variable_declarations()), applied against the linked cell's own
    subtree with declarer_depth=-1: senior to any declaration the linked
    cell (or any node inside it) makes for itself, since bundle.toml sits
    entirely outside that cell's own config_tree() call (see that
    function's own docstring). A bundle.toml is NOT parsed as an ordinary
    cell TOML -- it has no [[nodes]]/[[parameters]] of its own; its only
    two responsibilities are naming the linked cell and supplying
    bindings for it/its descendants from outside its file entirely.

    The linked cell's own file may itself declare kind='cell' children
    (via 'toml' or 'cell_idn', same resolution machinery) and its own
    [[variables.actors]] grafts/bindings, same as any other cell TOML --
    see configTree.py's module docstring.

    resources (a table_index-shaped resource manifest, see ReadWriteOps.py)
    is forwarded to cell_from_file as an even-more-senior override still --
    see that function's own `resources` param docstring. Callers typically
    load it via ReadWriteOps.load_resources() against this same bundle's
    resolved bundle_root/resources.json before calling this.

    app_name=None (the default) auto-discovers -- see resolve_cell_fp --
    resolved exactly once here, then passed down as a concrete value to
    resolve_bundle()/resolve_cell_fp() (which never re-discover once
    given a real app_name)."""
    from . import configTree
    from .cell import cell_from_file

    app_name, fallback_data_roots = _resolve_app_config( app_name, fallback_data_roots )
    entry          = resolve_bundle( bundle_name, app_name, bundle_index_file )
    bundle_toml_fp = entry.bundle_root / bundle_toml_file
    if not bundle_toml_fp.exists():
        raise FileNotFoundError(
            f"{bundle_toml_file!r} not found for bundle {bundle_name!r} at {bundle_toml_fp}"
        )

    with open( bundle_toml_fp, 'rb' ) as f:
        bundle_data = tomllib.load( f )

    template_vars = _template_vars( entry.bundle_root, app_name, fallback_data_roots )
    cell_fp, main_id = _bundle_cell_link(
        bundle_data, bundle_toml_fp, app_name, fallback_data_roots, cells_index_file, template_vars,
    )
    extra_declarations = configTree.bundle_variable_declarations(
        bundle_data, main_id, bundle_toml_fp.parent, template_vars,
    )
    return cell_from_file(
        path                = cell_fp,
        op_classes          = op_classes,
        main_id             = main_id,
        rng                 = rng,
        verbose             = verbose,
        template_vars       = template_vars,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file    = cells_index_file,
        resources           = resources,
        extra_declarations  = extra_declarations,
    )
#/def cell_from_bundle


def resolve_bundle_target(
    name:                    str,
    app_name:                str | None = None,
    fallback_data_roots:     dict[ str, Path ] | None = None,
    cells_index_file:        Path = CELLS_INDEX_FILE_DEFAULT,
    bundle_index_file:       Path = BUNDLE_INDEX_FILE_DEFAULT,
    bundle_toml_file:        Path = BUNDLE_TOML_DEFAULT,
    ) -> tuple[ Path, dict[ str, str ], str, str ]:
    """
    Resolve `name` to a cell FILE (no live Cell construction -- for
    tooling that only wants to read/introspect a cell tree, e.g. sf-viz,
    not run one) by trying, in the same order a human would guess it:

      1. A registered bundle_name -- resolve_bundle() then this bundle's
         own bundle.toml [cell] table (_bundle_cell_link()), exactly like
         cell_from_bundle(), giving a real {bundle_root} template var. Any
         [[variables.resources]]/[[variables.actors]] bundle.toml declares
         are NOT applied here -- this only resolves the linked FILE, it
         never constructs anything.
      2. A bare cell_idn -- resolve_cell_fp() alone, no bundle_root (a
         library cell was never attached to one); only fallback_data_roots'
         own {name} template vars are available.

    Returns (cell_fp, template_vars, resolved_app_name, root_node_idn) --
    root_node_idn is the bundle's own linked cell's main_id in case (1), or
    `name` itself in case (2) (same main_id convention cell_from_bundle/
    cell_from_file use). Raises ValueError/FileNotFoundError if neither
    resolves. app_name=None auto-discovers -- see resolve_cell_fp;
    resolved once, up front, so both attempts below share one concrete
    app_name rather than re-discovering per attempt.
    """
    app_name, fallback_data_roots = _resolve_app_config( app_name, fallback_data_roots )
    try:
        entry = resolve_bundle( name, app_name, bundle_index_file )
    except ValueError:
        pass
    else:
        bundle_toml_fp = entry.bundle_root / bundle_toml_file
        if not bundle_toml_fp.exists():
            raise FileNotFoundError(
                f"{bundle_toml_file!r} not found for bundle {name!r} at {bundle_toml_fp}"
            )
        with open( bundle_toml_fp, 'rb' ) as f:
            bundle_data = tomllib.load( f )
        template_vars = _template_vars( entry.bundle_root, app_name, fallback_data_roots )
        cell_fp, main_id = _bundle_cell_link(
            bundle_data, bundle_toml_fp, app_name, fallback_data_roots, cells_index_file, template_vars,
        )
        return cell_fp, template_vars, app_name, main_id

    cell_fp = resolve_cell_fp( name, app_name, fallback_data_roots, cells_index_file )
    template_vars = { k: str( Path( v ) ) for k, v in ( fallback_data_roots or {} ).items() }
    return cell_fp, template_vars, app_name, name
#/def resolve_bundle_target


# -- Directory layout

class BundleDirs:
    """
    Resolves platformdirs paths for a named bundle -- pure directory
    layout, no cell-resolution responsibility (that's cells_index.toml/
    resolve_cell_fp's job; which cell_idn is this bundle's own top-level
    one is bundle_index.toml/resolve_bundle's job).

    Layout under user_data_path/<app_name>/:
        bundles/<bundle_name>/
            bundle.toml # by default -- two independent roles, two separate
                        # top-level tables in the SAME file:
                        #   [bundle]   self-description (default_cell_idn,
                        #              required_cell_idns), read once at
                        #              pull-from-hub time to seed
                        #              bundle_index/cells_index
                        #   [cell]     the LINKED cell this bundle wraps
                        #              (one of 'toml'/'cell_idn', same
                        #              grammar a kind='cell' [[nodes]]
                        #              entry uses) plus this bundle's own
                        #              [[variables.resources]]/
                        #              [[variables.actors]] bindings for
                        #              it -- consulted at ordinary
                        #              resolution time by
                        #              cell_from_bundle()/
                        #              resolve_bundle_target(), see those
                        #              functions' own docstrings
            cells/              — bundle-specific cell files (optional)
            data/               — parquet/sqlite outputs, this bundle's own
            notebooks/          — Marimo .py notebooks
        settings/<bundle_name>/ — parameter JSON/TSV files, resources.json

    settings/ is the one exception kept OUTSIDE bundles/<bundle_name>/ --
    still keyed by bundle_name (one subdirectory per bundle), a sibling of
    bundles/ itself, so a bundle.toml can point {app_root}/settings/
    <some OTHER bundle_name>/... at a fixture tree it intentionally reuses
    (e.g. an 'observe_0' reading 'train_0's own settings) without also
    reusing that other bundle's data/. data/, by contrast, is back under
    each bundle's own bundles/<bundle_name>/ -- addressed via {bundle_root}
    (see _template_vars()), not {app_root}, since a bundle's own on-disk
    outputs are never meant to be borrowed by another bundle the way its
    settings sometimes are.
    """

    def __init__( self, bundle_name: str, app_name: str ) -> None:
        self._bundle_name = bundle_name
        self._app_root    = PlatformDirs( app_name ).user_data_path
    #

    @property
    def root( self ) -> Path:
        return self._app_root / 'bundles' / self._bundle_name
    #

    @property
    def cells( self ) -> Path:
        return self.root / 'cells'
    #

    @property
    def settings( self ) -> Path:
        return self._app_root / 'settings' / self._bundle_name
    #

    @property
    def data( self ) -> Path:
        return self.root / 'data'
    #

    @property
    def notebooks( self ) -> Path:
        return self.root / 'notebooks'
    #

    def ensure_dirs( self ) -> None:
        for d in ( self.cells, self.settings, self.data, self.notebooks ):
            d.mkdir( parents=True, exist_ok=True )
    #
# -- Loading the top-level cell from a bundle's own bundle.toml

def _default_cell_idn(
    dirs: BundleDirs,
    bundle_name: str,
    bundle_toml: Path, # likely 'bundle.toml'
) -> str:
    """Read the required `default_cell_idn` from a bundle's `bundle_toml`
    -- a bundle's own self-description, read once (at pull-from-hub time)
    to seed bundle_index.toml, not consulted at ordinary resolution time
    (see resolve_bundle/cell_from_bundle)."""
    bundle_meta_fp = dirs.root / bundle_toml
    if not bundle_meta_fp.exists():
        raise FileNotFoundError(
            f"{bundle_toml!r} not found for bundle {bundle_name!r} at {bundle_meta_fp}"
        )
    #
    with open( bundle_meta_fp, 'rb' ) as f:
        meta = tomllib.load( f ).get( 'bundle', {} )
    #

    if 'default_cell_idn' not in meta:
        raise ValueError(
            f"bundle_toml at {bundle_toml!r} for {bundle_name!r} is missing required 'default_cell_idn'"
        )
    #
    return meta[ 'default_cell_idn' ]
#/def _default_cell_idn

# -- Packing

def pack(
    bundle_name: str,
    output_path: Path,
    app_name:    str,
) -> Path:
    """Archive a bundle dir to a .tar.gz file."""
    dirs        = BundleDirs( bundle_name, app_name )
    output_path = Path( output_path )
    with tarfile.open( output_path, 'w:gz' ) as tar:
        tar.add( dirs.root, arcname=bundle_name )
    return output_path
#/def pack

# -- Hugging Face Hub push/pull

def push_to_hub(
    bundle_name: str,
    hf_repo_id:  str,
    app_name:    str,
    token:       str | None = None,
) -> None:
    """Upload bundle dir (cells + settings + data + notebooks) to HF Hub."""
    from huggingface_hub import upload_folder

    dirs = BundleDirs( bundle_name, app_name )
    upload_folder(
        folder_path = str( dirs.root ),
        repo_id     = hf_repo_id,
        repo_type   = 'dataset',
        token       = token,
    )
#/def push_to_hub

def pull_from_hub(
    hf_repo_id:  str,
    bundle_name: str,
    app_name:    str,
    token:       str | None = None,
    bundle_toml: Path = BUNDLE_TOML_DEFAULT,
) -> BundleDirs:
    """
    Download bundle from HF Hub into platformdirs bundles location, then
    register it: reads the pulled bundle's own bundle_toml for its
    default_cell_idn, registers the bundle itself (register_bundle --
    bundle_root omitted, since dirs.root *is* the convention path this was
    just downloaded to) and its top-level cell's file location
    (register_cell), and fetches any missing required commons cells.
    """
    from huggingface_hub import snapshot_download

    dirs = BundleDirs( bundle_name, app_name )
    dirs.root.mkdir( parents=True, exist_ok=True )
    snapshot_download(
        repo_id   = hf_repo_id,
        repo_type = 'dataset',
        local_dir = str( dirs.root ),
        token     = token,
    )

    _fetch_missing_commons( dirs, app_name, bundle_toml, token )
    cell_idn = _default_cell_idn( dirs, bundle_name, bundle_toml )
    register_bundle( bundle_name, cell_idn, app_name )
    register_cell( cell_idn, dirs.root / bundle_toml, app_name )
    return dirs
#/def pull_from_hub

def _fetch_missing_commons(
    dirs:        BundleDirs,
    app_name:    str,
    bundle_toml: Path, # relative to bundle root
    token:       str | None,
) -> None:
    """
    Pull missing required cells from the commons HF repo. Since a cell's
    on-disk path is no longer guessable from its cell_idn, this always
    refreshes the local global cells_index.toml from the remote repo
    first (the remote index is the source of truth for what's actually
    there), then resolves each required cell_idn against it before
    fetching exactly the missing files.
    """
    bundle_meta_fp = dirs.root / bundle_toml
    if not bundle_meta_fp.exists():
        return
    #

    with open( bundle_meta_fp, 'rb' ) as f:
        meta = tomllib.load( f ).get( 'bundle', {} )
    #

    required_cell_idns: list[ str ] = meta.get( 'required_cell_idns', [] )
    if not required_cell_idns:
        return
    #

    commons_repo = os.environ.get(
        f'{app_name.upper()}_COMMONS_REPO',
    )

    from huggingface_hub import snapshot_download

    app_root = dirs._app_root
    global_cells_index = app_root / CELLS_INDEX_FILE_DEFAULT

    # 1. Refresh the local global cells_index.toml from the remote repo.
    snapshot_download(
        repo_id        = commons_repo,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ str( CELLS_INDEX_FILE_DEFAULT ) ],
        token          = token,
    )

    remote_index = (
        _read_cells_index( global_cells_index ) if global_cells_index.exists() else {}
    )
    unresolved = [ c for c in required_cell_idns if c not in remote_index ]
    if unresolved:
        raise ValueError(
            f"required_cell_idns {unresolved!r} not found in commons repo "
            f"{commons_repo!r}'s cells_index.toml"
        )
    #

    # 2. Only fetch what's actually missing locally.
    local_index = _read_cells_index( global_cells_index ) if global_cells_index.exists() else {}
    missing_cell_idns = [ c for c in required_cell_idns if c not in local_index ]
    if not missing_cell_idns:
        return
    #

    missing_rel_paths = {
        c: remote_index[ c ].relative_to( app_root.resolve() )
        for c in missing_cell_idns
    }
    snapshot_download(
        repo_id        = commons_repo,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ str( p ) for p in missing_rel_paths.values() ],
        token          = token,
    )

    # 3. Merge the newly-fetched entries into the local global index.
    for c in missing_cell_idns:
        register_cell( c, app_root / missing_rel_paths[ c ], app_name )
    #
#/def _fetch_missing_commons

def push_commons_to_hub(
    hf_repo_id: str,
    app_name:   str,
    token:      str | None = None,
) -> None:
    """Upload the global cells_index.toml + whatever it references to a
    single HF Hub dataset repo."""
    from huggingface_hub import upload_folder

    app_root = PlatformDirs( app_name ).user_data_path
    upload_folder(
        folder_path    = str( app_root ),
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        allow_patterns = [ str( CELLS_INDEX_FILE_DEFAULT ), 'cells/*' ],
        token          = token,
    )
#/def push_commons_to_hub

def pull_commons_from_hub(
    hf_repo_id: str,
    app_name:   str,
    token:      str | None = None,
) -> None:
    """Download the global cells_index.toml + commons cells/ from HF Hub
    into platformdirs."""
    from huggingface_hub import snapshot_download

    app_root = PlatformDirs( app_name ).user_data_path
    app_root.mkdir( parents=True, exist_ok=True )
    snapshot_download(
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ str( CELLS_INDEX_FILE_DEFAULT ), 'cells/*' ],
        token          = token,
    )
#/def pull_commons_from_hub

# -- Registration and listing

def list_installed(
    app_name: str,
    bundle_index_file: Path = BUNDLE_INDEX_FILE_DEFAULT,
) -> list[ str ]:
    """List bundle names registered in the user-global bundle_index.toml."""
    global_index = PlatformDirs( app_name ).user_data_path / bundle_index_file
    if not global_index.exists():
        return []
    #
    return list( _read_bundle_index( global_index ).keys() )
#/def list_installed
