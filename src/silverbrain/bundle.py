import os
import tarfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from platformdirs import PlatformDirs

CELLS_INDEX_FILE_DEFAULT:  Path = Path('cells_index.toml')
BUNDLE_INDEX_FILE_DEFAULT: Path = Path('bundle_index.toml')
BUNDLE_TOML_DEFAULT:       Path = Path('bundle.toml')

# -- Generic idn -> path TOML index. cells_index.toml (cell_idn -> its file's
# location) is the only user of this shape now -- an idn/path pair per
# [[<array_key>]] block, path always stored RELATIVE to fp's own directory
# (both directions: read already resolved relative to fp.parent; write now
# uses os.path.relpath rather than an absolute path, so the file + whatever
# it points at stay portable together, and so does bundle_index.toml's own
# optional bundle_root, written the same way separately below).

def _read_idn_index( fp: Path, array_key: str, idn_key: str ) -> dict[ str, Path ]:
    """
        Return {idn: absolute_path} from a TOML idn->path index file:
        [[<array_key>]] blocks each carrying <idn_key> and 'path' (path
        resolved relative to fp's own directory).
    """
    with open( fp, 'rb' ) as f:
        data = tomllib.load( f )
    #/with open( fp, 'rb' ) as f

    result: dict[ str, Path ] = {}
    base = fp.parent
    for entry in data.get( array_key, [] ):
        result[ entry[ idn_key ] ] = ( base / entry[ 'path' ] ).resolve()
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
        lines.append( f'path = {rel!r}' )
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
    app_name:               str,
    fallback_data_roots:    dict[ str, Path ] | None = None,
    cells_index_file:       Path = CELLS_INDEX_FILE_DEFAULT,
    ) -> Path:
    """
    Find a cell's file location for a given cell_idn -- no bundle
    involvement whatsoever (see module docstring above cells_index).

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
    """
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

    raise ValueError(
        f"cell_idn={cell_idn!r} not found in any of {checked!r}"
    )
#/def resolve_cell_fp

def register_cell(
    cell_idn:          str,
    path:               Path,
    app_name:           str,
    cells_index_file:   Path = CELLS_INDEX_FILE_DEFAULT,
) -> None:
    """Add or update a cell_idn -> path entry in the user-global cells_index.toml."""
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
    app_name:            str,
    bundle_index_file:   Path = BUNDLE_INDEX_FILE_DEFAULT,
    ) -> BundleEntry:
    """
    Resolve bundle_name to its BundleEntry (bundle_root, cell_idn) via the
    user-global bundle_index.toml only. bundle_root falls back to today's
    platformdirs convention (BundleDirs(bundle_name, app_name).root) when
    the entry doesn't declare one explicitly -- most bundles (installed
    the normal way) already live exactly there and need nothing extra.
    Raises ValueError if bundle_name isn't registered, or its entry has no
    cell_idn.
    """
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
    app_name:            str,
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
    any other cell.
    """
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
    fallback_data_roots:      dict[ str, Path ] | None = None,
    ) -> dict[ str, str ]:
    """{bundle_root}, plus one {name} entry per fallback_data_roots key
    (e.g. {impl_data_root}/{silverflow_data_root}) -- for cell TOML files
    loaded via cell_from_bundle()/resolve_cell_fp(). bundle_root is
    whatever resolve_bundle() already resolved (convention or explicit
    override) -- this function no longer recomputes it itself."""
    tvars: dict[ str, str ] = { 'bundle_root': str( bundle_root ) }
    for name, root in ( fallback_data_roots or {} ).items():
        tvars[ name ] = str( Path( root ) )
    #/for name, root in ( fallback_data_roots or {} ).items()
    return tvars
#/def _template_vars


def cell_from_bundle(
    bundle_name:             str,
    app_name:                str,
    op_classes:              dict[ str, type ],
    fallback_data_roots:     dict[ str, Path ] | None = None,
    cells_index_file:        Path = CELLS_INDEX_FILE_DEFAULT,
    bundle_index_file:       Path = BUNDLE_INDEX_FILE_DEFAULT,
    rng                           = None,
    verbose:                 int  = 0,
    resources                     = None,
    ):
    """
    Load the top-level Cell for a registered bundle_name: resolve_bundle()
    finds its {bundle_root, cell_idn} via bundle_index.toml, then
    resolve_cell_fp() finds that cell_idn's actual file via cells_index.toml
    (platformdirs, then fallback_data_roots) -- bundle_root/resources
    resolution happens exactly once, here, at the top; every nested
    kind='cell' [[nodes]] reference (library cells, sub-components) reuses
    these same template_vars unchanged (see configTree.py) -- they're
    never re-resolved per nested cell_idn, by design (a nested cell is
    never itself "a bundle"). resources (a table_index-shaped resource
    manifest, see ReadWriteOps.py) is forwarded to cell_from_file as the
    ROOT cell's own caller-override -- merged there (caller-wins) against
    the root cell's own declared [resources] TOML defaults; NOT
    automatically visible to any nested kind='cell' child unless that
    child's own instantiation site declares a [nodes.resources] override
    in TOML (see cell.py's cell_from_file/configTree.py's
    [resources]/[nodes.resources]). Callers typically load it via
    ReadWriteOps.load_resources() against this same bundle's resolved
    bundle_root/resources.json before calling this.
    """
    from .cell import cell_from_file

    entry         = resolve_bundle( bundle_name, app_name, bundle_index_file )
    cell_fp       = resolve_cell_fp( entry.cell_idn, app_name, fallback_data_roots, cells_index_file )
    template_vars = _template_vars( entry.bundle_root, fallback_data_roots )
    return cell_from_file(
        path                = cell_fp,
        op_classes          = op_classes,
        main_id             = entry.cell_idn,
        rng                 = rng,
        verbose             = verbose,
        template_vars       = template_vars,
        app_name            = app_name,
        fallback_data_roots = fallback_data_roots,
        cells_index_file    = cells_index_file,
        resources           = resources,
    )
#/def cell_from_bundle


# -- Directory layout

class BundleDirs:
    """
    Resolves platformdirs paths for a named bundle -- pure directory
    layout, no cell-resolution responsibility (that's cells_index.toml/
    resolve_cell_fp's job; which cell_idn is this bundle's own top-level
    one is bundle_index.toml/resolve_bundle's job).

    Layout under user_data_path/<app_name>/:
        bundles/<bundle_name>/
            bundle.toml # by default -- a bundle's own self-description
                        # (default_cell_idn, required_cell_idns), read
                        # once at pull-from-hub time to seed bundle_index/
                        # cells_index -- not consulted at resolution time
            cells/              — bundle-specific cell files (optional)
            settings/           — parameter JSON files, resources.json
            data/               — parquet outputs
            notebooks/          — Marimo .py notebooks
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
        return self.root / 'settings'
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
