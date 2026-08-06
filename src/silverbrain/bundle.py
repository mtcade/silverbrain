import os
import tarfile
import tomllib
from pathlib import Path

from platformdirs import PlatformDirs

MAPS_INDEX_FILE_DEFAULT:  Path = Path('maps_index.toml')
CELLS_INDEX_FILE_DEFAULT: Path = Path('cells_index.toml')
BUNDLE_TOML_DEFAULT:      Path = Path('bundle.toml')

# -- Generic idn -> path TOML index. maps_index.toml (bundle main_id -> its
# default cell's absolute path) and cells_index.toml (cell_idn -> absolute
# path, one per cells/ directory) both reuse this exact shape -- an
# idn/path pair per [[<array_key>]] block. TOML-only by design: index/
# metadata files (this, bundle.toml) are always TOML; the *content* files
# they point at (cell maps) keep full .toml/.json flexibility via
# configTree.config_tree's own suffix dispatch.

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
    lines = []
    for idn, path in entries.items():
        lines.append( f'[[{array_key}]]' )
        lines.append( f'{idn_key} = {idn!r}' )
        lines.append( f'path = {str( path )!r}' )
        lines.append( '' )
    #/for idn, path in entries.items()
    fp.write_text( '\n'.join( lines ) )
#/def _write_idn_index


# -- Maps index (bundle main_id -> its default cell's absolute path)

def _read_maps_index( fp: Path ) -> dict[ str, Path ]:
    return _read_idn_index( fp, array_key='maps', idn_key='main_id' )
#/def _read_maps_index

def _write_maps_index( fp: Path, maps: dict[ str, Path ] ) -> None:
    _write_idn_index( fp, maps, array_key='maps', idn_key='main_id' )
#/def _write_maps_index

def resolve_map_fp(
    main_id:                str,
    app_name:               str,
    local_maps_index_fp:    Path | None = None,
    maps_index_file:        Path = MAPS_INDEX_FILE_DEFAULT,
    ) -> Path:
    """
    Find the cell map file for a given main_id.

    Search order (first match wins):
      1. Repo-local maps_index.toml (local_maps_index_fp) — dev/WIP configs
      2. User-global maps_index.toml (platformdirs) — installed bundles
    """
    # 1. repo-local
    if local_maps_index_fp is not None:
        local_fp = Path( local_maps_index_fp )
        if local_fp.exists():
            local_maps = _read_maps_index( local_fp )
            if main_id in local_maps:
                return local_maps[ main_id ]
            #/if main_id in local_maps
        #/if local_fp.exists()
    #/if local_maps_index_fp is not None

    # 2. user-global
    global_index = PlatformDirs( app_name ).user_data_path / maps_index_file
    if global_index.exists():
        global_maps = _read_maps_index( global_index )
        if main_id in global_maps:
            return global_maps[ main_id ]
        #/if main_id in global_maps
    #/if global_index.exists()

    raise ValueError(
        f"main_id={main_id!r} not found in"
        + ( f" {local_maps_index_fp!r} or" if local_maps_index_fp else "" )
        + f" {global_index!r}"
    )
#/def resolve_map_fp

def register_bundle(
    bundle_id:          str,
    bundle_map_path:    Path,
    app_name:           str,
    maps_index_file:    Path = MAPS_INDEX_FILE_DEFAULT,
) -> None:
    """Add or update a bundle entry in the user-global `maps_index_path`, where the index of maps is stored relative to `PlatformDirs( app_name ).user_data_path`."""
    global_index = PlatformDirs( app_name ).user_data_path / maps_index_file
    global_index.parent.mkdir( parents=True, exist_ok=True )

    existing: dict[ str, Path ] = {}
    if global_index.exists():
        existing = _read_maps_index( global_index )
    #/if global_index.exists()

    existing[ bundle_id ] = Path( bundle_map_path ).resolve()
    _write_maps_index( global_index, existing )
#/def register_bundle


# -- Cells index (cell_idn -> absolute path; one file per cells/ directory)

def _read_cells_index( fp: Path ) -> dict[ str, Path ]:
    return _read_idn_index( fp, array_key='cells', idn_key='cell_idn' )
#/def _read_cells_index

def _write_cells_index( fp: Path, cells: dict[ str, Path ] ) -> None:
    _write_idn_index( fp, cells, array_key='cells', idn_key='cell_idn' )
#/def _write_cells_index


# -- Directory layout

class BundleDirs:
    """
    Resolves platformdirs paths for a named bundle. A cell's on-disk path
    is never guessed from its cell_idn -- cells_index.toml (hand-maintained,
    living in cells/ and common cells/) is the only source of truth mapping
    cell_idn -> path, so a cell file can live anywhere, named anything, in
    any format configTree.config_tree understands.

    Layout under user_data_path/<app_name>/:
        cells/                  — common/shared cells + cells_index.toml
        bundles/<bundle_id>/
            bundle.toml # by default
            cells/              — bundle-specific cells + cells_index.toml
                                  (overrides common on matching cell_idn)
            settings/           — parameter JSON files, paths.json
            data/               — parquet outputs
            notebooks/          — Marimo .py notebooks
    """

    def __init__( self, bundle_id: str, app_name: str ) -> None:
        self._bundle_id = bundle_id
        self._app_root  = PlatformDirs( app_name ).user_data_path
    #

    @property
    def root( self ) -> Path:
        return self._app_root / 'bundles' / self._bundle_id
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

    @property
    def common_cells( self ) -> Path:
        return self._app_root / 'cells'
    #

    @property
    def cells_index_path( self ) -> Path:
        return self.cells / CELLS_INDEX_FILE_DEFAULT
    #

    @property
    def common_cells_index_path( self ) -> Path:
        return self.common_cells / CELLS_INDEX_FILE_DEFAULT
    #

    def cell_index( self ) -> dict[ str, Path ]:
        """Bundle-specific cell_idn -> absolute path index."""
        if not self.cells_index_path.exists():
            return {}
        return _read_cells_index( self.cells_index_path )
    #

    def common_cell_index( self ) -> dict[ str, Path ]:
        """Shared cell_idn -> absolute path index."""
        if not self.common_cells_index_path.exists():
            return {}
        return _read_cells_index( self.common_cells_index_path )
    #

    def cell_path( self, cell_idn: str ) -> Path:
        """Bundle-specific cells_index.toml first, then common cells_index.toml fallback."""
        bundle_index = self.cell_index()
        if cell_idn in bundle_index:
            return bundle_index[ cell_idn ]
        common_index = self.common_cell_index()
        if cell_idn in common_index:
            return common_index[ cell_idn ]
        raise FileNotFoundError(
            f"cell_idn={cell_idn!r} not found in {self.cells_index_path!r} "
            f"or {self.common_cells_index_path!r}"
        )
    #

    def register_cell( self, cell_idn: str, path: Path, common: bool = False ) -> None:
        """Add/update one entry in the bundle-specific (or, if common=True,
        shared) cells_index.toml."""
        index_path = self.common_cells_index_path if common else self.cells_index_path
        index_path.parent.mkdir( parents=True, exist_ok=True )
        existing = _read_cells_index( index_path ) if index_path.exists() else {}
        existing[ cell_idn ] = Path( path ).resolve()
        _write_cells_index( index_path, existing )
    #

    def _cells_root( self ) -> Path:
        """The directory to expose as {cells_root}: bundle-specific if its
        cell index has entries, else common."""
        if self.cell_index():
            return self.cells
        return self.common_cells
    #

    def ensure_dirs( self ) -> None:
        for d in ( self.cells, self.settings, self.data, self.notebooks ):
            d.mkdir( parents=True, exist_ok=True )
        self.common_cells.mkdir( parents=True, exist_ok=True )
    #
# -- Loading the top-level cell from a bundle

def _default_cell_idn(
    dirs: BundleDirs,
    bundle_id: str,
    bundle_toml: Path, # likely 'bundle.toml'
) -> str:
    """Read the required `default_cell_idn` from a bundle's `bundle_toml`."""
    bundle_meta_fp = dirs.root / bundle_toml
    if not bundle_meta_fp.exists():
        raise FileNotFoundError(
            f"{bundle_toml!r} not found for bundle {bundle_id!r} at {bundle_meta_fp}"
        )
    #
    with open( bundle_meta_fp, 'rb' ) as f:
        meta = tomllib.load( f ).get( 'bundle', {} )
    #

    if 'default_cell_idn' not in meta:
        raise ValueError(
            f"bundle_toml at {bundle_toml!r} for {bundle_id!r} is missing required 'default_cell_idn'"
        )
    #
    return meta[ 'default_cell_idn' ]
#/def _default_cell_idn

def _resolve_bundle_map(
    bundle_id: str,
    app_name:  str,
    bundle_toml: Path, # likely `BUNDLE_TOML_DEFAULT` = 'bundle.toml'
) -> tuple[ BundleDirs, str, Path, dict[ str, str ] ]:
    """
    Resolve a bundle's default cell_idn plus its file path and the
    {cells_root}/etc. template_vars every bundle-aware config load needs --
    shared by cell_from_bundle and config_tree_from_bundle.
    """
    dirs = BundleDirs(
        bundle_id,
        app_name,
    )
    cell_idn = _default_cell_idn(
        dirs,
        bundle_id,
        bundle_toml,
    )
    cell_fp = dirs.cell_path( cell_idn )
    template_vars = {
        'cells_root':   str( dirs._cells_root() ),
        'bundle_root':  str( dirs.root ),
        'app_bundles':  str( dirs._app_root / 'bundles' ),
    }
    return dirs, cell_idn, cell_fp, template_vars
#/def _resolve_bundle_map


def cell_from_bundle(
    bundle_id: str,
    app_name:  str,
    bundle_toml: Path = BUNDLE_TOML_DEFAULT,
    **kwargs,
):
    """
    Load the top-level Cell for an installed bundle by id, resolving
    {cells_root}. The bundle's map TOML (formerly a composite Web, now a
    plain [cell] with kind='cell' [[nodes]] entries) is loaded via
    cell_from_file, which delegates to configTree.py to recurse into any of
    them itself.
    """
    from .cell import cell_from_file

    dirs, cell_idn, cell_fp, template_vars = _resolve_bundle_map(
        bundle_id,
        app_name,
        bundle_toml,
    )
    kwargs.setdefault( 'main_id', cell_idn )
    return cell_from_file( path=cell_fp, template_vars=template_vars, **kwargs )
#/def cell_from_bundle

def config_tree_from_bundle(
    bundle_id: str,
    app_name:  str,
    bundle_toml: Path = BUNDLE_TOML_DEFAULT,
    **kwargs,
):
    """
    Parse (not construct) the (nodes, parameters) dataframe pair for an
    installed bundle's map TOML, resolving {cells_root}/etc. the same way
    cell_from_bundle does. Useful when you want the tree/forest themselves
    -- e.g. to inspect or feed to a Cell-building step manually -- rather
    than a live Cell.
    """
    from .configTree import config_tree

    dirs, cell_idn, cell_fp, template_vars = _resolve_bundle_map(
        bundle_id,
        app_name,
        bundle_toml,
    )
    kwargs.setdefault( 'node_idn', cell_idn )
    return config_tree( path=cell_fp, template_vars=template_vars, **kwargs )
#/def config_tree_from_bundle

# -- Packing

def pack(
    bundle_id:   str,
    output_path: Path,
    app_name:    str,
) -> Path:
    """Archive a bundle dir to a .tar.gz file."""
    dirs        = BundleDirs( bundle_id, app_name )
    output_path = Path( output_path )
    with tarfile.open( output_path, 'w:gz' ) as tar:
        tar.add( dirs.root, arcname=bundle_id )
    return output_path
#/def pack

# -- Hugging Face Hub push/pull

def push_to_hub(
    bundle_id:  str,
    hf_repo_id: str,
    app_name:   str,
    token:      str | None = None,
) -> None:
    """Upload bundle dir (cells + settings + data + notebooks) to HF Hub."""
    from huggingface_hub import upload_folder

    dirs = BundleDirs( bundle_id, app_name )
    upload_folder(
        folder_path = str( dirs.root ),
        repo_id     = hf_repo_id,
        repo_type   = 'dataset',
        token       = token,
    )
#/def push_to_hub

def pull_from_hub(
    hf_repo_id: str,
    bundle_id:  str,
    app_name:   str,
    token:      str | None = None,
    bundle_toml:   Path = BUNDLE_TOML_DEFAULT,
) -> BundleDirs:
    """
    Download bundle from HF Hub into platformdirs bundles location.
    Reads required_cell_idns from `bundle_toml` and fetches missing common
    items from the configured commons repo.
    """
    from huggingface_hub import snapshot_download

    dirs = BundleDirs( bundle_id, app_name )
    dirs.root.mkdir( parents=True, exist_ok=True )
    snapshot_download(
        repo_id   = hf_repo_id,
        repo_type = 'dataset',
        local_dir = str( dirs.root ),
        token     = token,
    )

    _fetch_missing_commons( dirs, app_name, bundle_toml, token )
    cell_idn = _default_cell_idn( dirs, bundle_id, bundle_toml )
    register_bundle( bundle_id, dirs.cell_path( cell_idn ), app_name )
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
    refreshes the local commons cells_index.toml from the remote repo
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

    # 1. Refresh the local commons cells_index.toml from the remote repo.
    snapshot_download(
        repo_id        = commons_repo,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ f'cells/{ CELLS_INDEX_FILE_DEFAULT }' ],
        token          = token,
    )

    remote_index = (
        _read_cells_index( dirs.common_cells_index_path )
        if dirs.common_cells_index_path.exists() else {}
    )
    unresolved = [ c for c in required_cell_idns if c not in remote_index ]
    if unresolved:
        raise ValueError(
            f"required_cell_idns {unresolved!r} not found in commons repo "
            f"{commons_repo!r}'s cells_index.toml"
        )
    #

    # 2. Only fetch what's actually missing locally.
    local_index = dirs.cell_index() | dirs.common_cell_index()
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

    # 3. Merge the newly-fetched entries into the local commons index.
    for c in missing_cell_idns:
        dirs.register_cell( c, app_root / missing_rel_paths[ c ], common=True )
    #
#/def _fetch_missing_commons

def push_commons_to_hub(
    hf_repo_id: str,
    app_name:   str,
    token:      str | None = None,
) -> None:
    """Upload common cells/ to a single HF Hub dataset repo."""
    from huggingface_hub import upload_folder

    app_root = PlatformDirs( app_name ).user_data_path
    upload_folder(
        folder_path    = str( app_root ),
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        allow_patterns = [ 'cells/*' ],
        token          = token,
    )
#/def push_commons_to_hub

def pull_commons_from_hub(
    hf_repo_id: str,
    app_name:   str,
    token:      str | None = None,
) -> None:
    """Download common cells/ from HF Hub into platformdirs."""
    from huggingface_hub import snapshot_download

    app_root = PlatformDirs( app_name ).user_data_path
    app_root.mkdir( parents=True, exist_ok=True )
    snapshot_download(
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ 'cells/*' ],
        token          = token,
    )
#/def pull_commons_from_hub

# -- Registration and listing

def list_installed(
    app_name: str,
    maps_index_file: Path = MAPS_INDEX_FILE_DEFAULT,
) -> list[ str ]:
    """List bundle IDs registered in the user-global maps index."""
    global_index = PlatformDirs( app_name ).user_data_path / maps_index_file
    if not global_index.exists():
        return []
    #
    return list( _read_maps_index( global_index ).keys() )
#/def list_installed
