import os
import tarfile
import tomllib
from pathlib import Path

from platformdirs import PlatformDirs

from .maps import register_bundle


# -- Directory layout

class BundleDirs:
    """
    Resolves platformdirs paths for a named bundle.

    Layout under user_data_path/<app_name>/:
        cells/                  — common/shared cell TOMLs
        webs/                   — common/shared web map TOMLs
        bundles/<bundle_id>/
            bundle.toml
            cells/              — bundle-specific (overrides common)
            webs/               — bundle-specific web TOMLs
            settings/           — parameter JSON files, paths.json
            data/               — parquet outputs
            notebooks/          — Marimo .py notebooks
    """

    def __init__( self, bundle_id: str, app_name: str = 'silverknockoff' ) -> None:
        self._bundle_id = bundle_id
        self._app_root  = PlatformDirs( app_name ).user_data_path

    @property
    def root( self ) -> Path:
        return self._app_root / 'bundles' / self._bundle_id

    @property
    def webs( self ) -> Path:
        return self.root / 'webs'

    @property
    def cells( self ) -> Path:
        return self.root / 'cells'

    @property
    def settings( self ) -> Path:
        return self.root / 'settings'

    @property
    def data( self ) -> Path:
        return self.root / 'data'

    @property
    def notebooks( self ) -> Path:
        return self.root / 'notebooks'

    @property
    def common_webs( self ) -> Path:
        return self._app_root / 'webs'

    @property
    def common_cells( self ) -> Path:
        return self._app_root / 'cells'

    def cell_path( self, name: str ) -> Path:
        """Bundle-specific cells/ first, then common cells/ fallback."""
        bundle_p = self.cells / f'{name}.toml'
        if bundle_p.exists():
            return bundle_p
        common_p = self.common_cells / f'{name}.toml'
        if common_p.exists():
            return common_p
        raise FileNotFoundError(
            f"Cell {name!r} not found in {self.cells} or {self.common_cells}"
        )

    def web_path( self, name: str ) -> Path:
        """Bundle-specific webs/ first, then common webs/ fallback."""
        bundle_p = self.webs / f'{name}.toml'
        if bundle_p.exists():
            return bundle_p
        common_p = self.common_webs / f'{name}.toml'
        if common_p.exists():
            return common_p
        raise FileNotFoundError(
            f"Web {name!r} not found in {self.webs} or {self.common_webs}"
        )

    def _cells_root( self ) -> Path:
        """The directory to expose as {cells_root}: bundle-specific if non-empty, else common."""
        if self.cells.exists() and any( self.cells.glob( '*.toml' ) ):
            return self.cells
        return self.common_cells

    def _webs_root( self ) -> Path:
        """The directory to expose as {webs_root}: bundle-specific if non-empty, else common."""
        if self.webs.exists() and any( self.webs.glob( '*.toml' ) ):
            return self.webs
        return self.common_webs

    def ensure_dirs( self ) -> None:
        for d in ( self.webs, self.cells, self.settings, self.data, self.notebooks ):
            d.mkdir( parents=True, exist_ok=True )
        self.common_cells.mkdir( parents=True, exist_ok=True )
        self.common_webs.mkdir( parents=True, exist_ok=True )


# -- Loading a web from a bundle

def web_from_bundle(
    bundle_id: str,
    app_name:  str = 'silverknockoff',
    **kwargs,
):
    """Load a Web from an installed bundle by id, resolving {cells_root}/{webs_root}."""
    from .web import web_from_toml

    dirs  = BundleDirs( bundle_id, app_name )
    toml  = dirs.web_path( bundle_id )
    template_vars = {
        'cells_root':   str( dirs._cells_root() ),
        'webs_root':    str( dirs._webs_root() ),
        'bundle_root':  str( dirs.root ),
        'app_bundles':  str( dirs._app_root / 'bundles' ),
    }
    return web_from_toml( path=toml, template_vars=template_vars, **kwargs )


# -- Packing

def pack(
    bundle_id:   str,
    output_path: Path,
    app_name:    str = 'silverknockoff',
) -> Path:
    """Archive a bundle dir to a .tar.gz file."""
    dirs        = BundleDirs( bundle_id, app_name )
    output_path = Path( output_path )
    with tarfile.open( output_path, 'w:gz' ) as tar:
        tar.add( dirs.root, arcname=bundle_id )
    return output_path


# -- Hugging Face Hub push/pull

def push_to_hub(
    bundle_id:  str,
    hf_repo_id: str,
    app_name:   str       = 'silverknockoff',
    token:      str | None = None,
) -> None:
    """Upload bundle dir (webs + cells + settings + data + notebooks) to HF Hub."""
    from huggingface_hub import upload_folder

    dirs = BundleDirs( bundle_id, app_name )
    upload_folder(
        folder_path = str( dirs.root ),
        repo_id     = hf_repo_id,
        repo_type   = 'dataset',
        token       = token,
    )


def pull_from_hub(
    hf_repo_id: str,
    bundle_id:  str,
    app_name:   str       = 'silverknockoff',
    token:      str | None = None,
) -> 'BundleDirs':
    """
    Download bundle from HF Hub into platformdirs bundles location.
    Reads required_cells/required_webs from bundle.toml and fetches
    missing common items from the configured commons repo.
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

    _fetch_missing_commons( dirs, app_name, token )
    register_bundle( bundle_id, dirs.web_path( bundle_id ), app_name )
    return dirs


def _fetch_missing_commons(
    dirs:     BundleDirs,
    app_name: str,
    token:    str | None,
) -> None:
    """Pull missing required cells/webs from the commons HF repo."""
    bundle_toml = dirs.root / 'bundle.toml'
    if not bundle_toml.exists():
        return

    with open( bundle_toml, 'rb' ) as f:
        meta = tomllib.load( f ).get( 'bundle', {} )

    commons_repo = os.environ.get(
        f'{app_name.upper()}_COMMONS_REPO',
        'BigBrainStuff/sk-commons',
    )

    required_cells: list[ str ] = meta.get( 'required_cells', [] )
    required_webs:  list[ str ] = meta.get( 'required_webs', [] )

    missing_cells = [
        c for c in required_cells
        if not ( dirs.common_cells / f'{c}.toml' ).exists()
        and not ( dirs.cells / f'{c}.toml' ).exists()
    ]
    missing_webs = [
        w for w in required_webs
        if not ( dirs.common_webs / f'{w}.toml' ).exists()
        and not ( dirs.webs / f'{w}.toml' ).exists()
    ]

    if not missing_cells and not missing_webs:
        return

    from huggingface_hub import snapshot_download

    app_root = dirs._app_root
    patterns = (
        [ f'cells/{c}.toml' for c in missing_cells ]
        + [ f'webs/{w}.toml' for w in missing_webs ]
    )
    snapshot_download(
        repo_id        = commons_repo,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = patterns,
        token          = token,
    )


def push_commons_to_hub(
    hf_repo_id: str,
    app_name:   str       = 'silverknockoff',
    token:      str | None = None,
) -> None:
    """Upload common cells/ and webs/ to a single HF Hub dataset repo."""
    from huggingface_hub import upload_folder

    app_root = PlatformDirs( app_name ).user_data_path
    upload_folder(
        folder_path    = str( app_root ),
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        allow_patterns = [ 'cells/*', 'webs/*' ],
        token          = token,
    )


def pull_commons_from_hub(
    hf_repo_id: str,
    app_name:   str       = 'silverknockoff',
    token:      str | None = None,
) -> None:
    """Download common cells/ and webs/ from HF Hub into platformdirs."""
    from huggingface_hub import snapshot_download

    app_root = PlatformDirs( app_name ).user_data_path
    app_root.mkdir( parents=True, exist_ok=True )
    snapshot_download(
        repo_id        = hf_repo_id,
        repo_type      = 'dataset',
        local_dir      = str( app_root ),
        allow_patterns = [ 'cells/*', 'webs/*' ],
        token          = token,
    )


# -- Registration and listing

def list_installed( app_name: str = 'silverknockoff' ) -> list[ str ]:
    """List bundle IDs registered in the user-global maps_index.toml."""
    from .maps import _read_maps_index

    global_index = PlatformDirs( app_name ).user_data_path / 'maps_index.toml'
    if not global_index.exists():
        return []
    return list( _read_maps_index( global_index ).keys() )
