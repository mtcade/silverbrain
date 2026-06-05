import tomllib
from pathlib import Path

from platformdirs import PlatformDirs


def _read_maps_index( fp: Path ) -> dict[ str, Path ]:
    """Return {main_id: absolute_toml_path} from a maps_index.toml file."""
    with open( fp, 'rb' ) as f:
        data = tomllib.load( f )
    result: dict[ str, Path ] = {}
    base = fp.parent
    for entry in data.get( 'maps', [] ):
        result[ entry[ 'main_id' ] ] = ( base / entry[ 'path' ] ).resolve()
    return result


def resolve_map_fp(
    main_id:             str,
    app_name:            str        = 'silverknockoff',
    local_maps_index_fp: Path | None = None,
) -> Path:
    """
    Find the TOML map file for a given main_id.

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

    # 2. user-global
    global_index = PlatformDirs( app_name ).user_data_path / 'maps_index.toml'
    if global_index.exists():
        global_maps = _read_maps_index( global_index )
        if main_id in global_maps:
            return global_maps[ main_id ]

    raise ValueError(
        f"main_id={main_id!r} not found in"
        + ( f" {local_maps_index_fp!r} or" if local_maps_index_fp else "" )
        + f" {global_index!r}"
    )


def register_bundle(
    bundle_id:   str,
    toml_path:   Path,
    app_name:    str = 'silverknockoff',
) -> None:
    """Add or update a bundle entry in the user-global maps_index.toml."""
    global_index = PlatformDirs( app_name ).user_data_path / 'maps_index.toml'
    global_index.parent.mkdir( parents=True, exist_ok=True )

    existing: dict[ str, Path ] = {}
    if global_index.exists():
        existing = _read_maps_index( global_index )

    existing[ bundle_id ] = Path( toml_path ).resolve()
    _write_maps_index( global_index, existing )


def _write_maps_index( fp: Path, maps: dict[ str, Path ] ) -> None:
    lines = []
    for main_id, toml_path in maps.items():
        lines.append( '[[maps]]' )
        lines.append( f'main_id = {main_id!r}' )
        lines.append( f'path    = {str( toml_path )!r}' )
        lines.append( '' )
    fp.write_text( '\n'.join( lines ) )
