#
#//  test_bundle.py
#//  silverbrain
#//

"""
Tests for BundleDirs' cell_idn -> path resolution via cells_index.toml
(hand-maintained idn->path index, replacing filename-guessing -- see
squishy-sleeping-snowflake.md) and _default_cell_idn's bundle.toml schema.
"""

from pathlib import Path

import pytest

from silverbrain.bundle import BundleDirs, _default_cell_idn


def _make_dirs( tmp_path: Path, bundle_id: str = 'b' ) -> BundleDirs:
    dirs = BundleDirs( bundle_id, 'test_app' )
    dirs._app_root = tmp_path
    return dirs


def test_register_cell_then_cell_path_round_trips( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    target = tmp_path / 'somewhere' / 'weird_name.cell.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )

    dirs.register_cell( 'root', target )

    assert dirs.cell_path( 'root' ) == target.resolve()


def test_cell_path_falls_back_to_common_index( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    common_target = tmp_path / 'shared.toml'
    common_target.write_text( '[cell]\n' )

    dirs.register_cell( 'shared_cell', common_target, common=True )

    assert dirs.cell_path( 'shared_cell' ) == common_target.resolve()


def test_bundle_specific_overrides_common_for_same_cell_idn( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    bundle_target = tmp_path / 'bundle_version.toml'
    common_target = tmp_path / 'common_version.toml'
    bundle_target.write_text( '[cell]\n' )
    common_target.write_text( '[cell]\n' )

    dirs.register_cell( 'shared', common_target, common=True )
    dirs.register_cell( 'shared', bundle_target )

    assert dirs.cell_path( 'shared' ) == bundle_target.resolve()


def test_cell_path_raises_when_cell_idn_unregistered( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    with pytest.raises( FileNotFoundError, match="cell_idn='missing'" ):
        dirs.cell_path( 'missing' )


def test_cells_root_is_bundle_specific_iff_index_has_entries( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    assert dirs._cells_root() == dirs.common_cells

    target = tmp_path / 'x.toml'
    target.write_text( '[cell]\n' )
    dirs.register_cell( 'x', target )

    assert dirs._cells_root() == dirs.cells


def test_default_cell_idn_reads_bundle_toml_key( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    dirs.root.mkdir( parents=True )
    ( dirs.root / 'bundle.toml' ).write_text(
        '[bundle]\ndefault_cell_idn = "root"\n'
    )

    assert _default_cell_idn( dirs, 'b', Path( 'bundle.toml' ) ) == 'root'


def test_default_cell_idn_raises_when_key_missing( tmp_path: Path ) -> None:
    dirs = _make_dirs( tmp_path )
    dirs.root.mkdir( parents=True )
    ( dirs.root / 'bundle.toml' ).write_text( '[bundle]\n' )

    with pytest.raises( ValueError, match="default_cell_idn" ):
        _default_cell_idn( dirs, 'b', Path( 'bundle.toml' ) )


# -- maps_index.toml (cell_idn -> cell map file), resolve_map_fp/cell_from_map/register_bundle


def _write_maps_index_fixture( fp: Path, cell_idn: str, target_path: Path ) -> None:
    fp.parent.mkdir( parents=True, exist_ok=True )
    fp.write_text( f'[[maps]]\ncell_idn = {cell_idn!r}\npath = {str(target_path)!r}\n' )


def test_resolve_map_fp_finds_cell_idn_in_fallback_data_root( tmp_path: Path ) -> None:
    from silverbrain.bundle import resolve_map_fp

    data_root = tmp_path / 'data_root'
    target    = tmp_path / 'somewhere' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )
    _write_maps_index_fixture( data_root / 'maps_index.toml', 'my_cell', target )

    resolved = resolve_map_fp(
        'my_cell', app_name='nonexistent_test_app_xyz',
        fallback_data_roots={ 'data_root': data_root },
    )
    assert resolved == target.resolve()


def test_resolve_map_fp_raises_when_cell_idn_not_found( tmp_path: Path ) -> None:
    from silverbrain.bundle import resolve_map_fp

    with pytest.raises( ValueError, match="cell_idn='missing'" ):
        resolve_map_fp(
            'missing', app_name='nonexistent_test_app_xyz',
            fallback_data_roots={ 'data_root': tmp_path },
        )


def test_cell_from_map_builds_cell_via_cell_idn( tmp_path: Path ) -> None:
    from silverbrain.bundle import cell_from_map

    data_root = tmp_path / 'data_root'
    target    = tmp_path / 'cells' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\nsource = ["inp"]\ntarget = ["out"]\n' )
    _write_maps_index_fixture( data_root / 'maps_index.toml', 'my_cell', target )

    cell = cell_from_map(
        'my_cell', app_name='nonexistent_test_app_xyz', op_classes={},
        fallback_data_roots={ 'data_root': data_root },
    )
    assert cell.main_id == 'my_cell'


def test_register_bundle_then_resolve_map_fp_round_trips( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    target = tmp_path / 'somewhere' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )

    bundle_module.register_bundle( 'my_cell', target, app_name='nonexistent_test_app_xyz' )

    resolved = bundle_module.resolve_map_fp( 'my_cell', app_name='nonexistent_test_app_xyz' )
    assert resolved == target.resolve()
