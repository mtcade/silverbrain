#
#//  test_bundle.py
#//  silverbrain
#//

"""
Tests for the bundle/cell separation: cells_index.toml (cell_idn -> file
location, global + fallback_data_roots search, no bundle semantics) and
bundle_index.toml (bundle_name -> {bundle_root, cell_idn}, global only) --
see bundle.py's module-level comments for the full design.
"""

from pathlib import Path

import pytest

from silverbrain.bundle import BundleDirs, _default_cell_idn


def _make_dirs( tmp_path: Path, bundle_name: str = 'b' ) -> BundleDirs:
    dirs = BundleDirs( bundle_name, 'test_app' )
    dirs._app_root = tmp_path
    return dirs


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


# -- cells_index.toml (cell_idn -> file location) / resolve_cell_fp / register_cell


def _write_cells_index_fixture( fp: Path, cell_idn: str, target_path: Path ) -> None:
    fp.parent.mkdir( parents=True, exist_ok=True )
    fp.write_text( f'[[cells]]\ncell_idn = {cell_idn!r}\ntoml = {str(target_path)!r}\n' )


def test_resolve_cell_fp_finds_cell_idn_in_fallback_data_root( tmp_path: Path ) -> None:
    from silverbrain.bundle import resolve_cell_fp

    data_root = tmp_path / 'data_root'
    target    = tmp_path / 'somewhere' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )
    _write_cells_index_fixture( data_root / 'cells_index.toml', 'my_cell', target )

    resolved = resolve_cell_fp(
        'my_cell', app_name='nonexistent_test_app_xyz',
        fallback_data_roots={ 'data_root': data_root },
    )
    assert resolved == target.resolve()


def test_resolve_cell_fp_raises_when_cell_idn_not_found( tmp_path: Path ) -> None:
    from silverbrain.bundle import resolve_cell_fp

    with pytest.raises( ValueError, match="cell_idn='missing'" ):
        resolve_cell_fp(
            'missing', app_name='nonexistent_test_app_xyz',
            fallback_data_roots={ 'data_root': tmp_path },
        )


def test_register_cell_then_resolve_cell_fp_round_trips( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    target = tmp_path / 'somewhere' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )

    bundle_module.register_cell( 'my_cell', target, app_name='nonexistent_test_app_xyz' )

    resolved = bundle_module.resolve_cell_fp( 'my_cell', app_name='nonexistent_test_app_xyz' )
    assert resolved == target.resolve()


def test_cell_from_bundle_builds_cell_via_bundle_name( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    target = tmp_path / 'cells' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\nsource = ["inp"]\ntarget = ["out"]\n' )

    bundle_module.register_cell( 'my_cell', target, app_name='nonexistent_test_app_xyz' )
    bundle_module.register_bundle( 'my_bundle', 'my_cell', app_name='nonexistent_test_app_xyz' )

    dirs = bundle_module.BundleDirs( 'my_bundle', 'nonexistent_test_app_xyz' )
    dirs.root.mkdir( parents=True )
    ( dirs.root / 'bundle.toml' ).write_text( '[cell]\ncell_idn = "my_cell"\n' )

    cell = bundle_module.cell_from_bundle(
        'my_bundle', app_name='nonexistent_test_app_xyz', op_classes={},
    )
    assert cell.main_id == 'my_cell'


def test_cell_from_bundle_raises_when_bundle_toml_missing( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    target = tmp_path / 'cells' / 'root.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )

    bundle_module.register_cell( 'my_cell', target, app_name='nonexistent_test_app_xyz' )
    bundle_module.register_bundle( 'my_bundle', 'my_cell', app_name='nonexistent_test_app_xyz' )

    with pytest.raises( FileNotFoundError, match="bundle.toml" ):
        bundle_module.cell_from_bundle(
            'my_bundle', app_name='nonexistent_test_app_xyz', op_classes={},
        )


def test_cell_from_bundle_applies_bundle_toml_resources_and_grafts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
    """bundle.toml's own [[variables.resources]]/[[variables.actors]] reach
    into the linked cell's subtree exactly like an in-tree declaration
    would -- see bundle.py's cell_from_bundle docstring."""
    from silverbrain import bundle as bundle_module
    from silverbrain.tableOps import PlaceholderOp

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )

    cells_dir = tmp_path / 'cells'
    cells_dir.mkdir( parents=True )
    ( cells_dir / 'root.toml' ).write_text(
        '[cell]\n'
        'source = ["_trigger"]\n'
        'target = ["out"]\n'
        '\n'
        '[[nodes]]\n'
        'node_idn = "picker"\n'
        'kind = "variable"\n'
        'source = ["_trigger"]\n'
        'target = ["out"]\n'
        'columns = { a = "Int64" }\n'
    )
    bundle_module.register_cell( 'my_cell', cells_dir / 'root.toml', app_name='nonexistent_test_app_xyz' )
    bundle_module.register_bundle( 'my_bundle', 'my_cell', app_name='nonexistent_test_app_xyz' )

    dirs = bundle_module.BundleDirs( 'my_bundle', 'nonexistent_test_app_xyz' )
    dirs.root.mkdir( parents=True )
    ( dirs.root / 'bundle.toml' ).write_text(
        '[cell]\n'
        'cell_idn = "my_cell"\n'
        '\n'
        '[[variables.actors]]\n'
        'target = "picker"\n'
        'op_type = "identity_op"\n'
    )

    import polars as pl

    class _IdentityOp( PlaceholderOp ):
        def __init__( self, rng=None ):
            super().__init__( columns = { 'a': pl.Int64 }, rng = rng )

    cell = bundle_module.cell_from_bundle(
        'my_bundle', app_name='nonexistent_test_app_xyz',
        op_classes={ 'identity_op': _IdentityOp },
    )
    picker_actor = cell.tableOps[ 'picker' ]
    assert isinstance( picker_actor, _IdentityOp )


# -- bundle_index.toml (bundle_name -> {bundle_root, cell_idn}) / resolve_bundle / register_bundle


def test_resolve_bundle_raises_when_not_registered( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    with pytest.raises( ValueError, match="bundle_name='missing'" ):
        bundle_module.resolve_bundle( 'missing', app_name='nonexistent_test_app_xyz' )


def test_register_bundle_then_resolve_bundle_falls_back_to_convention_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    bundle_module.register_bundle( 'my_bundle', 'my_cell', app_name='nonexistent_test_app_xyz' )

    entry = bundle_module.resolve_bundle( 'my_bundle', app_name='nonexistent_test_app_xyz' )
    assert entry.cell_idn == 'my_cell'
    assert entry.bundle_root == bundle_module.BundleDirs( 'my_bundle', 'nonexistent_test_app_xyz' ).root


def test_register_bundle_with_explicit_bundle_root( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    custom_root = tmp_path / 'somewhere_else'
    custom_root.mkdir()
    bundle_module.register_bundle(
        'my_bundle', 'my_cell', app_name='nonexistent_test_app_xyz', bundle_root=custom_root,
    )

    entry = bundle_module.resolve_bundle( 'my_bundle', app_name='nonexistent_test_app_xyz' )
    assert entry.bundle_root == custom_root.resolve()


def test_list_installed_lists_registered_bundle_names( tmp_path: Path, monkeypatch: pytest.MonkeyPatch ) -> None:
    from silverbrain import bundle as bundle_module

    monkeypatch.setattr(
        bundle_module, 'PlatformDirs',
        lambda app_name: type( 'P', (), { 'user_data_path': tmp_path } )(),
    )
    assert bundle_module.list_installed( 'nonexistent_test_app_xyz' ) == []

    bundle_module.register_bundle( 'a', 'a_cell', app_name='nonexistent_test_app_xyz' )
    bundle_module.register_bundle( 'b', 'b_cell', app_name='nonexistent_test_app_xyz' )
    assert set( bundle_module.list_installed( 'nonexistent_test_app_xyz' ) ) == { 'a', 'b' }


# -- Relative-path round-trip (regression test for the previous absolute-path bug)


def test_written_index_paths_are_relative_and_survive_a_move( tmp_path: Path ) -> None:
    from silverbrain.bundle import _read_cells_index, _write_cells_index

    original_root = tmp_path / 'original'
    original_root.mkdir()
    target = original_root / 'sub' / 'cell.toml'
    target.parent.mkdir( parents=True )
    target.write_text( '[cell]\n' )

    index_fp = original_root / 'cells_index.toml'
    _write_cells_index( index_fp, { 'my_cell': target } )
    assert not Path( index_fp.read_text().split( "toml = " )[ 1 ].split( "\n" )[ 0 ].strip( "'\"" ) ).is_absolute()

    # Move the whole directory tree -- a relatively-stored path must still resolve.
    moved_root = tmp_path / 'moved'
    original_root.rename( moved_root )
    moved_index_fp = moved_root / 'cells_index.toml'

    resolved = _read_cells_index( moved_index_fp )
    assert resolved[ 'my_cell' ] == ( moved_root / 'sub' / 'cell.toml' ).resolve()
