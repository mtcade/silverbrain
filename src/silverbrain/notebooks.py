import os
import subprocess
from pathlib import Path

from .bundle import BundleDirs


def launch(
    bundle_id:     str,
    notebook_name: str,
    app_name:      str = 'silverknockoff',
) -> None:
    """Resolve a notebook path from a bundle and open it with marimo edit."""
    dirs = BundleDirs( bundle_id, app_name )
    nb   = dirs.notebooks / f'{notebook_name}.py'
    if not nb.exists():
        raise FileNotFoundError(
            f"Notebook {notebook_name!r} not found in {dirs.notebooks}\n"
            f"Available: {[p.stem for p in dirs.notebooks.glob('*.py')]}"
        )
    os.execvp( 'marimo', [ 'marimo', 'edit', str( nb ) ] )


def create(
    bundle_id:     str,
    notebook_name: str,
    app_name:      str        = 'silverknockoff',
    template:      Path | None = None,
) -> Path:
    """
    Create a new Marimo notebook in the bundle's notebooks/ dir and open it.
    If template is given, copies it; otherwise creates a minimal stub.
    """
    dirs = BundleDirs( bundle_id, app_name )
    dirs.notebooks.mkdir( parents=True, exist_ok=True )

    nb = dirs.notebooks / f'{notebook_name}.py'
    if nb.exists():
        raise FileExistsError( f"Notebook already exists: {nb}" )

    if template is not None:
        import shutil
        shutil.copy2( template, nb )
    else:
        nb.write_text(
            f'import marimo\n\n'
            f'__generated_with = marimo.__version__\n'
            f'app = marimo.App()\n\n\n'
            f'@app.cell\n'
            f'def _():\n'
            f'    from silverbrain.bundle import web_from_bundle\n'
            f'    web = web_from_bundle({bundle_id!r})\n'
            f'    return (web,)\n\n\n'
            f'if __name__ == "__main__":\n'
            f'    app.run()\n'
        )

    os.execvp( 'marimo', [ 'marimo', 'edit', str( nb ) ] )
    return nb
