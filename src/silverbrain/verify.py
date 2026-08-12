#
#//  verify.py
#//  silverbrain
#//

"""
Pure-TOML consistency checker for cell TOML files, including nested
kind='cell' [[nodes]] entries. Requires no op_classes and no live execution.
"""

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class VerifyIssue:
    path:  Path
    issue: str

    def __str__( self ) -> str:
        return f"{self.path}: {self.issue}"
    #/def __str__
#class VerifyIssue

def verify_toml( path: Path ) -> list[ VerifyIssue ]:
    """
    Load a cell TOML at *path* and return a list of structural issues,
    recursing into any kind='cell' [[nodes]] entries.

    Checks performed:
    - No duplicate node_idn across [[nodes]] (node_idn is a tableOps dict
      key, so must stay unique within one Cell -- unlike source/target
      names, which may legitimately repeat; see
      squishy-sleeping-snowflake.md).
    - Each [[nodes]] entry has a node_idn and a recognized kind
      ('op'/'cell'/'variable').
    - Each kind='cell' entry has a 'toml' path that exists; its own TOML is
      recursively verified.
    - Each kind='variable' entry has a 'columns' table.
    """
    path   = Path( path ).resolve()
    issues: list[ VerifyIssue ] = []

    with open( path, 'rb' ) as f:
        data = tomllib.load( f )

    if 'cell' not in data:
        issues.append( VerifyIssue( path, "TOML has no [cell] section" ) )
        return issues

    _check_cell( path, data, issues )
    return issues


def _check_cell(
    path:   Path,
    data:   dict,
    issues: list[ VerifyIssue ],
) -> None:
    seen: set[ str ] = set()
    base_dir = path.parent

    for entry in data.get( 'nodes', [] ):
        node_idn = entry.get( 'node_idn' )
        kind     = entry.get( 'kind' )

        if not node_idn:
            issues.append( VerifyIssue( path, "[[nodes]] entry missing node_idn" ) )
        elif node_idn in seen:
            issues.append( VerifyIssue( path, f"Duplicate node_idn {node_idn!r}" ) )
        else:
            seen.add( node_idn )

        if kind not in ( 'op', 'cell', 'variable' ):
            issues.append( VerifyIssue(
                path, f"[[nodes]] entry {node_idn!r} has unknown kind {kind!r} -- "
                      f"expected 'op', 'cell', or 'variable'"
            ) )
            continue

        if kind == 'variable':
            if not entry.get( 'columns' ):
                issues.append( VerifyIssue(
                    path, f"[[nodes]] entry {node_idn!r} (kind='variable') missing 'columns'"
                ) )
            continue

        if kind == 'op':
            continue

        # kind == 'cell'
        raw_toml = entry.get( 'toml' )
        if not raw_toml:
            issues.append( VerifyIssue( path, f"Child {node_idn!r} missing 'toml' field" ) )
            continue

        child_path = ( base_dir / raw_toml ).resolve()
        if not child_path.exists():
            issues.append( VerifyIssue(
                path, f"Child {node_idn!r} toml path not found: {child_path}"
            ) )
            continue

        with open( child_path, 'rb' ) as f:
            child_data = tomllib.load( f )

        if 'cell' not in child_data:
            issues.append( VerifyIssue( child_path, "TOML has no [cell] section" ) )
            continue

        _check_cell( child_path, child_data, issues )
