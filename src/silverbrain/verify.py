#
#//  verify.py
#//  silverbrain
#//

"""
Pure-TOML consistency checker for cell TOML files, including nested
[[children]]. Requires no op_classes and no live execution.
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


def verify_toml( path: Path ) -> list[ VerifyIssue ]:
    """
    Load a cell TOML at *path* and return a list of structural issues,
    recursing into any [[children]].

    Checks performed:
    - No duplicate node_idn across [[processes]] and [[children]] (node_idn
      is a tableOps dict key, so must stay unique within one Cell -- unlike
      source/target names, which may legitimately repeat; see
      squishy-sleeping-snowflake.md).
    - Each [[children]] entry has a node_idn and a toml path that exists.
    - Each child's own TOML is recursively verified.
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

    for proc in data.get( 'processes', [] ):
        node_idn = proc.get( 'node_idn' )
        if not node_idn:
            issues.append( VerifyIssue( path, "[[processes]] entry missing node_idn" ) )
            continue
        if node_idn in seen:
            issues.append( VerifyIssue( path, f"Duplicate node_idn {node_idn!r}" ) )
        seen.add( node_idn )

    base_dir = path.parent
    for child_desc in data.get( 'children', [] ):
        node_idn = child_desc.get( 'node_idn' )
        raw_toml = child_desc.get( 'toml' )

        if not node_idn:
            issues.append( VerifyIssue( path, "[[children]] entry missing node_idn" ) )
        elif node_idn in seen:
            issues.append( VerifyIssue( path, f"Duplicate node_idn {node_idn!r}" ) )
        else:
            seen.add( node_idn )

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
