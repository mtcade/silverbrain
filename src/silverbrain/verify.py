#
#//  verify.py
#//  silverbrain
#//
#//  Created by Evan Mason on 6/22/26.
#
"""
Pure-TOML consistency checker for cell and composite web TOML files.
Requires no ops_module and no live execution.
"""

import tomllib
from dataclasses import dataclass
from pathlib import Path

from .web import _apply_template_vars

_BUILTIN_OP_IDNS = frozenset( { 'compute_op_bindings' } )
_CHECK_TYPE      = 'TableCheckRef'


@dataclass
class VerifyIssue:
    path:  Path
    issue: str

    def __str__( self ) -> str:
        return f"{self.path}: {self.issue}"


def verify_toml(
    path:          Path,
    template_vars: dict[ str, str ] | None = None,
) -> list[ VerifyIssue ]:
    """
    Load a cell or composite TOML at *path* and return a list of structural issues.

    Checks performed:
    - Cell: no duplicate [[processes]] op_idns.
    - Composite: node toml paths exist; [[nodes.ops]] op_idns exist in child's
      processes; no duplicate exposed names; [[processes]] terms reference known
      exposed ops or other [[processes]].
    """
    path   = Path( path ).resolve()
    tvars  = template_vars or {}
    issues: list[ VerifyIssue ] = []

    with open( path, 'rb' ) as f:
        data = tomllib.load( f )

    if 'cell' in data:
        _check_cell( path, data, issues )
    elif 'composite' in data:
        _check_composite( path, data, tvars, issues )
    else:
        issues.append( VerifyIssue( path, 'TOML has neither [cell] nor [composite] section' ) )

    return issues


def _cell_op_idns( data: dict ) -> set[ str ]:
    """Domain process op_idns for a cell TOML (excludes TableCheckRef and builtins)."""
    return {
        p[ 'op_idn' ]
        for p in data.get( 'processes', [] )
        if p.get( 'type' ) != _CHECK_TYPE
        and p[ 'op_idn' ] not in _BUILTIN_OP_IDNS
    }


def _composite_exposed_op_idns( data: dict ) -> set[ str ]:
    """
    Op_idns a composite web exposes to its parent.
    If [[processes]] entries exist, those are the interface.
    Otherwise the composite exposes nothing (the parent uses [[nodes.ops]] for routing).
    """
    procs = data.get( 'processes', [] )
    return { p[ 'op_idn' ] for p in procs }


def _check_cell(
    path:   Path,
    data:   dict,
    issues: list[ VerifyIssue ],
) -> None:
    seen: set[ str ] = set()
    for proc in data.get( 'processes', [] ):
        op = proc.get( 'op_idn' )
        if not op:
            issues.append( VerifyIssue( path, "[[processes]] entry missing op_idn" ) )
            continue
        if op in seen:
            issues.append( VerifyIssue( path, f"Duplicate [[processes]] op_idn {op!r}" ) )
        seen.add( op )


def _check_composite(
    path:   Path,
    data:   dict,
    tvars:  dict[ str, str ],
    issues: list[ VerifyIssue ],
) -> None:
    base_dir     = path.parent
    exposed: dict[ str, str ] = {}  # exposed_name → node_id

    for node_desc in data.get( 'nodes', [] ):
        node_id  = node_desc.get( 'node_id', '?' )
        raw_toml = node_desc.get( 'toml' )
        if not raw_toml:
            issues.append( VerifyIssue( path, f"Node {node_id!r} missing 'toml' field" ) )
            continue

        child_path = ( base_dir / _apply_template_vars( raw_toml, tvars ) ).resolve()
        if not child_path.exists():
            issues.append( VerifyIssue(
                path, f"Node {node_id!r} toml path not found: {child_path}"
            ) )
            continue

        with open( child_path, 'rb' ) as f:
            child_data = tomllib.load( f )

        is_composite = 'composite' in child_data
        child_ops    = (
            _composite_exposed_op_idns( child_data )
            if is_composite
            else _cell_op_idns( child_data )
        )

        for op in node_desc.get( 'ops', [] ):
            op_idn  = op.get( 'op_idn' )
            alias   = op.get( 'alias_idn' )
            exposed_name = alias if alias else op_idn

            if not op_idn:
                issues.append( VerifyIssue(
                    path, f"Node {node_id!r} [[nodes.ops]] entry missing op_idn"
                ) )
                continue

            if child_ops and op_idn not in child_ops:
                kind = 'subweb' if is_composite else 'cell'
                issues.append( VerifyIssue(
                    path,
                    f"Node {node_id!r} op_idn {op_idn!r} not found in {kind} "
                    f"{child_path.name!r}; available: {sorted( child_ops )}"
                ) )

            if exposed_name in exposed:
                issues.append( VerifyIssue(
                    path,
                    f"Duplicate exposed op name {exposed_name!r}: "
                    f"nodes {exposed[ exposed_name ]!r} and {node_id!r}"
                ) )
            else:
                exposed[ exposed_name ] = node_id

    proc_op_idns = { p[ 'op_idn' ] for p in data.get( 'processes', [] ) }
    valid_terms  = set( exposed ) | proc_op_idns

    for proc in data.get( 'processes', [] ):
        all_terms = (
            proc.get( 'terms', [] )
            + proc.get( 'ifs', [] )
            + proc.get( 'thens', [] )
        )
        for term in all_terms:
            if term not in valid_terms:
                issues.append( VerifyIssue(
                    path,
                    f"Web process {proc[ 'op_idn' ]!r} term {term!r} "
                    f"not in exposed node ops or [[processes]]"
                ) )
        otherwise = proc.get( 'otherwise' )
        if otherwise and otherwise not in valid_terms:
            issues.append( VerifyIssue(
                path,
                f"Web process {proc[ 'op_idn' ]!r} otherwise {otherwise!r} "
                f"not in exposed node ops or [[processes]]"
            ) )
