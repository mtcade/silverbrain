#
#//  exprParse.py
#//  silverbrain
#//
#//  Created by Evan Mason on 8/6/26.
#//
"""
A human-writable text grammar for bdd.Expr, parsed with pyparsing -- lets a
cell map (configTree.py) declare a placeholder's interface as a real
set-expression (with ColVar) instead of only a flat {name: dtype} dict.

GRAMMAR

    expr := Union( expr, ... )
          | Intersect( expr, ... )
          | Difference( expr, expr )
          | Cond( expr, expr, expr )
          | ColVar( 'name' )
          | Atom( 'name' [, dtype_tok] )
          | 'name'                    -- sugar for Atom('name'), dtype inferred

    dtype_tok := '?'                  -- explicit inference sentinel
               | <dtype string>       -- whatever polarsDataTypeStrings.dtype_to_str
                                         produces (e.g. Int64, List(String),
                                         ipc:<base64>) -- captured whole and handed
                                         to str_to_dtype, never re-parsed by hand

Keywords (Union/Intersect/Difference/Cond/Atom/ColVar) mirror bdd.py's Expr
class names directly, so the text is a near-literal transcription of the
Python constructor calls.

DTYPE INFERENCE

Unlike bdd.py's Python API (where every Atom's dtype is always required and
explicit), this text format lets an atom's dtype be omitted three
equivalent ways: a bare 'name' (no Atom(...) wrapper at all), Atom('name')
(one arg), or Atom('name', ?) (explicit sentinel -- for a generator that
always emits the 2-arg form uniformly and needs to say "infer" without
varying its own arity). In every omitted case, parse_expr() resolves the
dtype via `dtype_lookup` (defaults to schema.cannonical_schema): the given
dtype if present and not '?', else dtype_lookup[name], else ValueError
naming the atom -- neither the map nor dtype_lookup said what it is. Once
parsed, every bdd.Atom in the result has a concrete dtype, same invariant
as everywhere else in bdd.py -- inference is a parse-time-only concept,
never a runtime "maybe-unknown" state inside bdd.py itself.

expr_to_text() is the reverse direction and is NOT lenient back: it always
emits explicit dtypes (Atom('name', Int64), never '?' or bare-name sugar)
-- a faithful, unambiguous, round-trippable serialization. The parser's
leniency is an authoring convenience; the serializer doesn't need it.
"""

from . import bdd
from .polarsDataTypeStrings import dtype_to_str, str_to_dtype
from .schema import cannonical_schema

import pyparsing as pp

import polars as pl

from dataclasses import dataclass
from typing import Self

# -- Intermediate (unresolved) AST -- parsing produces this, _resolve() (with
# a dtype_lookup) turns it into a real bdd.Expr. ColVar needs no resolution
# (no dtype), so parsing builds real bdd.ColVar nodes directly.

@dataclass( frozen = True )
class _UnresolvedAtom:
    name:        str
    dtype_token: str | None  # None = omitted; '?' = explicit sentinel; else a dtype string
#/class _UnresolvedAtom

@dataclass( frozen = True )
class _RawUnion:
    args: tuple
#/class _RawUnion

@dataclass( frozen = True )
class _RawIntersect:
    args: tuple
#/class _RawIntersect

@dataclass( frozen = True )
class _RawDifference:
    left:  object
    right: object
#/class _RawDifference

@dataclass( frozen = True )
class _RawCond:
    flag:  object
    then_: object
    else_: object
#/class _RawCond

_RawNode = _UnresolvedAtom | _RawUnion | _RawIntersect | _RawDifference | _RawCond | bdd.ColVar


# -- Grammar (built once at import time -- stateless; dtype resolution is a
# separate post-pass, not a parse action, so this doesn't need rebuilding
# per dtype_lookup)

_LPAR, _RPAR, _COMMA = map( pp.Suppress, '(),' )
_QUOTED = pp.QuotedString( "'" ) | pp.QuotedString( '"' )

_dtype_word   = pp.Word( pp.alphanums + '_:+/=' )
_dtype_parens = pp.original_text_for( pp.nested_expr( '(', ')' ) )
_DTYPE_TOKEN  = pp.Literal( '?' ) | pp.Combine( _dtype_word + pp.Optional( _dtype_parens ) )

_expr = pp.Forward()

_atom_expr = (
    pp.Suppress( pp.Keyword( 'Atom' ) ) + _LPAR
    + _QUOTED( 'name' ) + pp.Optional( _COMMA + _DTYPE_TOKEN( 'dtype' ) )
    + _RPAR
)
_atom_expr.set_parse_action( lambda t: _UnresolvedAtom( t[ 'name' ], t.get( 'dtype' ) ) )

_bare_name_expr = _QUOTED.copy()
_bare_name_expr.set_parse_action( lambda t: _UnresolvedAtom( t[ 0 ], None ) )

_colvar_expr = pp.Suppress( pp.Keyword( 'ColVar' ) ) + _LPAR + _QUOTED + _RPAR
_colvar_expr.set_parse_action( lambda t: bdd.ColVar( t[ 0 ] ) )

_union_expr = (
    pp.Suppress( pp.Keyword( 'Union' ) ) + _LPAR
    + pp.Optional( pp.DelimitedList( _expr ) ) + _RPAR
)
_union_expr.set_parse_action( lambda t: _RawUnion( tuple( t ) ) )

_intersect_expr = (
    pp.Suppress( pp.Keyword( 'Intersect' ) ) + _LPAR
    + pp.Optional( pp.DelimitedList( _expr ) ) + _RPAR
)
_intersect_expr.set_parse_action( lambda t: _RawIntersect( tuple( t ) ) )

_difference_expr = (
    pp.Suppress( pp.Keyword( 'Difference' ) ) + _LPAR
    + _expr + _COMMA + _expr + _RPAR
)
_difference_expr.set_parse_action( lambda t: _RawDifference( t[ 0 ], t[ 1 ] ) )

_cond_expr = (
    pp.Suppress( pp.Keyword( 'Cond' ) ) + _LPAR
    + _expr + _COMMA + _expr + _COMMA + _expr + _RPAR
)
_cond_expr.set_parse_action( lambda t: _RawCond( t[ 0 ], t[ 1 ], t[ 2 ] ) )

_expr <<= (
    _atom_expr | _colvar_expr | _union_expr | _intersect_expr
    | _difference_expr | _cond_expr | _bare_name_expr
)

_GRAMMAR = _expr


# -- Resolution: _RawNode (+ dtype_lookup) -> real bdd.Expr

def _resolve_dtype( name: str, token: str | None, dtype_lookup: dict[ str, pl.DataType ] ) -> pl.DataType:
    if token is not None and token != '?':
        return str_to_dtype( token )
    if name in dtype_lookup:
        return dtype_lookup[ name ]
    raise ValueError(
        f"Atom {name!r} has no dtype -- not given in the expression and not "
        f"found in dtype_lookup (cannonical_schema by default)"
    )
#/def _resolve_dtype

def _resolve( node: _RawNode, dtype_lookup: dict[ str, pl.DataType ] ) -> bdd.Expr:
    if isinstance( node, bdd.ColVar ):
        return node
    if isinstance( node, _UnresolvedAtom ):
        return bdd.Atom( node.name, _resolve_dtype( node.name, node.dtype_token, dtype_lookup ) )
    if isinstance( node, _RawUnion ):
        return bdd.Union( *( _resolve( a, dtype_lookup ) for a in node.args ) )
    if isinstance( node, _RawIntersect ):
        return bdd.Intersect( *( _resolve( a, dtype_lookup ) for a in node.args ) )
    if isinstance( node, _RawDifference ):
        return bdd.Difference( _resolve( node.left, dtype_lookup ), _resolve( node.right, dtype_lookup ) )
    if isinstance( node, _RawCond ):
        return bdd.Cond(
            _resolve( node.flag, dtype_lookup ),
            _resolve( node.then_, dtype_lookup ),
            _resolve( node.else_, dtype_lookup ),
        )
    raise TypeError( f'Unrecognized parsed node: {node!r}' )
#/def _resolve


# -- Public API

def parse_expr( text: str, dtype_lookup: dict[ str, pl.DataType ] | None = None ) -> bdd.Expr:
    """Parse `text` (the grammar above) into a fully dtype-resolved bdd.Expr. dtype_lookup defaults to schema.cannonical_schema; see module docstring's 'DTYPE INFERENCE'."""
    lookup = cannonical_schema if dtype_lookup is None else dtype_lookup
    raw = _GRAMMAR.parse_string( text, parse_all = True )[ 0 ]
    return _resolve( raw, lookup )
#/def parse_expr

def expr_to_text( expr: bdd.Expr ) -> str:
    """Serialize expr back to the grammar above -- always explicit (never '?' or bare-name sugar), a faithful round-trippable form."""
    if isinstance( expr, bdd.Atom ):
        return f"Atom('{expr.name}', {dtype_to_str( expr.dtype )})"
    if isinstance( expr, bdd.ColVar ):
        return f"ColVar('{expr.name}')"
    if isinstance( expr, bdd.Union ):
        return 'Union({})'.format( ', '.join( expr_to_text( a ) for a in expr.args ) )
    if isinstance( expr, bdd.Intersect ):
        return 'Intersect({})'.format( ', '.join( expr_to_text( a ) for a in expr.args ) )
    if isinstance( expr, bdd.Difference ):
        return f'Difference({expr_to_text( expr.left )}, {expr_to_text( expr.right )})'
    if isinstance( expr, bdd.Cond ):
        return f'Cond({expr_to_text( expr.flag )}, {expr_to_text( expr.then_ )}, {expr_to_text( expr.else_ )})'
    raise TypeError( f'Unrecognized Expr subtype: {type(expr)!r}' )
#/def expr_to_text
