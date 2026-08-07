#
#//  bdd.py
#//  silverbrain
#//
#//  Created by Evan Mason on 8/6/26.
#//
"""
A canonical (Reduced Ordered) Binary Decision Diagram, plus a small
set-expression algebra built on top of it -- Union/Intersect/Difference of
named column atoms and free ColVar placeholders, meant for *generated*
actor signatures (e.g. a TableOp's declared output) rather than
hand-written ones.

WHY A BDD

A ColVar is a placeholder standing for a whole (not-yet-known) list/set of
columns, bound later to a concrete or still-symbolic expression -- "bind
values to these variables, and let the result flow through." Structural
equivalence between expressions that still contain unbound ColVars (e.g.
recognizing Difference(ColVar('C'), ColVar('C')) is provably empty no
matter what C eventually binds to, or that Union(a,b) and Union(b,a) are
the same expression) is exactly what a *free Boolean algebra on named
generators* gives you: treat every distinct atom name and every distinct
ColVar name as its own independent generator/decision-variable, Union/
Intersect/Difference as OR/AND/AND-NOT over those variables, and reduce to
canonical (hash-consed) form. Two BDDNodes are then structurally equal iff
`is` -- see BDDNode below.

TWO NAMESPACES

Every generator is registered under a (namespace, name) key, namespace
being 'atom' (a literal, already-resolved column) or 'var' (a free ColVar,
not yet bound) -- kept strictly separate so a column literally named 'C'
can never collide with ColVar('C'). BDDManager assigns each new
(namespace, name) pair the next integer decision-variable index, in
first-seen order; that order is also the BDD's variable order (no dynamic
reordering -- see "SCOPE" below).

BINARY VS N-ARY

Union/Intersect are n-ary (associative + commutative -- folded pairwise
via the binary `or`/`and` primitives). Difference is strictly binary
(`A - B`): order matters (`A - B != B - A`), so n-ary difference chains
are ambiguous and must be nested explicitly (`Difference(Difference(A,B),
C)` for `A - B - C`).

EXTRACTING A CONCRETE COLUMN SET

Once every ColVar in an expression has been bound away (no 'var'-namespace
generators remain reachable), the fully-grounded BDD encodes a genuine
concrete set of columns -- but reading it back out is *not* "evaluate the
formula." A literal atom is a known, disjoint singleton (no two distinct
atom names denote the same column), so for each atom `a` appearing:
restrict the formula to `x_a = 1` and every *other* atom variable to `0`
(since a real candidate point can equal at most one distinct atom name),
and check whether the result is the TRUE terminal. If so, `a` is a member
of the resolved set.

Worked example: `Difference(Union('a', 'b'), 'a')` compiles to
`(x_a OR x_b) AND NOT x_a`.
  - test point 'a' (x_a=1, x_b=0): (1 OR 0) AND NOT 1 = 0 -> 'a' NOT in result
  - test point 'b' (x_a=0, x_b=1): (0 OR 1) AND NOT 0 = 1 -> 'b' IS in result
  -> resolved set is {'b'}, matching plain set-difference semantics.

See resolve_to_columns().

PREDICATES

Five query operators fall out of the algebra -- plain bool answers, not
new expression-builders -- all on BDDManager:
  - is_empty(f)        : f is the empty set
  - is_subset(f, g)     : f subset-eq g -- holds even with unbound
                          ColVars still present (a "true for every
                          possible binding" check, not just a concrete-
                          set check)
  - is_superset(f, g)   : g subset-eq f
  - overlaps(f, g)      : could f and g share a column for *some*
                          binding
  - contains(f, name)   : is `name` definitely in f, for every binding
  - may_contain(f, name): could `name` possibly be in f, for some binding
contains/may_contain differ precisely when `name` only reaches f through
an unbound ColVar: contains(ColVar('C'), 'a') is False (C might bind to
something without 'a'), but may_contain(ColVar('C'), 'a') is True (C
might bind to something *with* 'a').

None of these are a plain "is difference(f,g) FALSE?" / "is intersect(f,g)
not FALSE?" check over the raw boolean formula -- that would be unsound.
Two *different* literal atoms are semantically disjoint (see "EXTRACTING
A CONCRETE COLUMN SET" above), but nothing in the raw boolean algebra
enforces that: Intersect(Atom('a'), Atom('b')) is semantically the empty
set, yet AND(x_a, x_b) is satisfiable as a raw formula (x_a=x_b=1). Every
predicate here instead walks the same per-candidate-point construction
resolve_to_columns uses (pin one atom "in", every other mentioned atom
"out", plus a fresh/unmentioned-atom case for when a ColVar could pull in
a brand new name), quantifying any remaining ColVars via exists/forall as
appropriate -- see BDDManager._at_point/.exists/.forall.

COND / FLAGS -- NO COMPLEMENT, NO NOT

Cond(flag, then_, else_) is a thin wrapper over BDDManager.ite (already
the general primitive every other op here reduces to), letting a
generated schema branch on a flag -- typically a ColVar later bound to
TRUE or FALSE via compose(). There is deliberately no Not/NotFlag
expression: negating a binary conditional's own flag is always exactly
equivalent to swapping its two branches (`Cond(flag, A, B)` negated ==
`Cond(flag, B, A)`), so nothing is lost by omitting it, and omitting it
avoids reopening the same problem complement already ruled out. TRUE is
NOT "the universal set of columns" -- it's intersect's identity element
and every leaf's terminal, nothing more. Concretely: resolve_to_columns
only ever walks a formula's actual support, so a hypothetical
Not(Atom('a')) would silently resolve to the *empty* set (support is
just {a}, and restricting a=1 makes NOT(1)=0) rather than "every column
except a" -- quietly wrong, not a clear error, which is worse than an
unsupported operator. That is why Not/complement stay out of the AST
entirely; BDDManager.negate remains available as the internal/power-user
primitive (already used by difference) for building a flag's BDDNode
directly outside the Expr AST, if ever needed.

SERIALIZATION -- A FOREST, NOT A STRING

What gets serialized to a DataFrame is the source *Expr* AST, never the
compiled/canonical BDDNode graph (that graph is BDDManager-relative --
variable indices, hash-consing -- and not meaningful across a save/load
boundary; same split as configTree.py's declarative config_tree vs. a
live Cell). The encoding follows parameterParse.py's tree-shaped forest
convention rather than a stringly-typed nested blob: one row per node,
node_idx/parent_idx (permanent ids, root has parent_idx=null), a `kind`
discriminant ('union'|'intersect'|'difference'|'cond'|'colvar'|'atom'),
`name` (non-null only on 'colvar'/'atom' leaves), and `branch` (non-null
only on a 'difference' node's two children -- 0=left/minuend,
1=right/subtrahend -- or a 'cond' node's three children -- 0=flag,
1=then_, 2=else_; Union/Intersect children are order-independent and
don't need it).
node_idx is threaded via the same counter-in/counter-out convention as
ParameterParser.to_polars()/config_tree(), so multiple forests can share
one globally-unique node_idx space across a call.

A var_name -> expression bindings table (two logical columns: which
ColVar, and what it resolves to) can't fit "one whole tree per cell," so
it's encoded as one shared multi-root forest instead: every row belongs to
exactly one binding's expression tree, and each root row additionally
carries the var_name it defines -- the same multi-tree-in-one-DataFrame
trick parameterParse.py already uses for multiple ops' parameter trees.
See bindings_to_polars()/polars_to_bindings().

SCOPE

There is no dynamic variable reordering, and BDDManager does not validate
that two nodes passed to the same call came from the same manager --
mixing nodes across managers silently produces meaningless results, since
variable indices are manager-relative. Acceptable at the expected scale
(variable count = number of distinct column/ColVar names a schema ever
references, not a SAT-scale problem); revisit if that stops being true.

NOT YET IMPLEMENTED

The TableOp that actually resolves an expression forest against a
bindings forest (tableOps.ResolveExpressionOp) is a stub -- see its
docstring for the intended eventual shape.
"""

import polars as pl

from dataclasses import dataclass
from typing import Self, Sequence

# -- BDD core

class BDDNode:
    """
    A single BDD node -- either a terminal (var is None, value is the
    constant it represents) or an internal Shannon-expansion node
    (var/low/high: "if var then high else low"). Never constructed
    directly outside this module -- always via BDDManager._mk() (internal
    nodes) or the TRUE/FALSE singletons below (terminals), which is what
    makes hash-consing valid.

    EQUALITY IS IDENTITY. No __eq__/__hash__ override -- two BDDNodes are
    semantically equal iff `is`, by construction (every node this module
    ever hands out for a given (manager, formula) pair is the same Python
    object). Do not compare BDDNodes structurally; do not implement
    __eq__ on this class.
    """
    __slots__ = ( 'var', 'low', 'high', 'value' )

    def __init__(
        self: Self,
        var:   int | None       = None,
        low:   'BDDNode | None' = None,
        high:  'BDDNode | None' = None,
        value: bool | None      = None,
        ) -> None:
        self.var   = var
        self.low   = low
        self.high  = high
        self.value = value
    #/def __init__

    @property
    def is_terminal( self: Self ) -> bool:
        return self.var is None
    #/def is_terminal

    def __repr__( self: Self ) -> str:
        if self.is_terminal:
            return 'TRUE' if self.value else 'FALSE'
        return f'BDDNode(var={self.var}, low={self.low!r}, high={self.high!r})'
    #/def __repr__
#/class BDDNode

# Terminals are manager-agnostic (they reference no variable) -- true
# module-level singletons, shared across every BDDManager.
FALSE: BDDNode = BDDNode( value = False )
TRUE:  BDDNode = BDDNode( value = True )


class BDDManager:
    """
    Owns one variable-index namespace and one hash-consing unique table.
    Nodes from different BDDManager instances must never be mixed (see
    "SCOPE" in the module docstring) -- construct one manager per
    independent expression universe (e.g. per Cell/pipeline), not a
    shared global.
    """

    def __init__( self: Self ) -> None:
        self._var_order: dict[ tuple[ str, str ], int ] = {}
        self._var_names: list[ tuple[ str, str ] ]       = []
        self._unique:    dict[ tuple[ int, int, int ], BDDNode ] = {}
        self._ite_cache: dict[ tuple[ int, int, int ], BDDNode ] = {}
    #/def __init__

    # -- variable registration

    def _var_index( self: Self, namespace: str, name: str ) -> int:
        key = ( namespace, name )
        idx = self._var_order.get( key )
        if idx is None:
            idx = len( self._var_names )
            self._var_order[ key ] = idx
            self._var_names.append( key )
        return idx
    #/def _var_index

    def var_name( self: Self, var_idx: int ) -> tuple[ str, str ]:
        """(namespace, name) for a decision-variable index -- mainly for debugging/repr."""
        return self._var_names[ var_idx ]
    #/def var_name

    # -- canonical construction

    def _mk( self: Self, var_idx: int, low: BDDNode, high: BDDNode ) -> BDDNode:
        if low is high:
            return low
        key = ( var_idx, id( low ), id( high ) )
        node = self._unique.get( key )
        if node is None:
            node = BDDNode( var = var_idx, low = low, high = high )
            self._unique[ key ] = node
        return node
    #/def _mk

    def atom( self: Self, name: str ) -> BDDNode:
        """A literal, already-resolved single column -- see the 'atom' namespace."""
        idx = self._var_index( 'atom', name )
        return self._mk( idx, FALSE, TRUE )
    #/def atom

    def colvar( self: Self, name: str ) -> BDDNode:
        """A free, not-yet-bound placeholder standing for a whole list/set of columns -- see the 'var' namespace."""
        idx = self._var_index( 'var', name )
        return self._mk( idx, FALSE, TRUE )
    #/def colvar

    # -- algebra

    def ite( self: Self, f: BDDNode, g: BDDNode, h: BDDNode ) -> BDDNode:
        """
        General Shannon-expansion "if f then g else h", from which every
        other op here is derived. Memoized per-manager -- safe to keep
        forever since nodes are canonical/immutable for the manager's
        lifetime.
        """
        if f is TRUE:
            return g
        if f is FALSE:
            return h
        if g is TRUE and h is FALSE:
            return f
        if g is h:
            return g

        key = ( id( f ), id( g ), id( h ) )
        cached = self._ite_cache.get( key )
        if cached is not None:
            return cached

        top = min(
            ( n.var for n in ( f, g, h ) if not n.is_terminal )
        )

        def _cofactor( n: BDDNode, value: bool ) -> BDDNode:
            if n.is_terminal or n.var != top:
                return n
            return n.high if value else n.low
        #/def _cofactor

        low  = self.ite( _cofactor( f, False ), _cofactor( g, False ), _cofactor( h, False ) )
        high = self.ite( _cofactor( f, True ),  _cofactor( g, True ),  _cofactor( h, True ) )
        result = self._mk( top, low, high )
        self._ite_cache[ key ] = result
        return result
    #/def ite

    def negate( self: Self, f: BDDNode ) -> BDDNode:
        return self.ite( f, FALSE, TRUE )
    #/def negate

    def intersect( self: Self, *args: BDDNode ) -> BDDNode:
        result = TRUE
        for f in args:
            result = self.ite( result, f, FALSE )
        return result
    #/def intersect

    def union( self: Self, *args: BDDNode ) -> BDDNode:
        result = FALSE
        for f in args:
            result = self.ite( result, TRUE, f )
        return result
    #/def union

    def difference( self: Self, f: BDDNode, g: BDDNode ) -> BDDNode:
        return self.ite( f, self.negate( g ), FALSE )
    #/def difference

    def restrict( self: Self, f: BDDNode, var_idx: int, value: bool ) -> BDDNode:
        """
        Cofactor: substitute a constant for one decision variable.
        Ordered-BDD short-circuit -- if f's topmost variable already comes
        after var_idx in registration order, var_idx cannot occur anywhere
        below it, so f is returned unchanged.
        """
        if f.is_terminal or f.var > var_idx:
            return f
        if f.var == var_idx:
            branch = f.high if value else f.low
            return branch
        return self._mk(
            f.var,
            self.restrict( f.low, var_idx, value ),
            self.restrict( f.high, var_idx, value ),
        )
    #/def restrict

    def compose( self: Self, f: BDDNode, var_idx: int, g: BDDNode ) -> BDDNode:
        """
        Bind decision variable var_idx to expression g inside f, and let
        the result flow through -- the operational meaning of "resolving"
        a ColVar.
        """
        return self.ite(
            g,
            self.restrict( f, var_idx, True ),
            self.restrict( f, var_idx, False ),
        )
    #/def compose

    def exists( self: Self, f: BDDNode, var_idx: int ) -> BDDNode:
        """Existential quantification -- is f satisfiable for *some* value of var_idx."""
        return self.union( self.restrict( f, var_idx, False ), self.restrict( f, var_idx, True ) )
    #/def exists

    def forall( self: Self, f: BDDNode, var_idx: int ) -> BDDNode:
        """Universal quantification -- does f hold for *every* value of var_idx."""
        return self.intersect( self.restrict( f, var_idx, False ), self.restrict( f, var_idx, True ) )
    #/def forall

    # -- extraction

    def support( self: Self, f: BDDNode ) -> set[ int ]:
        """Every decision-variable index reachable in f."""
        seen: set[ int ] = set()
        visited: set[ int ] = set()

        def _walk( n: BDDNode ) -> None:
            if n.is_terminal or id( n ) in visited:
                return
            visited.add( id( n ) )
            seen.add( n.var )
            _walk( n.low )
            _walk( n.high )
        #/def _walk

        _walk( f )
        return seen
    #/def support

    def resolve_to_columns( self: Self, f: BDDNode ) -> frozenset[ str ]:
        """
        Read a concrete set of column names back out of a fully-grounded
        (no remaining 'var'-namespace generators) BDD -- see "EXTRACTING A
        CONCRETE COLUMN SET" in the module docstring for the reasoning.
        Raises ValueError if any ColVar is still unbound.
        """
        atom_vars: list[ int ] = []
        unbound:   list[ str ] = []
        for idx in self.support( f ):
            namespace, name = self._var_names[ idx ]
            if namespace == 'var':
                unbound.append( name )
            else:
                atom_vars.append( idx )

        if unbound:
            raise ValueError(
                'Cannot resolve to a concrete column set -- unbound ColVar(s): {}'.format(
                    ', '.join( sorted( unbound ) )
                )
            )

        result: set[ str ] = set()
        for idx in atom_vars:
            point = f
            for other in atom_vars:
                point = self.restrict( point, other, other == idx )
            if point is TRUE:
                result.add( self._var_names[ idx ][ 1 ] )
        return frozenset( result )
    #/def resolve_to_columns

    # -- predicates (see module docstring's "PREDICATES")
    #
    # NOTE ON CORRECTNESS: these cannot be plain "is difference(f,g)
    # FALSE?" / "is intersect(f,g) not FALSE?" checks over the raw
    # boolean formula. Two *different* literal atoms are semantically
    # disjoint (see "EXTRACTING A CONCRETE COLUMN SET"), but nothing in
    # the raw boolean algebra enforces that -- Intersect(Atom('a'),
    # Atom('b')) is semantically the empty set, yet AND(x_a, x_b) is
    # satisfiable as a raw formula (x_a=x_b=1). So every predicate below
    # walks the same per-candidate-point construction resolve_to_columns
    # uses (pin one atom "in", every other mentioned atom "out"), plus a
    # fresh/unmentioned-atom case (relevant only when a ColVar could pull
    # in a brand new name), quantifying any remaining ColVars via
    # exists/forall as appropriate.

    def _atoms_in( self: Self, f: BDDNode ) -> list[ int ]:
        return sorted( i for i in self.support( f ) if self._var_names[ i ][ 0 ] == 'atom' )
    #/def _atoms_in

    def _colvars_in( self: Self, f: BDDNode ) -> list[ int ]:
        return sorted( i for i in self.support( f ) if self._var_names[ i ][ 0 ] == 'var' )
    #/def _colvars_in

    def _at_point( self: Self, f: BDDNode, atom_idxs: Sequence[ int ], target: int | None ) -> BDDNode:
        """
        Restrict f to one hypothetical candidate point: the literal atom
        `target`, or (target=None) a fresh point equal to none of
        atom_idxs. Every atom index in atom_idxs is pinned per the
        disjointness rule; any ColVar variables remain free in the result.
        """
        point = f
        for idx in atom_idxs:
            point = self.restrict( point, idx, idx == target )
        return point
    #/def _at_point

    def is_empty( self: Self, f: BDDNode ) -> bool:
        return f is FALSE
    #/def is_empty

    def is_subset( self: Self, f: BDDNode, g: BDDNode ) -> bool:
        """f subset-eq g -- true at every point, for every possible binding of any remaining ColVars."""
        diff        = self.difference( f, g )
        atom_idxs   = sorted( set( self._atoms_in( f ) ) | set( self._atoms_in( g ) ) )
        colvar_idxs = sorted( set( self._colvars_in( f ) ) | set( self._colvars_in( g ) ) )
        for target in ( *atom_idxs, None ):
            point = self._at_point( diff, atom_idxs, target )
            for c in colvar_idxs:
                point = self.exists( point, c )  # a binding making diff true anywhere is a counterexample
            if point is not FALSE:
                return False
        return True
    #/def is_subset

    def is_superset( self: Self, f: BDDNode, g: BDDNode ) -> bool:
        return self.is_subset( g, f )
    #/def is_superset

    def overlaps( self: Self, f: BDDNode, g: BDDNode ) -> bool:
        """Could f and g share some column, for *some* point and *some* binding."""
        inter       = self.intersect( f, g )
        atom_idxs   = sorted( set( self._atoms_in( f ) ) | set( self._atoms_in( g ) ) )
        colvar_idxs = sorted( set( self._colvars_in( f ) ) | set( self._colvars_in( g ) ) )
        for target in ( *atom_idxs, None ):
            point = self._at_point( inter, atom_idxs, target )
            for c in colvar_idxs:
                point = self.exists( point, c )
            if point is not FALSE:
                return True
        return False
    #/def overlaps

    def contains( self: Self, f: BDDNode, name: str ) -> bool:
        """Definite membership -- is `name` in f no matter how remaining ColVars bind."""
        idx       = self._var_index( 'atom', name )
        atom_idxs = sorted( set( self._atoms_in( f ) ) | { idx } )
        point     = self._at_point( f, atom_idxs, idx )
        for c in self._colvars_in( f ):
            point = self.forall( point, c )
        return point is TRUE
    #/def contains

    def may_contain( self: Self, f: BDDNode, name: str ) -> bool:
        """Possible membership -- could `name` be in f for *some* binding."""
        idx       = self._var_index( 'atom', name )
        atom_idxs = sorted( set( self._atoms_in( f ) ) | { idx } )
        point     = self._at_point( f, atom_idxs, idx )
        for c in self._colvars_in( f ):
            point = self.exists( point, c )
        return point is TRUE
    #/def may_contain
#/class BDDManager


# -- Expression AST

class Expr:
    """Base class for the set-expression AST -- every leaf/op knows how to compile itself to a canonical BDDNode via a BDDManager."""
    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        raise NotImplementedError
    #/def to_bdd
#/class Expr

def _coerce( x: 'Expr | str' ) -> Expr:
    """Bare str args auto-wrap as Atom -- atoms only; ColVar is always explicit (never coerced from a string)."""
    return Atom( x ) if isinstance( x, str ) else x
#/def _coerce

@dataclass( frozen = True )
class Atom( Expr ):
    """A single, already-resolved literal column name."""
    name: str

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.atom( self.name )
    #/def to_bdd
#/class Atom

@dataclass( frozen = True )
class ColVar( Expr ):
    """
    A declared free-variable symbol, TypeVar-styled (`ColVar('C')`, not a
    naked string that silently promotes). Stands for a whole list/set of
    columns -- an entire unresolved sub-expression, never a single
    column -- bound later via BDDManager.compose(); see the module
    docstring's "WHY A BDD" / "BINDING" sections.
    """
    name: str

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.colvar( self.name )
    #/def to_bdd
#/class ColVar

@dataclass( frozen = True )
class Union( Expr ):
    """N-ary, associative + commutative union of its args (bare str args auto-coerce to Atom)."""
    args: tuple[ Expr, ... ]

    def __init__( self: Self, *args: 'Expr | str' ) -> None:
        object.__setattr__( self, 'args', tuple( _coerce( a ) for a in args ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.union( *( a.to_bdd( manager ) for a in self.args ) )
    #/def to_bdd
#/class Union

@dataclass( frozen = True )
class Intersect( Expr ):
    """N-ary, associative + commutative intersection of its args (bare str args auto-coerce to Atom)."""
    args: tuple[ Expr, ... ]

    def __init__( self: Self, *args: 'Expr | str' ) -> None:
        object.__setattr__( self, 'args', tuple( _coerce( a ) for a in args ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.intersect( *( a.to_bdd( manager ) for a in self.args ) )
    #/def to_bdd
#/class Intersect

@dataclass( frozen = True )
class Difference( Expr ):
    """Strictly binary: left - right (order matters -- see module docstring's 'BINARY VS N-ARY'). Bare str args auto-coerce to Atom."""
    left:  Expr
    right: Expr

    def __init__( self: Self, left: 'Expr | str', right: 'Expr | str' ) -> None:
        object.__setattr__( self, 'left', _coerce( left ) )
        object.__setattr__( self, 'right', _coerce( right ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.difference( self.left.to_bdd( manager ), self.right.to_bdd( manager ) )
    #/def to_bdd
#/class Difference

@dataclass( frozen = True )
class Cond( Expr ):
    """
    if flag then then_ else else_ -- a thin wrapper over BDDManager.ite,
    letting a generated schema branch on a flag (typically a ColVar later
    bound to TRUE/FALSE via compose()). No Not/NotFlag exists to negate
    `flag` -- swap then_/else_ instead (exactly equivalent for a binary
    conditional); see module docstring's "COND / FLAGS". Bare str args
    auto-coerce to Atom.
    """
    flag:  Expr
    then_: Expr
    else_: Expr

    def __init__( self: Self, flag: 'Expr | str', then_: 'Expr | str', else_: 'Expr | str' ) -> None:
        object.__setattr__( self, 'flag', _coerce( flag ) )
        object.__setattr__( self, 'then_', _coerce( then_ ) )
        object.__setattr__( self, 'else_', _coerce( else_ ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.ite(
            self.flag.to_bdd( manager ),
            self.then_.to_bdd( manager ),
            self.else_.to_bdd( manager ),
        )
    #/def to_bdd
#/class Cond


# -- Forest serialization
#
# node_idx/parent_idx are permanent, forest-local ids -- following
# parameterParse.py's precedent, these dtypes are kept local to this
# module rather than added to schema.cannonical_schema.

_EXPR_TREE_COLS: dict[ str, pl.DataType ] = {
    'node_idx':   pl.UInt32,
    'parent_idx': pl.UInt32,
    'kind':       pl.Utf8,   # 'union' | 'intersect' | 'difference' | 'cond' | 'colvar' | 'atom'
    'name':       pl.Utf8,   # non-null only on 'colvar'/'atom' leaves
    'branch':     pl.UInt8,  # non-null only on a 'difference' node's children (0=left/1=right) or a 'cond' node's children (0=flag/1=then_/2=else_)
}

_BINDINGS_TREE_COLS: dict[ str, pl.DataType ] = _EXPR_TREE_COLS | {
    'var_name': pl.Utf8,  # non-null only on root rows -- which ColVar this subtree defines
}

def _expr_rows(
    expr:            Expr,
    node_idx_start:  int,
    parent_idx:      int | None,
    branch:          int | None,
    rows:            list[ dict ],
    ) -> int:
    """Depth-first row emission shared by expr_to_polars/bindings_to_polars. Returns the next free node_idx."""
    node_idx  = node_idx_start
    next_idx  = node_idx_start + 1

    if isinstance( expr, Atom ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'atom', 'name': expr.name, 'branch': branch } )
    elif isinstance( expr, ColVar ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'colvar', 'name': expr.name, 'branch': branch } )
    elif isinstance( expr, Union ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'union', 'name': None, 'branch': branch } )
        for child in expr.args:
            next_idx = _expr_rows( child, next_idx, node_idx, None, rows )
    elif isinstance( expr, Intersect ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'intersect', 'name': None, 'branch': branch } )
        for child in expr.args:
            next_idx = _expr_rows( child, next_idx, node_idx, None, rows )
    elif isinstance( expr, Difference ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'difference', 'name': None, 'branch': branch } )
        next_idx = _expr_rows( expr.left, next_idx, node_idx, 0, rows )
        next_idx = _expr_rows( expr.right, next_idx, node_idx, 1, rows )
    elif isinstance( expr, Cond ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'cond', 'name': None, 'branch': branch } )
        next_idx = _expr_rows( expr.flag, next_idx, node_idx, 0, rows )
        next_idx = _expr_rows( expr.then_, next_idx, node_idx, 1, rows )
        next_idx = _expr_rows( expr.else_, next_idx, node_idx, 2, rows )
    else:
        raise TypeError( f'Unrecognized Expr subtype: {type(expr)!r}' )

    return next_idx
#/def _expr_rows

def expr_to_polars( expr: Expr, node_idx_start: int = 0 ) -> tuple[ pl.DataFrame, int ]:
    """Serialize a single Expr tree -- see module docstring's 'SERIALIZATION'. Returns (forest, next_node_idx)."""
    rows: list[ dict ] = []
    next_idx = _expr_rows( expr, node_idx_start, None, None, rows )
    return pl.DataFrame( rows, schema = _EXPR_TREE_COLS ), next_idx
#/def expr_to_polars

def _expr_from_row( df: pl.DataFrame, node_idx: int ) -> Expr:
    row = df.filter( pl.col( 'node_idx' ) == node_idx ).row( 0, named = True )
    kind = row[ 'kind' ]

    if kind == 'atom':
        return Atom( row[ 'name' ] )
    if kind == 'colvar':
        return ColVar( row[ 'name' ] )

    children = df.filter( pl.col( 'parent_idx' ) == node_idx ).sort( 'node_idx' )

    if kind == 'union':
        return Union( *( _expr_from_row( df, i ) for i in children[ 'node_idx' ].to_list() ) )
    if kind == 'intersect':
        return Intersect( *( _expr_from_row( df, i ) for i in children[ 'node_idx' ].to_list() ) )
    if kind == 'difference':
        by_branch = { r[ 'branch' ]: r[ 'node_idx' ] for r in children.to_dicts() }
        return Difference(
            _expr_from_row( df, by_branch[ 0 ] ),
            _expr_from_row( df, by_branch[ 1 ] ),
        )
    if kind == 'cond':
        by_branch = { r[ 'branch' ]: r[ 'node_idx' ] for r in children.to_dicts() }
        return Cond(
            _expr_from_row( df, by_branch[ 0 ] ),
            _expr_from_row( df, by_branch[ 1 ] ),
            _expr_from_row( df, by_branch[ 2 ] ),
        )
    raise ValueError( f'Unrecognized kind: {kind!r}' )
#/def _expr_from_row

def polars_to_expr( df: pl.DataFrame, root_node_idx: int | None = None ) -> Expr:
    """
    Reconstruct a single Expr tree. root_node_idx defaults to the sole
    null-parent_idx row -- pass it explicitly for a multi-root forest
    (e.g. one produced by bindings_to_polars).
    """
    if root_node_idx is None:
        roots = df.filter( pl.col( 'parent_idx' ).is_null() )
        if roots.height != 1:
            raise ValueError(
                f'Forest has {roots.height} root(s) -- pass root_node_idx explicitly to disambiguate'
            )
        root_node_idx = roots[ 'node_idx' ][ 0 ]
    return _expr_from_row( df, root_node_idx )
#/def polars_to_expr

def bindings_to_polars(
    bindings:       dict[ str, Expr ],
    node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
    """
    Serialize a var_name -> expression bindings dict as one shared,
    multi-root forest -- see module docstring's 'SERIALIZATION'. Returns
    (forest, next_node_idx).
    """
    rows: list[ dict ] = []
    next_idx = node_idx_start
    root_var_name: dict[ int, str ] = {}

    for var_name, expr in bindings.items():
        root_idx = next_idx
        next_idx = _expr_rows( expr, next_idx, None, None, rows )
        root_var_name[ root_idx ] = var_name

    for row in rows:
        row[ 'var_name' ] = root_var_name.get( row[ 'node_idx' ] )

    return pl.DataFrame( rows, schema = _BINDINGS_TREE_COLS ), next_idx
#/def bindings_to_polars

def polars_to_bindings( df: pl.DataFrame ) -> dict[ str, Expr ]:
    """Reconstruct a var_name -> expression dict from a bindings_to_polars() forest."""
    roots = df.filter( pl.col( 'parent_idx' ).is_null() )
    return {
        row[ 'var_name' ]: _expr_from_row( df, row[ 'node_idx' ] )
        for row in roots.to_dicts()
    }
#/def polars_to_bindings
