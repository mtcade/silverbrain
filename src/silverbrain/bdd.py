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

DTYPE-CARRYING ATOMS

Atom(name, dtype) carries a pl.DataType -- there is no bare-string
shorthand for an atom anymore (there's no dtype to give it), so
Union/Intersect/Difference/Cond only accept Expr instances, never a
naked str. ColVar is unchanged (it denotes a whole *list* of columns, not
one typed value -- dtype doesn't apply until it's resolved down to
concrete atoms). BDDManager keeps a name -> pl.DataType registry,
populated the first time an atom with that name is compiled; declaring
the same name with a *different* dtype anywhere in the same manager
raises ValueError -- dtype is metadata keyed by name, not part of the
variable's boolean identity (two Atom('a', Int64) uses must agree; they
don't become "different variables" the way the atom/var namespaces do).
manager.dtype(name) looks it up. resolve_to_typed_columns() is the
dtype-aware sibling of resolve_to_columns() below, for once you actually
want a real dict[str, pl.DataType] back out, not just names.

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

Worked example (atoms written as bare names here for readability -- real
code writes Atom('a', dtype)): `Difference(Union('a', 'b'), 'a')`
compiles to `(x_a OR x_b) AND NOT x_a`.
  - test point 'a' (x_a=1, x_b=0): (1 OR 0) AND NOT 1 = 0 -> 'a' NOT in result
  - test point 'b' (x_a=0, x_b=1): (0 OR 1) AND NOT 0 = 1 -> 'b' IS in result
  -> resolved set is {'b'}, matching plain set-difference semantics.

See resolve_to_columns()/resolve_to_typed_columns().

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

RESOLUTION -- DECOMPILING A BDDNODE BACK TO EXPR

tableOps.ResolveExpressionOp resolves a table-processes signature map
(each child actor's declared inputs/outputs, as expressions -- see
signatures_to_polars() below) against a bindings map, substituting every
ColVar the bindings cover and leaving the rest untouched -- "resolve as
much as possible," not "require full grounding." ColVar scope is GLOBAL:
one shared symbol table across the whole map, not qualified per actor, so
the same ColVar('C') referenced by two different actors' signatures is
the same variable and one binding resolves it everywhere. Every atom
carries its own dtype (see "DTYPE-CARRYING ATOMS" above), so once a
signature is fully grounded, resolve_to_typed_columns() can turn it
directly into a real dict[str, pl.DataType] -- no separate dtype-lookup
step needed.

Composing a binding into a signature's BDDNode is easy (compose(), see
above) -- the hard direction is turning the *result* back into an Expr
tree for re-serialization, since resolve_to_columns() only works once
everything is fully grounded, but partial resolution must still produce
valid output. decompile() does this via the standard Shannon-expansion
identity, rewritten to use only Union/Intersect/Difference (no raw
negation exposed): for a node (var=v, low=L, high=H),
    ite(x_v, H, L) = (x_v AND H) OR (L AND NOT x_v) = (x_v INTERSECT H) UNION (L DIFFERENCE x_v)
i.e. `Union(Intersect(leaf(v), decompile(H)), Difference(decompile(L), leaf(v)))`.
decompile(FALSE) = Union() (empty union, our existing empty-set
convention).

TRUE needs care, and it can genuinely appear on EITHER a HIGH or a LOW
cofactor -- there is no invariant that rules LOW out (an earlier version
of this docstring claimed one; it was wrong -- a HIGH decision taken
higher up the tree, e.g. some other variable pinned to 1, can make a
*later* variable's LOW branch collapse to TRUE too, not just its own
leaf's HIGH). On a HIGH cofactor this is routine and harmless -- every
bare Atom/ColVar leaf's own high branch is TRUE by construction ("once
this one variable is 1, the formula is unconditionally satisfied") --
decompile() special-cases it directly (Intersect(leaf, "everything") is
just leaf, no recursion into decompile(TRUE) needed). On a LOW cofactor
it is NOT handleable: it means "everything (from here down) satisfies the
formula regardless of every remaining variable," which requires the
complement/universal-set notion this module deliberately has no way to
express. This is a real, reachable failure mode, not just a defensive
check -- e.g. resolving Difference(Union(ColVar('C'), 'a', 'b'),
Intersect('a', ColVar('D'))) hits it: at a=0 the subtracted term
Intersect('a', ColVar('D')) is false regardless of ColVar('D'), so the
difference is unconditionally true there, regardless of D -- decompile()
raises ValueError rather than guessing. Caveat: decompile() does not
preserve BDD DAG sharing -- the output tree can be larger than the input
DAG in pathological cases; fine at table-schema scale.

Resolving one signature in isolation isn't enough, since a binding can
itself reference *other* ColVars ("involve other variable expressions") --
close_bindings() first closes the whole bindings dict against itself
(topologically ordered by dependency, composing each binding's own
referenced bindings into it before it becomes available to later ones),
raising ValueError on a genuine cycle, before resolve_signatures() applies
the closed bindings to every signature and decompiles the result.

SCOPE

There is no dynamic variable reordering, and BDDManager does not validate
that two nodes passed to the same call came from the same manager --
mixing nodes across managers silently produces meaningless results, since
variable indices are manager-relative. Acceptable at the expected scale
(variable count = number of distinct column/ColVar names a schema ever
references, not a SAT-scale problem); revisit if that stops being true.
"""

import polars as pl

from dataclasses import dataclass
from typing import Self, Sequence

from .polarsDataTypeStrings import dtype_to_str, str_to_dtype

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
        # name -> dtype, populated the first time an atom with that name
        # is compiled -- see module docstring's "DTYPE-CARRYING ATOMS".
        self._dtypes: dict[ str, pl.DataType ] = {}
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

    def dtype( self: Self, name: str ) -> pl.DataType:
        """The registered dtype for an atom name -- see module docstring's 'DTYPE-CARRYING ATOMS'."""
        return self._dtypes[ name ]
    #/def dtype

    def _register_dtype( self: Self, name: str, dtype: pl.DataType ) -> None:
        existing = self._dtypes.get( name )
        if existing is None:
            self._dtypes[ name ] = dtype
        elif existing != dtype:
            raise ValueError(
                f'Atom {name!r} already registered with dtype {existing!r}, '
                f'got conflicting dtype {dtype!r}'
            )
    #/def _register_dtype

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

    def atom( self: Self, name: str, dtype: pl.DataType ) -> BDDNode:
        """A literal, already-resolved single column -- see the 'atom' namespace. Raises ValueError if `name` was already registered with a different dtype."""
        self._register_dtype( name, dtype )
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

    def resolve_to_typed_columns( self: Self, f: BDDNode ) -> dict[ str, pl.DataType ]:
        """Dtype-aware sibling of resolve_to_columns() -- same rules, same ValueError on an unbound ColVar, but returns dtype per column via the manager's dtype registry."""
        return { name: self.dtype( name ) for name in self.resolve_to_columns( f ) }
    #/def resolve_to_typed_columns

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
    """
    Base class for the set-expression AST -- every leaf/op knows how to
    compile itself to a canonical BDDNode via a BDDManager, and how to
    serialize to/from a plain dict (see expr_from_dict() below for the
    dict -> Expr direction). No bare-string shorthand anywhere in this
    AST -- see Atom's docstring for why.
    """
    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        raise NotImplementedError
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        raise NotImplementedError
    #/def to_dict
#/class Expr

@dataclass( frozen = True )
class Atom( Expr ):
    """
    A single, already-resolved literal column, carrying its pl.DataType --
    see module docstring's 'DTYPE-CARRYING ATOMS'. No bare-string
    shorthand: unlike the pre-dtype version of this module, there's no
    dtype to give a naked str, so every atom is written explicitly as
    Atom(name, dtype).
    """
    name:  str
    dtype: pl.DataType

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.atom( self.name, self.dtype )
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        return { 'kind': 'atom', 'name': self.name, 'dtype': dtype_to_str( self.dtype ) }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( d[ 'name' ], str_to_dtype( d[ 'dtype' ] ) )
    #/def from_dict
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

    def to_dict( self: Self ) -> dict:
        return { 'kind': 'colvar', 'name': self.name }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( d[ 'name' ] )
    #/def from_dict
#/class ColVar

@dataclass( frozen = True )
class Union( Expr ):
    """N-ary, associative + commutative union of its args."""
    args: tuple[ Expr, ... ]

    def __init__( self: Self, *args: Expr ) -> None:
        object.__setattr__( self, 'args', tuple( args ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.union( *( a.to_bdd( manager ) for a in self.args ) )
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        return { 'kind': 'union', 'args': [ a.to_dict() for a in self.args ] }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( *( expr_from_dict( a ) for a in d[ 'args' ] ) )
    #/def from_dict
#/class Union

@dataclass( frozen = True )
class Intersect( Expr ):
    """N-ary, associative + commutative intersection of its args."""
    args: tuple[ Expr, ... ]

    def __init__( self: Self, *args: Expr ) -> None:
        object.__setattr__( self, 'args', tuple( args ) )
    #/def __init__

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.intersect( *( a.to_bdd( manager ) for a in self.args ) )
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        return { 'kind': 'intersect', 'args': [ a.to_dict() for a in self.args ] }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( *( expr_from_dict( a ) for a in d[ 'args' ] ) )
    #/def from_dict
#/class Intersect

@dataclass( frozen = True )
class Difference( Expr ):
    """Strictly binary: left - right (order matters -- see module docstring's 'BINARY VS N-ARY')."""
    left:  Expr
    right: Expr

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.difference( self.left.to_bdd( manager ), self.right.to_bdd( manager ) )
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        return { 'kind': 'difference', 'left': self.left.to_dict(), 'right': self.right.to_dict() }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( expr_from_dict( d[ 'left' ] ), expr_from_dict( d[ 'right' ] ) )
    #/def from_dict
#/class Difference

@dataclass( frozen = True )
class Cond( Expr ):
    """
    if flag then then_ else else_ -- a thin wrapper over BDDManager.ite,
    letting a generated schema branch on a flag (typically a ColVar later
    bound to TRUE/FALSE via compose()). No Not/NotFlag exists to negate
    `flag` -- swap then_/else_ instead (exactly equivalent for a binary
    conditional); see module docstring's "COND / FLAGS".
    """
    flag:  Expr
    then_: Expr
    else_: Expr

    def to_bdd( self: Self, manager: BDDManager ) -> BDDNode:
        return manager.ite(
            self.flag.to_bdd( manager ),
            self.then_.to_bdd( manager ),
            self.else_.to_bdd( manager ),
        )
    #/def to_bdd

    def to_dict( self: Self ) -> dict:
        return {
            'kind':  'cond',
            'flag':  self.flag.to_dict(),
            'then_': self.then_.to_dict(),
            'else_': self.else_.to_dict(),
        }
    #/def to_dict

    @classmethod
    def from_dict( cls: type, d: dict ) -> Self:
        return cls( expr_from_dict( d[ 'flag' ] ), expr_from_dict( d[ 'then_' ] ), expr_from_dict( d[ 'else_' ] ) )
    #/def from_dict
#/class Cond

_EXPR_CLASSES: dict[ str, type ] = {
    'atom':       Atom,
    'colvar':     ColVar,
    'union':      Union,
    'intersect':  Intersect,
    'difference': Difference,
    'cond':       Cond,
}

def expr_from_dict( d: dict ) -> Expr:
    """Reconstruct an Expr from a dict produced by any Expr subclass's to_dict()."""
    return _EXPR_CLASSES[ d[ 'kind' ] ].from_dict( d )
#/def expr_from_dict

def colvar_names( expr: Expr ) -> set[ str ]:
    """
    Every ColVar name expr references, transitively -- a plain Expr-tree
    walk, no BDDManager/compilation involved (this is pure AST
    inspection, used e.g. to recover InputSchema/OutputSchema's old
    extra/passthrough_from semantics from ColVar presence/sharing alone).
    """
    if isinstance( expr, ColVar ):
        return { expr.name }
    if isinstance( expr, Atom ):
        return set()
    if isinstance( expr, ( Union, Intersect ) ):
        return set().union( *( colvar_names( a ) for a in expr.args ) ) if expr.args else set()
    if isinstance( expr, Difference ):
        return colvar_names( expr.left ) | colvar_names( expr.right )
    if isinstance( expr, Cond ):
        return colvar_names( expr.flag ) | colvar_names( expr.then_ ) | colvar_names( expr.else_ )
    raise TypeError( f'Unrecognized Expr subtype: {type(expr)!r}' )
#/def colvar_names


# -- Decompiling a BDDNode back to Expr (see module docstring's "RESOLUTION")

def _leaf_expr( manager: BDDManager, var_idx: int ) -> Expr:
    namespace, name = manager.var_name( var_idx )
    return ColVar( name ) if namespace == 'var' else Atom( name, manager.dtype( name ) )
#/def _leaf_expr

def decompile( manager: BDDManager, node: BDDNode ) -> Expr:
    """
    Reconstruct an Expr tree denoting the same set as `node` -- not
    necessarily the same syntax that produced it (canonical BDDs don't
    remember that), just an equivalent one. See module docstring's
    "RESOLUTION" for the algorithm and for when this can genuinely fail.

    TRUE can legitimately appear on a HIGH cofactor (every bare Atom/
    ColVar leaf's high branch, routinely -- "once this one variable is 1,
    the rest doesn't matter") and that's handled directly: Intersect(leaf,
    "everything") is just leaf. TRUE on a LOW cofactor is different and
    NOT handleable: it means, at this point in the decomposition,
    "everything (from here down) satisfies the formula regardless of
    every remaining variable" -- expressing that requires the complement/
    universal-set notion this module deliberately has no way to write
    (same reason raw Not/complement was excluded from the AST). This is a
    real, reachable case for some Difference-with-ColVar combinations
    (e.g. resolving can produce a sub-formula like NOT(a AND ColVar('D'))
    -- at a=0 that's unconditionally true, regardless of what ColVar('D')
    ever binds to) -- decompile raises ValueError rather than guessing.
    """
    if node is FALSE:
        return Union()
    if node is TRUE:
        raise ValueError(
            'decompile() cannot express this result without the universal-set/'
            'complement notion this module deliberately excludes -- the resolved '
            'expression, at this point, is true regardless of every remaining '
            'variable, which has no Union/Intersect/Difference/Atom/ColVar '
            'representation; see module docstring\'s "RESOLUTION"'
        )

    leaf = _leaf_expr( manager, node.var )
    # node.high is TRUE routinely (e.g. every bare leaf's high branch) --
    # Intersect(leaf, "everything") is just leaf, so simplify directly
    # rather than recursing into decompile(TRUE).
    high_part = leaf if node.high is TRUE else Intersect( leaf, decompile( manager, node.high ) )
    low_part  = Difference( decompile( manager, node.low ), leaf )
    return Union( high_part, low_part )
#/def decompile


# -- Resolution: closing bindings against each other, then applying them

def _topological_order( deps: dict[ str, set[ str ] ] ) -> list[ str ]:
    """Kahn's algorithm. Raises ValueError naming the cycle if `deps` isn't a DAG."""
    remaining = { name: set( d ) for name, d in deps.items() }
    order: list[ str ] = []

    while remaining:
        ready = sorted( name for name, d in remaining.items() if not d )
        if not ready:
            raise ValueError(
                'Cyclic ColVar bindings: {}'.format( ', '.join( sorted( remaining ) ) )
            )
        for name in ready:
            del remaining[ name ]
        for d in remaining.values():
            d.difference_update( ready )
        order.extend( ready )

    return order
#/def _topological_order

def close_bindings( manager: BDDManager, bindings: dict[ str, Expr ] ) -> dict[ str, BDDNode ]:
    """
    Fully substitute each binding's own ColVar references against the
    *other* bindings (transitively, dependency-ordered), so every closed
    binding is ready to compose directly into anything else. A ColVar a
    binding references that is NOT itself a key of `bindings` is left
    free (not an error) -- an intentionally still-symbolic binding.
    Raises ValueError on a cyclic binding graph (see _topological_order).
    """
    compiled = { name: expr.to_bdd( manager ) for name, expr in bindings.items() }
    deps = {
        name: {
            manager.var_name( i )[ 1 ]
            for i in manager.support( node )
            if manager.var_name( i )[ 0 ] == 'var'
            and manager.var_name( i )[ 1 ] in bindings
            and manager.var_name( i )[ 1 ] != name
        }
        for name, node in compiled.items()
    }
    order = _topological_order( deps )

    closed: dict[ str, BDDNode ] = {}
    for name in order:
        node = compiled[ name ]
        for dep_name in deps[ name ]:
            dep_idx = manager._var_index( 'var', dep_name )
            node = manager.compose( node, dep_idx, closed[ dep_name ] )
        closed[ name ] = node
    return closed
#/def close_bindings

def apply_bindings( manager: BDDManager, node: BDDNode, closed: dict[ str, BDDNode ] ) -> BDDNode:
    """Compose every already-closed binding into `node` -- order doesn't matter once bindings are closed against each other."""
    for name, bound_node in closed.items():
        idx = manager._var_index( 'var', name )
        node = manager.compose( node, idx, bound_node )
    return node
#/def apply_bindings

def resolve_signatures(
    manager:    BDDManager,
    signatures: dict,
    bindings:   dict[ str, Expr ],
    ) -> dict:
    """
    Resolve every signature expression against `bindings`, substituting
    as many ColVars as possible (transitively, through other bindings).
    ColVars bindings doesn't cover remain in the result -- not an error,
    per "resolve as much as possible." Generic over the signature dict's
    key type (e.g. the (op_idn, direction, index) triples
    signatures_to_polars uses).
    """
    closed = close_bindings( manager, bindings )
    return {
        key: decompile( manager, apply_bindings( manager, expr.to_bdd( manager ), closed ) )
        for key, expr in signatures.items()
    }
#/def resolve_signatures


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
    'dtype':      pl.Utf8,   # non-null only on 'atom' leaves -- dtype_to_str-encoded, see module docstring's 'DTYPE-CARRYING ATOMS'
    'branch':     pl.UInt8,  # non-null only on a 'difference' node's children (0=left/1=right) or a 'cond' node's children (0=flag/1=then_/2=else_)
}

_BINDINGS_TREE_COLS: dict[ str, pl.DataType ] = _EXPR_TREE_COLS | {
    'var_name': pl.Utf8,  # non-null only on root rows -- which ColVar this subtree defines
}

_SIGNATURE_TREE_COLS: dict[ str, pl.DataType ] = _EXPR_TREE_COLS | {
    # non-null only on root rows -- which actor-signature slot this subtree
    # defines. Mirrors schema.py's __table_op_schema__ identifier columns.
    'op_idn':    pl.Utf8,
    'direction': pl.Utf8,  # 'input' | 'output'
    'index':     pl.UInt32,
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
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'atom', 'name': expr.name, 'dtype': dtype_to_str( expr.dtype ), 'branch': branch } )
    elif isinstance( expr, ColVar ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'colvar', 'name': expr.name, 'dtype': None, 'branch': branch } )
    elif isinstance( expr, Union ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'union', 'name': None, 'dtype': None, 'branch': branch } )
        for child in expr.args:
            next_idx = _expr_rows( child, next_idx, node_idx, None, rows )
    elif isinstance( expr, Intersect ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'intersect', 'name': None, 'dtype': None, 'branch': branch } )
        for child in expr.args:
            next_idx = _expr_rows( child, next_idx, node_idx, None, rows )
    elif isinstance( expr, Difference ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'difference', 'name': None, 'dtype': None, 'branch': branch } )
        next_idx = _expr_rows( expr.left, next_idx, node_idx, 0, rows )
        next_idx = _expr_rows( expr.right, next_idx, node_idx, 1, rows )
    elif isinstance( expr, Cond ):
        rows.append( { 'node_idx': node_idx, 'parent_idx': parent_idx, 'kind': 'cond', 'name': None, 'dtype': None, 'branch': branch } )
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
        return Atom( row[ 'name' ], str_to_dtype( row[ 'dtype' ] ) )
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
    """Reconstruct a var_name -> expression dict from a bindings_to_polars() forest. Raises ValueError on a duplicate root var_name (silently picking a winner would hide a real data bug)."""
    roots = df.filter( pl.col( 'parent_idx' ).is_null() )
    var_names = roots[ 'var_name' ].to_list()
    dupes = { name for name in var_names if var_names.count( name ) > 1 }
    if dupes:
        raise ValueError( f'Duplicate var_name root(s) in bindings forest: {sorted(dupes)}' )
    return {
        row[ 'var_name' ]: _expr_from_row( df, row[ 'node_idx' ] )
        for row in roots.to_dicts()
    }
#/def polars_to_bindings

def signatures_to_polars(
    signatures:     dict[ tuple[ str, str, int ], Expr ],
    node_idx_start: int = 0,
    ) -> tuple[ pl.DataFrame, int ]:
    """
    Serialize an (op_idn, direction, index) -> expression signature dict
    (a Cell's table-processes map) as one shared, multi-root forest --
    sibling of bindings_to_polars, tagged by the compound key instead of a
    single var_name. Returns (forest, next_node_idx).
    """
    rows: list[ dict ] = []
    next_idx = node_idx_start
    root_key: dict[ int, tuple[ str, str, int ] ] = {}

    for key, expr in signatures.items():
        root_idx = next_idx
        next_idx = _expr_rows( expr, next_idx, None, None, rows )
        root_key[ root_idx ] = key

    for row in rows:
        key = root_key.get( row[ 'node_idx' ] )
        row[ 'op_idn' ]    = key[ 0 ] if key is not None else None
        row[ 'direction' ] = key[ 1 ] if key is not None else None
        row[ 'index' ]     = key[ 2 ] if key is not None else None

    return pl.DataFrame( rows, schema = _SIGNATURE_TREE_COLS ), next_idx
#/def signatures_to_polars

def polars_to_signatures( df: pl.DataFrame ) -> dict[ tuple[ str, str, int ], Expr ]:
    """Reconstruct an (op_idn, direction, index) -> expression dict from a signatures_to_polars() forest. Raises ValueError on a duplicate root key."""
    roots = df.filter( pl.col( 'parent_idx' ).is_null() )
    keys = [ ( r[ 'op_idn' ], r[ 'direction' ], r[ 'index' ] ) for r in roots.to_dicts() ]
    dupes = { k for k in keys if keys.count( k ) > 1 }
    if dupes:
        raise ValueError( f'Duplicate (op_idn, direction, index) root(s) in signature forest: {sorted(dupes)}' )
    return {
        ( row[ 'op_idn' ], row[ 'direction' ], row[ 'index' ] ): _expr_from_row( df, row[ 'node_idx' ] )
        for row in roots.to_dicts()
    }
#/def polars_to_signatures
