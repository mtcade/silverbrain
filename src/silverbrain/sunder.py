#
#//  sunder.py
#//  silverbrain
#//

"""
Sunder tables: named, single-row tables whose name starts with a leading
underscore (e.g. `_trigger`), marking them as framework-supplied
runtime-context tables rather than domain data a bundle author wires up
themselves. Any node inside a Cell (an 'op' or nested 'cell' row in
__nodes__) may declare a sunder name as one of its own source ports; if
this Cell's own external contract (self.source) never receives that name
from its caller, the Cell auto-supplies it itself -- see cell.py's
_WiringEngine.__init__. This means a sunder table no longer needs to be
threaded explicitly through every enclosing `[cell] source = [...]` on
its way down to whichever node actually needs it -- each Cell resolves
its own nodes' sunder needs independently, at whatever nesting depth.

A sunder table's schema is a perfectly ordinary schema.table_schemas
entry (e.g. table_schemas['_trigger']) -- no separate schema registry
here, so "is this a sunder table" is purely "does sunder_table_factories
have an entry for this table_schemas name", not a second source of truth
for its columns. `sunder_table_factories` maps table_schemas name -> a
zero-arg callable building one fresh row -- called once per Cell
invocation that needs it (never cached), since a sunder table's whole
purpose is per-run context (a fresh run_id/timestamp each time), not a
constant.

An implementor (silverflow, or any other silverbrain-based
application) adds its own sunder tables by declaring their schema in its
own table_schemas extension (schema.py's cannonical_schema_for_keys
pattern) and merging its own factory on top of sunder_table_factories,
exactly like `duckpond.schema.table_schemas = _silverflow_table_schemas
| {...}` already does for schemas.
"""

from .schema import table_schemas

from datetime import datetime, timezone
from typing import Callable
from uuid_extensions import uuid7

import polars as pl


def _make_trigger_table() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'run_id':       [ str( uuid7() ) ],
            'triggered_at': [ datetime.now( timezone.utc ) ],
        },
        schema = table_schemas[ '_trigger' ],
    )
#/def _make_trigger_table


sunder_table_factories: dict[ str, Callable[ [], pl.DataFrame ] ] = {
    '_trigger': _make_trigger_table,
}
