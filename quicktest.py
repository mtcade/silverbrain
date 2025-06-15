"""
    Connect to the simple database at `data/example/quicktest.duckdb`
"""

from silverbrain import cell
import polars as pl


nucleus_0_schema: dict[ str, str ] = {
    'letter': pl.String,
    'number': pl.UInt8,
}

nucleus_0: cell.Nucleus = cell.Nucleus(
    status = {},
    table_schema = nucleus_0_schema,
    update_lambda = cell.getUpdateLambda_forKey( ['letter'] )
)

nucleus_0.queueUpdate(
    pl.DataFrame(
        {
            'letter': ['a'],
            'number': [0]
        },
        schema = nucleus_0_schema
    )
)

nucleus_0.updateAll()

print( nucleus_0.table )

nucleus_0.queueUpdate(
    pl.DataFrame(
        {
            'letter': ['b'],
            'number': [1]
        },
        schema = nucleus_0_schema
    )
)

nucleus_0.updateAll()

print( nucleus_0.table )

nucleus_0.queueUpdate(
    pl.DataFrame(
        {
            'letter': ['a'],
            'number': [2]
        },
        schema = nucleus_0_schema
    )
)

nucleus_0.updateAll()

print( nucleus_0.table )
