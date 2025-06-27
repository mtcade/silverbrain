"""
    Basic tests for silverbrain
"""
 
from silverbrain import nucleus
import polars as pl

TestResult: type = bool

# -- Settings - Display
VERBOSE = 3
# Verbose:
#    >0: Start and end of tests
#    >1: Schema
#    >2: Data inputs and outputs

INDENT_BASE: int = 1 # Spaces after the "#" for messages
PREFIX_BASE: str = ' '*INDENT_BASE

if VERBOSE > 0:
    print("#" + PREFIX_BASE + "quicktest")
    print("#" + PREFIX_BASE + "quicktest VERBOSE={}".format( VERBOSE ))
#


# -- Tests

def test_cell_0(
    schema: dict = {},
    verbose: int = 0,
    indent: int = 1
    ) -> TestResult:
    
    prefix_0: str = ' '*indent
    prefix_1: str = prefix_0 + '  '
    prefix_2: str = prefix_1 + '  '
    
    if verbose > 0:
        print("#" + prefix_0 + "quicktest.test_cell_0")
    #
    
    if schema and verbose > 1:
        print("#" + prefix_1 + "quicktest.test_cell_0 schema:")
        print( schema )
    #
    
    # Test -- nucleus.PatientMemoryCell
    #   update a table by one column
    
    nucleus_0_schema: dict[ str, type ] = {
        'letter': pl.String,
        'number': pl.UInt8,
    }

    nucleus_0: nucleus.PatientMemoryCell = nucleus.PatientMemoryCell(
        status = {},
        table_schema = nucleus_0_schema,
        update_lambda = nucleus.getUpdateLambda_forKey( ['letter'] )
    )
    
    data_0: pl.DataFrame = pl.DataFrame(
        {
            'letter': ['a'],
            'number': [0]
        },
        schema = nucleus_0_schema
    )
    
    nucleus_0.queueUpdate(
        data_0
    )

    nucleus_0.updateAll()
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_cell_0 data_0:")
        print( data_0 )
        
        print("#" + prefix_2 + "quicktest.test_cell_0 nucleus_0.table:")
        print( nucleus_0.table )
    #/if verbose > 2
    
    assert nucleus_0.table.equals( data_0 )
    
    # -- Test nucleus.PatientMemoryCell
    #  Add row
    data_1: pl.DataFrame = pl.DataFrame(
        {
            'letter': ['b'],
            'number': [1]
        },
        schema = nucleus_0_schema
    )
    
    data_check_1: pl.DataFrame = pl.DataFrame(
        {
            'letter': ['a','b'],
            'number': [0,1]
        },
        schema = nucleus_0_schema
    )
    
    nucleus_0.queueUpdate(
        data_1
    )
    
    nucleus_0.updateAll()
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_cell_0 data_1:")
        print( data_1 )
    
        print("#" + prefix_2 + "quicktest.test_cell_0 nucleus_0.table:")
        print( nucleus_0.table )
    #

    assert nucleus_0.table.equals( data_check_1 )

    # -- Test nucleus.PatientMemoryCell
    #  Update value (row) by letter (a)
    
    data_2: pl.DataFrame = pl.DataFrame(
        {
            'letter': ['a'],
            'number': [2]
        },
        schema = nucleus_0_schema
    )
    
    data_check_2: pl.DataFrame = pl.DataFrame(
        {
            'letter': ['a','b'],
            'number': [2,1]
        },
        schema = nucleus_0_schema
    )
    
    nucleus_0.queueUpdate(
        data_2
    )

    nucleus_0.updateAll()
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_cell_0 data_2:")
        print( data_2 )
        
        print("#" + prefix_2 + "quicktest.test_cell_0 nucleus_0.table:")
        print( nucleus_0.table )
    #

    assert nucleus_0.table.equals( data_check_2 )
    
    if verbose > 0:
        print("#" + prefix_0 + "/quicktest.test_cell_0")
    #
    
    return True
#/def test_cell_0

def test_web_0(
    schema: dict = {},
    verbose: int = 0,
    indent: int = 1
    ) -> TestResult:
    """
        Make a one-celled web, with only a simple transformation
    """
    prefix_0: str = ' '*indent
    prefix_1: str = prefix_0 + '  '
    prefix_2: str = prefix_1 + '  '
    
    if verbose > 0:
        print("#" + prefix_0 + "quicktest.test_web_0")
    #
    if schema and verbose > 1:
        print("#" + prefix_1 + "quicktest.test_web_0 schema:")
        print( schema )
    #
    
    # Data setup
    data_0: pl.DataFrame = pl.DataFrame(
        {'letter':['a','b'], 'number': [0,1] },
        schema = {
            'letter': pl.String,
            'number': pl.UInt8,
        }
    )
    
    data_check_0: pl.DataFrame = pl.DataFrame(
        {'letter':['a','b'], 'number': [1,2] },
        schema = {
            'letter': pl.String,
            'number': pl.UInt8,
        }
    )
    
    # Test -- TableLambda
    
    transform_lambda_0: nucleus.TableLambda = lambda cel, tab: tab.with_columns(
        pl.col("number") + 1
    )
    
    data_0_result_0: pl.DataFrame = transform_lambda_0( None, data_0)
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_web_0 data_0:")
        print( data_0 )

        print("#" + prefix_2 + "quicktest.test_web_0 data_0_result_0:")
        print( data_0_result_0 )
    #
    
    assert data_0_result_0.equals( data_check_0 )
    
    # Test -- nucleus.TransformerCell.updateAll
    
    transformerCell_0: nucleus.TransformerCell = nucleus.TransformerCell(
        transform_lambda = transform_lambda_0
    )
    
    transformerCell_0.queueUpdate( data_0 )
    transformerCell_0.updateAll()
    
    assert not transformerCell_0.outbox.empty()
    
    data_0_result_1: pl.DataFrame = transformerCell_0.outbox.get()
    transformerCell_0.outbox.task_done()
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_web_0 data_0:")
        print( data_0 )
        
        print("#" + prefix_2 + "quicktest.test_web_0 data_0_result_1:")
        print( data_0_result_1 )
    #
    
    assert data_0_result_1.equals( data_check_0 )
    
    assert transformerCell_0.inbox.empty()
    assert transformerCell_0.outbox.empty()
    
    # Test -- nucleus.TransformerCell.updateOnce
    
    transformerCell_0.queueUpdate( data_0 )
    transformerCell_0.updateOnce()
    
    assert not transformerCell_0.outbox.empty()
    data_0_result_2: pl.DataFrame = transformerCell_0.outbox.get()
    transformerCell_0.outbox.task_done()
    
    if verbose > 2:
        print("#" + prefix_2 + "quicktest.test_web_0 data_0:")
        print( data_0 )
        
        print("#" + prefix_2 + "quicktest.test_web_0 data_0_result_2:")
        print( data_0_result_2 )
    #
    
    assert data_0_result_0.equals( data_check_0 )
    
    if verbose > 0:
        print("#" + prefix_0 + "/quicktest.test_web_0")
    #
    
    return True
#/def test_web_0

if True: test_cell_0(
    verbose = VERBOSE,
    indent = INDENT_BASE + 2
)

if True: test_web_0(
    verbose = VERBOSE,
    indent = INDENT_BASE + 2
)

if VERBOSE > 0:
    print("#" + PREFIX_BASE + "/quicktest")
#
