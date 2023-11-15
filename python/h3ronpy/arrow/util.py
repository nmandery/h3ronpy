import pyarrow.compute as pc
import pyarrow as pa


# from https://issues.apache.org/jira/browse/ARROW-12099
def explode_table_include_null(table: pa.Table, column: str) -> pa.Table:
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    indices = pc.list_parent_indices(pc.fill_null(table[column], [None]))
    result = table.select(other_columns)
    try:
        # may result in a large memory allocation
        result = result.take(indices)
    except pa.ArrowIndexError:
        # See https://github.com/nmandery/h3ronpy/issues/40
        # Using RuntimeWarning as ResourceWarning is often not displayed to the user.
        import warnings

        warnings.warn("This ArrowIndexError may be a sign of the process running out of memory.", RuntimeWarning)
        raise
    result = result.append_column(
        pa.field(column, table.schema.field(column).type.value_type),
        pc.list_flatten(pc.fill_null(table[column], [None])),
    )
    return result
