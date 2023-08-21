import pyarrow.compute as pc
import pyarrow as pa


# from https://issues.apache.org/jira/browse/ARROW-12099
def explode_table_include_null(table: pa.Table, column: str) -> pa.Table:
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    indices = pc.list_parent_indices(pc.fill_null(table[column], [None]))
    result = table.select(other_columns).take(indices)
    result = result.append_column(
        pa.field(column, table.schema.field(column).type.value_type),
        pc.list_flatten(pc.fill_null(table[column], [None])),
    )
    return result
