%pyspark


SQL_SKELETON = """(SELECT
    DISTINCT name,
    id
FROM
    raichu_crud.{}
ORDER BY
    name) result
"""


table_names = [
    "categories",
    "product_types",
    "problem_types"
]
loaded_tables = dict()


# Query fetching

for table_name in table_names:
    SQL_QUERY = SQL_SKELETON.format(table_name)

    result = sqlContext.read.format('jdbc').options(
        url=JDBC_CONNECTION_STRING_TEMPLATE.format(
            DW_HOST, DW_PORT, DW_DATABASE, DW_USER, DW_PASSWORD
        ),
        dbtable=SQL_QUERY
    ).load()

    result = result.toPandas().values.tolist()

    loaded_tables[table_name] = [(_id, name) for (name, _id) in result]
    loaded_tables[table_name].insert(0, ("", ""))
