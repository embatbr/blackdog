%pyspark


SQL_SKELETON = """SELECT
    DISTINCT name,
    id
FROM
    raichu_crud.{}
ORDER BY
    name"""


table_names = [
    "categories",
    "product_types",
    "problem_types"
]
loaded_tables = dict()


# Query fetching

for table_name in table_names:
    sql_query = SQL_SKELETON.format(table_name)

    result = execute_query(sql_query)
    result = result.toPandas().values.tolist()

    loaded_tables[table_name] = [(_id, name) for (name, _id) in result]
    loaded_tables[table_name].insert(0, ("", ""))