%pyspark

import os


SQL_QUERY = """(SELECT
    DISTINCT name,
    id
FROM
    raichu_crud.problem_types
ORDER BY
    name) result
"""


# Query fetching

DW_HOST = os.environ.get("DW_HOST")
DW_PORT = os.environ.get("DW_PORT")
DW_DATABASE = os.environ.get("DW_DATABASE")
DW_USER = os.environ.get("DW_USER")
DW_PASSWORD = os.environ.get("DW_PASSWORD")

result = sqlContext.read.format('jdbc').options(
    url='jdbc:postgresql://{DW_HOST}:{DW_PORT}/{DW_DATABASE}?user={DW_USER}&password={DW_PASSWORD}'.format(**{
        "DW_HOST": DW_HOST,
        "DW_PORT": DW_PORT,
        "DW_DATABASE": DW_DATABASE,
        "DW_USER": DW_USER,
        "DW_PASSWORD": DW_PASSWORD
    }),
    dbtable=SQL_QUERY
).load()

result = result.toPandas().values.tolist()

problem_types = [(_id, name) for (name, _id) in result]
problem_types.insert(0, ("", ""))
