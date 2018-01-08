%pyspark

import os
import pendulum as pdm


SQL_SKELETON = """(SELECT
    {selected_fields}
FROM
    raichu_flattened.complains
WHERE
    {datetime_column} >= '{initial_datetime}'
    AND {datetime_column} <= '{final_datetime}'{evaluated_filter}
LIMIT
    1000) result
"""


# Fields to select

fields = [
    ("id", "ID"),
    ("title", "Título"),
    ("description", "Texto"),
    ("company_id", "ID da Empresa"),
    ("company_name", "Nome da Empresa"),
    ("user_id", "ID do Usuário"),
    ("user_username", "Username do Usuário"),
    ("user_name", "Nome do Usuário"),
    ("user_email", "Email do Usuário"),
    ("user_birth_date", "Data de Nascimento do Usuário"),
    ("user_sex", "Sexo do Usuário"),
    ("origin", "Origem (SITE, MOBILE, APP, WHATSAPP)"),
    ("ip", "IP de Origem"),
    ("city", "Cidade"),
    ("state", "Estado"),
    ("region", "Região"),
    ("status", "Status"),
    ("category_id", "ID da Categoria"),
    ("category_name", "Nome da Categoria"),
    ("score", "Pontuação"),
    ("product_type_id", "ID do Tipo de Produto"),
    ("product_name", "Nome do Tipo de Produto"),
    ("problem_type_id", "ID do Tipo de Problema"),
    ("problem_name", "Nome do Tipo de Problema"),
    ("deleted", "Foi Deletada?"),
    ("frozen", "Foi Suspensa?"),
    ("deal_again", "Faria Negocio Novamente?"),
    ("solved", "Foi Resolvida?"),
    ("compliment", "É Elogio?"),
    ("in_moderation", "Está em Moderação?"),
    ("blackfriday", "É Blackfriday?"),
    ("created_at", "Data da Reclamação"),
    ("modified_at", "Data da Avaliação")
]
selected_fields = ',\n    '.join(z.checkbox("Colunas", fields, ["title"]))
if not selected_fields:
    raise Exception('Selecione ao menos 1 campo!')

# Initial and final datetime values

default_final_datetime = pdm.utcnow().in_timezone('Brazil/East')
default_initial_datetime = default_final_datetime.subtract(days=1)
show_date = lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')
default_initial_datetime = z.input("Data Inicial", show_date(default_initial_datetime))
default_final_datetime = z.input("Data Final", show_date(default_final_datetime))

# Type of datetime

date_type = [
    ("created_at", "Reclamacao"),
    ("modified_at", "Avaliacao")
]
selected_date_type = z.select("Data de", date_type, "created_at")
if not selected_date_type:
    raise Exception("Selecione um tipo de data!")

evaluated_filter = ""
if selected_date_type == "modified_at":
    evaluated_filter = "\n    AND status = 'EVALUATED'"


# Query building, fetching and result exhibition

SQL_QUERY = SQL_SKELETON.format(**{
    "selected_fields": selected_fields,
    "datetime_column": selected_date_type,
    "initial_datetime": default_initial_datetime,
    "final_datetime": default_final_datetime,
    "evaluated_filter": evaluated_filter
})

print SQL_QUERY

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

z.show(result)
