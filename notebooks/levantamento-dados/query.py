%pyspark

import os
import pendulum as pdm


SQL_SKELETON = """(SELECT
    {selected_fields}
FROM
    raichu_flattened.complains
WHERE
    {datetime_column} >= '{initial_datetime}'
    AND {datetime_column} <= '{final_datetime}'{evaluated_filter}{category_filter}{product_type_filter}{problem_type_filter}{companies_ids_filter}
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
    ("score", "Nota"),
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

# Categories (from Diderot)

category_id = z.select("Categoria (Diderot)", loaded_tables["categories"], "")

category_filter = ""
if category_id:
    category_filter = "\n    AND category_id = '{}'".format(category_id)

# Product Types

product_type_id = z.select("Tipo de Produto", loaded_tables["product_types"], "")

product_type_filter = ""
if product_type_id:
    product_type_filter = "\n    AND product_type_id = '{}'".format(product_type_id)

# Problem Types

problem_type_id = z.select("Tipo de Problema", loaded_tables["problem_types"], "")

problem_type_filter = ""
if problem_type_id:
    problem_type_filter = "\n    AND problem_type_id = '{}'".format(problem_type_id)

# Company ID filter

companies_ids = z.input("IDs de Empresas (separados por vírgula)")
companies_ids.strip()

companies_ids_filter = ""
if companies_ids:
    companies_ids = companies_ids.split(",")
    companies_ids = ["'{}'".format(company_id.strip()) for company_id in companies_ids]
    companies_ids = ', '.join(companies_ids)
    companies_ids_filter = "\n    AND company_id IN ({})".format(companies_ids)


# Query building, fetching and result exhibition

SQL_QUERY = SQL_SKELETON.format(**{
    "selected_fields": selected_fields,
    "datetime_column": selected_date_type,
    "initial_datetime": default_initial_datetime,
    "final_datetime": default_final_datetime,
    "evaluated_filter": evaluated_filter,
    "category_filter": category_filter,
    "product_type_filter": product_type_filter,
    "problem_type_filter": problem_type_filter,
    "companies_ids_filter": companies_ids_filter
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
