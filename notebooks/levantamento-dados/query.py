%pyspark

import pendulum as pdm


SQL_SKELETON = """SELECT
    {selected_fields_filter}
FROM
    raichu_flattened.complains{where}
LIMIT
    1000"""


# Fields to select

fields = [
    ("00_gambi_id", "ID"),
    ("01_gambi_title", "Título"),
    ("02_gambi_description", "Texto"),
    ("03_gambi_company_id", "ID da Empresa"),
    ("04_gambi_company_name", "Nome da Empresa"),
    ("05_gambi_user_id", "ID do Usuário"),
    ("06_gambi_user_username", "Username do Usuário"),
    ("07_gambi_user_name", "Nome do Usuário"),
    ("08_gambi_user_email", "Email do Usuário"),
    ("09_gambi_user_birth_date", "Data de Nascimento do Usuário"),
    ("10_gambi_user_sex", "Sexo do Usuário"),
    ("11_gambi_origin", "Origem (SITE, MOBILE, APP, WHATSAPP)"),
    ("12_gambi_ip", "IP de Origem"),
    ("13_gambi_city", "Cidade"),
    ("14_gambi_state", "Estado"),
    ("15_gambi_region", "Região"),
    ("16_gambi_status", "Status"),
    ("17_gambi_category_id", "ID da Categoria"),
    ("18_gambi_category_name", "Nome da Categoria"),
    ("19_gambi_score", "Nota"),
    ("20_gambi_product_type_id", "ID do Tipo de Produto"),
    ("21_gambi_product_name", "Nome do Tipo de Produto"),
    ("22_gambi_problem_type_id", "ID do Tipo de Problema"),
    ("23_gambi_problem_name", "Nome do Tipo de Problema"),
    ("24_gambi_deleted", "Deletada?"),
    ("25_gambi_frozen", "Congelada?"),
    ("26_gambi_deal_again", "Faria Negocio Novamente?"),
    ("27_gambi_solved", "Resolvida?"),
    ("28_gambi_compliment", "Elogio?"),
    ("29_gambi_in_moderation", "Em Moderação?"),
    ("30_gambi_blackfriday", "Blackfriday?"),
    ("31_gambi_created_at", "Data da Reclamação"),
    ("32_gambi_modified_at", "Data da Avaliação")
]
selected_fields = z.checkbox("Colunas", fields, ["01_gambi_title"])
if not selected_fields:
    selected_fields = [fields[1][0]]

selected_fields = sorted(selected_fields)
selected_fields = [selected_field.split('_gambi_')[1] for selected_field in selected_fields]

selected_fields_filter = ',\n    '.join(selected_fields)

# Initial and final datetime values

default_final_datetime = pdm.utcnow().in_timezone('Brazil/East')
default_initial_datetime = default_final_datetime.subtract(days=1)
show_date = lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')
initial_datetime = z.input("Data Inicial", show_date(default_initial_datetime)).strip()
final_datetime = z.input("Data Final", show_date(default_final_datetime)).strip()

initial_datetime_filter = ""
if initial_datetime:
    initial_datetime_filter = "{datetime_column} >= '%s'" % initial_datetime

final_datetime_filter = ""
if final_datetime:
    final_datetime_filter = "{datetime_column} <= '%s'" % final_datetime

# Type of datetime

date_type = [
    ("created_at", "Reclamacao"),
    ("modified_at", "Avaliacao")
]
datetime_column = z.select("Filtrar por data de", date_type, "created_at")
if not datetime_column:
    raise Exception("Selecione um tipo de data!")

evaluated_filter = ""
if datetime_column == "modified_at":
    evaluated_filter = "status = 'EVALUATED'"

if initial_datetime_filter:
    initial_datetime_filter = initial_datetime_filter.format(**{"datetime_column": datetime_column})
if final_datetime_filter:
    final_datetime_filter = final_datetime_filter.format(**{"datetime_column": datetime_column})

# Categories (from Diderot)

category_id = z.select("Categoria (Diderot)", loaded_tables["categories"], "")

category_filter = ""
if category_id:
    category_filter = "category_id = '{}'".format(category_id)

# Product Types

product_type_id = z.select("Tipo de Produto", loaded_tables["product_types"], "")

product_type_filter = ""
if product_type_id:
    product_type_filter = "product_type_id = '{}'".format(product_type_id)

# Problem Types

problem_type_id = z.select("Tipo de Problema", loaded_tables["problem_types"], "")

problem_type_filter = ""
if problem_type_id:
    problem_type_filter = "problem_type_id = '{}'".format(problem_type_id)

# Company ID filter

companies_ids = z.input("IDs de Empresas (separados por vírgula ou manter em branco)").strip()
companies_ids

companies_ids_filter = ""
if companies_ids:
    companies_ids = companies_ids.split(",")
    companies_ids = ["'{}'".format(company_id.strip()) for company_id in companies_ids]
    companies_ids = ', '.join(companies_ids)
    companies_ids_filter = "company_id IN ({})".format(companies_ids)

# Complain ID filter

complains_ids = z.input("IDs de Reclamações (separados por vírgula ou manter em branco)").strip()
complains_ids

complains_ids_filter = ""
if complains_ids:
    complains_ids = complains_ids.split(",")
    complains_ids = ["'{}'".format(company_id.strip()) for company_id in complains_ids]
    complains_ids = ', '.join(complains_ids)
    complains_ids_filter = "id IN ({})".format(complains_ids)

# Filter by word

comma_separated_words = z.input("Filtro de palavras no Título ou no Texto (por enquanto é de mentira")
comma_separated_words

comma_separated_words_filter = ""


# Joining all filters

where_filters = [
    initial_datetime_filter,
    final_datetime_filter,
    evaluated_filter,
    category_filter,
    product_type_filter,
    problem_type_filter,
    companies_ids_filter,
    complains_ids_filter,
    comma_separated_words_filter
]
where_filters = [where_filter for where_filter in where_filters if where_filter]

where = ""
if where_filters:
    where = "\nWHERE\n    %s" % "\n    AND ".join(where_filters)


# Query building, fetching and result exhibition

sql_query = SQL_SKELETON.format(**{
    "selected_fields_filter": selected_fields_filter,
    "where": where
})

print sql_query

result = execute_query(sql_query)
z.show(result)