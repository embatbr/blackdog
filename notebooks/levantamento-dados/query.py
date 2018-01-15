%pyspark

import pendulum as pdm


SQL_SKELETON = """SELECT
    {selected_fields_filter}
FROM
    raichu_flattened.complains{where}{group_by_statement}{order_by_statement}
LIMIT
    {limit}"""


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

companies_ids = z.input("IDs de Empresas (separados por vírgula)").strip()

companies_ids_filter = ""
if companies_ids:
    companies_ids = companies_ids.split(",")
    companies_ids = ["'{}'".format(company_id.strip()) for company_id in companies_ids]
    companies_ids = ', '.join(companies_ids)
    companies_ids_filter = "company_id IN ({})".format(companies_ids)

# Complain ID filter

complains_ids = z.input("IDs de Reclamações (separados por vírgula)").strip()

complains_ids_filter = ""
if complains_ids:
    complains_ids = complains_ids.split(",")
    complains_ids = ["'{}'".format(company_id.strip()) for company_id in complains_ids]
    complains_ids = ', '.join(complains_ids)
    complains_ids_filter = "id IN ({})".format(complains_ids)

# Filter by word

comma_separated_words = z.input("[not working] Filtro de palavras no Título ou no Texto").strip()

comma_separated_words_filter = ""

# Filter by region

region = z.select("Região", enums["brazillian_regions"], enums["brazillian_regions"][0][0])

region_filter = ""
if region:
    region_filter = "region = '%s'" % region

# Filter by state

state = z.select("Estado", enums["brazillian_states"], enums["brazillian_states"][0][0])

state_filter = ""
if state:
    state_filter = "state = '%s'" % state

# Filter by city

city = z.input("[not working] Cidade").strip()

city_filter = ""
# if city:
#     city_filter = "city = '%s'" % city

# Filter by minimum user birthdate

min_user_birthdate = z.input("Data de nascimento mínima").strip()

min_user_birthdate_filter = ""
if min_user_birthdate:
    min_user_birthdate_filter = "user_birth_date >= '%s'" % min_user_birthdate

# Filter by maximum user birthdate

max_user_birthdate = z.input("Data de nascimento máxima").strip()

max_user_birthdate_filter = ""
if max_user_birthdate:
    max_user_birthdate_filter = "user_birth_date <= '%s'" % max_user_birthdate

# Origin

origins = z.input("Origens (separadas por vírgula): SITE, MOBILE, APP, WHATSAPP").strip()

origins_filter = ""
if origins:
    origins = ["'%s'" % origin for origin in origins]
    origins_filter = "origin IN (%s)" % ", ".join(origins)

# Filter by deleted or not

deleted_or_not = [
    ("", "Todas"),
    ("f", "Ativas"),
    ("t", "Inativas")
]
deleted = z.select("Filtrar por reclamações", deleted_or_not, deleted_or_not[0][0])

deleted_filter = ""
if deleted:
    deleted_filter = "deleted = '%s'" % deleted

# Counting

count_by_options = [
    ("", ""),
    ("id", "ID da Reclamação"),
    ("DISTINCT user_id", "Consumidor")
]
count_by = z.select("Contar por", count_by_options, count_by_options[0][0])

# Grouping

group_options = [
    (False, "Não"),
    (True, "Sim")
]
group = z.select("Agrupar (por colunas selecionados)", group_options, group_options[0][0])

# GROUP BY filters (HAVING) when using count

having_count_gte = z.input("Limite inferior da contagem agrupada").strip()
if having_count_gte:
    having_count_gte = int(having_count_gte)
    if having_count_gte < 0:
        having_count_gte = 0

having_count_lte = z.input("Limite superior da contagem agrupada").strip()
if having_count_lte:
    having_count_lte = int(having_count_lte)
    if having_count_lte < 0:
        having_count_lte = 0

# Ordering

order_by = z.input("Ordenar (separe os colunas por vírgula e use 'DESC' após o coluna para inverter sua ordem)").strip()

order_by_statement = ""
if order_by:
    order_by = list(map(lambda x: x.strip(), order_by.split(',')))
    order_by_statement = '\nORDER BY\n    %s' % '\n    '.join(order_by)

# Limit

limit = z.input("Limite (de 1 a 1000)").strip()

if not limit:
    limit = "1000"

limit = int(limit)

if limit < 1:
    limit = 1
elif limit > 1000:
    limit = 1000

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
selected_fields = z.checkbox("Colunas (obs: a ordem da NÃO se mantém)", fields, ["01_gambi_title"])
selected_fields = [selected_field.split('_gambi_')[1] for selected_field in selected_fields]

# Checking group

group_by_statement = ""
if (group or count_by) and selected_fields:
    selected_fields_filter = ',\n    '.join(selected_fields)
    group_by_statement = "\nGROUP BY\n    %s" % selected_fields_filter

    if (having_count_gte != "") or (having_count_lte != ""):
        having_list = list()
        if having_count_gte != "":
            having_list.append("count(%s) >= %s" % (count_by, having_count_gte))
        if having_count_lte != "":
            having_list.append("count(%s) <= %s" % (count_by, having_count_lte))

        having_statement = "HAVING\n        %s" % "\n        AND ".join(having_list)
        group_by_statement = "%s\n    %s" % (group_by_statement, having_statement)

# Adding count in selected fields

selected_fields_filter = ''
if count_by:
    selected_fields.append("count(%s) AS _count" % count_by)

selected_fields_filter = ',\n    '.join(selected_fields)


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
    comma_separated_words_filter,
    region_filter,
    state_filter,
    city_filter,
    min_user_birthdate_filter,
    max_user_birthdate_filter,
    origins_filter,
    deleted_filter
]
where_filters = [where_filter for where_filter in where_filters if where_filter]

where = ""
if where_filters:
    where = "\nWHERE\n    %s" % "\n    AND ".join(where_filters)


# Query building, fetching and result exhibition

sql_query = SQL_SKELETON.format(**{
    "selected_fields_filter": selected_fields_filter,
    "where": where,
    "group_by_statement": group_by_statement,
    "order_by_statement": order_by_statement,
    "limit": limit
})

print "Query:"
print sql_query

result = execute_query(sql_query)
z.show(result)