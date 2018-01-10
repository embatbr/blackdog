%pyspark


SQL_SKELETON = """SELECT
    t.typname AS enum_name,
    e.enumlabel AS enum_value
FROM
    pg_type t
JOIN
    pg_enum e
    ON
        t.oid = e.enumtypid
JOIN
    pg_catalog.pg_namespace n
    ON
        n.nspname = 'public'
        AND n.oid = t.typnamespace
WHERE
    t.typname IN ('brazillian_regions', 'brazillian_states')
GROUP BY
    enum_name,
    enum_value
ORDER BY
    enum_name,
    enum_value"""