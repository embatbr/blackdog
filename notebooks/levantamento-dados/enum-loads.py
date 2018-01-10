%pyspark


SQL_SKELETON = """(SELECT
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
    t.typname = '{}'
ORDER BY
    enum_value) result"""
