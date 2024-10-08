create or replace view BLOB_AND_TABLE_MAPPING_VW (
	REFERENCED_DATABASE,
	REFERENCED_SCHEMA,
	REFERENCED_OBJECT_NAME,
	REFERENCED_OBJECT_DOMAIN,
	REFERENCING_DATABASE,
	REFERENCING_SCHEMA,
	REFERENCING_OBJECT_NAME,
	REFERENCING_OBJECT_DOMAIN,
	STAGE_URL,
	DEPENDENCY_TYPE
) as (
    WITH MAPPING_STAGES AS (
        select
            CASE
                WHEN REFERENCED_DATABASE IS NULL THEN STAGE_CATALOG
                ELSE REFERENCED_DATABASE
            END AS REFERENCED_DATABASE,
            CASE
                WHEN REFERENCED_SCHEMA IS NULL THEN STAGE_SCHEMA
                ELSE REFERENCED_SCHEMA
            END AS REFERENCED_SCHEMA,
            CASE
                WHEN b.STAGE_NAME = a.REFERENCING_OBJECT_NAME then b.STAGE_NAME
                ELSE REFERENCED_OBJECT_NAME
            END AS REFERENCED_OBJECT_NAME,
            --  REFERENCED_OBJECT_ID,
            CASE
                WHEN REFERENCED_OBJECT_DOMAIN = 'INTEGRATION' THEN 'STAGE'
                ELSE REFERENCED_OBJECT_DOMAIN
            END AS REFERENCED_OBJECT_DOMAIN,
            REFERENCING_DATABASE,
            REFERENCING_SCHEMA,
            REFERENCING_OBJECT_NAME,
            --  REFERENCING_OBJECT_ID,
            CASE
                WHEN REFERENCING_OBJECT_DOMAIN = 'STAGE' THEN 'TABLE'
                ELSE REFERENCING_OBJECT_DOMAIN
            END AS REFERENCING_OBJECT_DOMAIN,
            CASE
                WHEN b.STAGE_NAME = a.REFERENCING_OBJECT_NAME then b.STAGE_URL
                ELSE NULL
            END AS STAGE_URL,
            DEPENDENCY_TYPE
        FROM
            "SNOWFLAKE"."ACCOUNT_USAGE"."OBJECT_DEPENDENCIES" as a
            LEFT JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."STAGES" as b
        WHERE
            TRIM(REFERENCING_DATABASE) IN ('PRD_RAW', 'PRD_INT', 'PRD_PDT')
            AND TRIM(STAGE_CATALOG) IN ('PRD_RAW', 'PRD_INT', 'PRD_PDT')
        group by
            all
    )
    SELECT
        *
    FROM
        MAPPING_STAGES
    WHERE
        REFERENCED_OBJECT_NAME not like '%_SI_STO%' --and REFERENCED_OBJECT_NAME is not NULL
)
UNION
    (
        select
            distinct SPLIT_PART(a.SOURCE_OBJECT_NAME, '.', 1) AS REFERENCED_DATABASE,
            SPLIT_PART(a.SOURCE_OBJECT_NAME, '.', 2) AS REFERENCED_SCHEMA,
            SPLIT_PART(a.SOURCE_OBJECT_NAME, '.', 3) AS REFERENCED_OBJECT_NAME,
            'TABLE' AS REFERENCED_OBJECT_DOMAIN,
            SPLIT_PART(b.MODIFIED_OBJECT_NAME, '.', 1) AS REFERENCING_DATABASE,
            SPLIT_PART(b.MODIFIED_OBJECT_NAME, '.', 2) AS REFERENCING_SCHEMA,
            SPLIT_PART(b.MODIFIED_OBJECT_NAME, '.', 3) AS REFERENCING_OBJECT_NAME,
            'TABLE' AS REFERENCING_OBJECT_DOMAIN,
            'NULL' as STAGE_URL,
            'BY_NAME' as DEPENDENCY_TYPE
        from
            "PRD_RAW"."PURVIEW"."ACCESS_HISTORY_FLATTEN"  as a
            LEFT JOIN "PRD_RAW"."PURVIEW"."ACCESS_HISTORY_FLATTEN"  as b ON a.SOURCE_OBJECT_NAME = b.SOURCE_OBJECT_NAME
            AND a.SOURCE_OBJECT_NAME <> b.MODIFIED_OBJECT_NAME
        where
            true
            and a.SOURCE_OBJECT_NAME like 'PRD_%'
            AND a.SOURCE_OBJECT_NAME not like '%"%'
            AND a.SOURCE_OBJECT_NAME not like '%_EVENTS_%'
            AND a.SOURCE_OBJECT_NAME not like '%_AVSESSIONS_%'
            AND a.SOURCE_OBJECT_NAME not like '%_VISITS_%'
            AND a.SOURCE_OBJECT_NAME not like '%TEMP_%'
            AND b.MODIFIED_OBJECT_NAME not like '%"%'
            AND b.MODIFIED_OBJECT_NAME not like '%_EVENTS_%'
            AND b.MODIFIED_OBJECT_NAME not like '%_AVSESSIONS_%'
            AND b.MODIFIED_OBJECT_NAME not like '%_VISITS_%'
            AND b.MODIFIED_OBJECT_NAME not like '%TEMP_%'
            and b.SOURCE_OBJECT_NAME is not null
            and a.SOURCE_OBJECT_NAME like 'PRD%'
        group by
            ALL
        order by
            REFERENCED_DATABASE,
            REFERENCED_SCHEMA,
            REFERENCED_OBJECT_NAME
    );