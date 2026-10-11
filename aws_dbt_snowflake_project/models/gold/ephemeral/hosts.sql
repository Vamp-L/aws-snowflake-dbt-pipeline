{{ config(materialized='ephemeral') }}

with hosts as (
    select 
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        IS_SUPERHOST,
        RESPONSE_RATE_TAG,
        HOST_CREATED_AT
    from {{ ref('obt') }}
)
SELECT * FROM hosts
