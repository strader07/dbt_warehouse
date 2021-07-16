{{
    config(
        materialized='incremental',
		unique_key='id'
    )
}}

with content_titles as ( select * from {{ source('content_monitor', 'content_titles') }} )

SELECT titles.user_id,
titles.upload_id,
titles.date,
titles.content_title_cleaned,
titles.content_title,
titles.service,
('stream'::text || '-'::text) || md5(titles.content_title_cleaned) AS content_id,
md5(user_id || upload_id || date || content_title_cleaned || content_title || service) id
FROM (
    SELECT netflix_viewing_history_v.user_id::character varying AS user_id,
        netflix_viewing_history_v.upload_id::text,
        netflix_viewing_history_v.date,
        regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(netflix_viewing_history_v.title, ': Limited Series(.*)'::text, ''::text, 'g'::text), ': Season (.*)'::text, ''::text, 'g'::text), ': Part (.*)'::text, ''::text, 'g'::text), ': Book (.*)'::text, ''::text, 'g'::text), ': Volume (.*)'::text, ''::text, 'g'::text), ': Collection (.*)'::text, ''::text, 'g'::text), ': Temporada (.*)'::text, ''::text, 'g'::text), ': Säsong (.*)'::text, ''::text, 'g'::text), ': Mini-série (.*)'::text, ''::text, 'g'::text), ': Chapter (.*)'::text, ''::text, 'g'::text), ': Collection: (.*)'::text, ''::text, 'g'::text), ': Series (.*)'::text, ''::text, 'g'::text), ': Episode (.*)'::text, ''::text, 'g'::text), ': Chapters (.*)'::text, ''::text, 'g'::text), ': Vol. (.*)'::text, ''::text, 'g'::text), ': The Series: (.*)'::text, ''::text, 'g'::text) AS content_title_cleaned,
        netflix_viewing_history_v.title::character varying AS content_title,
        netflix_viewing_history_v.service::character varying AS service
        FROM netflix.viewing_history_v netflix_viewing_history_v
    UNION ALL
        SELECT amazon_viewing_history_v.user_id,
        amazon_viewing_history_v.upload_id::text AS id,
        amazon_viewing_history_v.date,
        regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(amazon_viewing_history_v.title, ': Limited Series(.*)'::text, ''::text, 'g'::text), ': Season (.*)'::text, ''::text, 'g'::text), ': Part (.*)'::text, ''::text, 'g'::text), ': Book (.*)'::text, ''::text, 'g'::text), ': Volume (.*)'::text, ''::text, 'g'::text), ': Collection (.*)'::text, ''::text, 'g'::text), ': Temporada (.*)'::text, ''::text, 'g'::text), ': Säsong (.*)'::text, ''::text, 'g'::text), ': Mini-série (.*)'::text, ''::text, 'g'::text), ': Chapter (.*)'::text, ''::text, 'g'::text), ': Collection: (.*)'::text, ''::text, 'g'::text), ': Series (.*)'::text, ''::text, 'g'::text), ': Episode (.*)'::text, ''::text, 'g'::text), ': Chapters (.*)'::text, ''::text, 'g'::text), ': Vol. (.*)'::text, ''::text, 'g'::text), ': The Series: (.*)'::text, ''::text, 'g'::text) AS content_title_cleaned,
        amazon_viewing_history_v.title::character varying AS content_title,
        amazon_viewing_history_v.service::character varying AS service
        FROM amazon.viewing_history_v amazon_viewing_history_v
) titles

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where id NOT IN (select id from content_monitor.content_titles)

{% endif %}