select

    id as event_id,
    anonymous_id,
    user_id,

    parse_url(referrer, 1):host::string as referrer_host_unparsed,
    regexp_substr(parse_url(referrer, 1):host::string, '(www\.)?(.+)', 1, 1, 'e', 2)
        as referrer_host,
    referrer,
    path,
    title,
    search,

    parse_url(context_page_url, 1):parameters as search_params,

    context_campaign_source as campaign_source,
    context_campaign_medium as campaign_medium,
    context_campaign_name as campaign_name,
    context_campaign_content as campaign_content,
    search_params:utm_term as utm_term,

    search_params:rdt_cid as reddit_cid,
    search_params:gclid as gclid,

    timestamp

from {{ source('segment_web', 'pages') }}
