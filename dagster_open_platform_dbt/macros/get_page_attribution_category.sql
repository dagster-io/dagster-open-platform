{%- macro get_page_attribution_category(
    referrer_host_column='referrer_host'
) -%}

case
    -- Ads (definite paid traffic indicators)
    when gclid is not null then 'cpc'
    when reddit_cid is not null then 'cpc'
    when msclkid is not null then 'cpc'
    when ttclid is not null then 'cpc'
    when linkedin_campaign_id is not null then 'cpc'
    when wbraid is not null then 'cpc'
    when gbraid is not null then 'cpc'
    when dclid is not null then 'cpc'
    when campaign_medium = 'cpc' then 'cpc'

    -- UTM Params
    when campaign_medium is not null then campaign_medium

    -- Social Media Platform Indicators (not necessarily paid)
    when fbclid is not null then 'social'
    when twclid is not null then 'social'

    -- Search Breakdown
    when referrer.medium = 'search' and path = '/pricing' then 'search-pricing'
    when referrer.medium = 'search' and path like '/careers%' then 'search-careers'
    when referrer.medium = 'search' and path = '/' then 'search-brand'
    when referrer.medium = 'search' and path = '/cloud' then 'search-brand'
    when referrer.medium = 'search' and path like '/blog%' then 'search-blog'
    when referrer.medium = 'search' and path like '/guides%' then 'search-guides'
    when referrer.medium = 'search' and path like '/glossary%' then 'search-glossary'
    when referrer.medium = 'search' and path like '/vs%' then 'search-vs'
    when referrer.medium = 'search' and path like '/integrations%' then 'search-integrations'
    when referrer.medium = 'search' then 'search-other'

    -- Referrers
    when referrer.medium is not null then referrer.medium

    -- Path Attribution
    when {{ referrer_host_column }} = 'docs.dagster.io' then 'docs'
    when {{ referrer_host_column }} in ('courses.dagster.io', 'dagster-university.vercel.app') then 'daggy-u'
    when path like '/blog%' and {{ referrer_host_column }} is null then 'blog'
    when path = '/glossary' and {{ referrer_host_column }} is null then 'glossary'
    when path like '/events%' and referrer_medium is null then 'events'

    -- Other, uncategorized
    when
        coalesce(
            campaign_source,
            campaign_medium,
            campaign_name,
            utm_term
        ) is not null
        then 'other-campaign'
    when {{ referrer_host_column }} in ('dagster.io', 'dagster.cloud', 'elementl.com', 'dagsterlabs.com') then 'dagster-referrer'
    when {{ referrer_host_column }} like '%.dagster.cloud' then 'dagster-referrer'
    when {{ referrer_host_column }} is not null then 'other-referrer'
    else 'uncategorized'
end

{%- endmacro -%} 