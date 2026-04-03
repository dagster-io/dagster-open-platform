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
    when campaign_medium in ('cpc', 'ppc') then 'cpc'

    -- AI providers (source-based, checked before UTM medium passthrough so AI tool names
    -- as campaign_source don't get swallowed by other-campaign).
    -- Guard: only trust campaign_source as an AI signal when there is no referrer or the
    -- referrer is itself an AI domain. This prevents shared/forwarded URLs that carry an
    -- AI utm_source from mis-attributing sessions that actually arrived via social/other.
    when campaign_source in (
        'anthropic', 'anthropic.com',
        'character.ai',
        'chatgpt', 'chatgpt.com',
        'claude', 'claude.ai',
        'codeium', 'codeium.com',
        'cohere', 'cohere.ai',
        'copilot', 'copilot.com', 'copilot.microsoft.com',
        'cursor', 'cursor.sh',
        'deepseek', 'deepseek.com',
        'gemini', 'gemini.google.com',
        'grok', 'grok.com',
        'groq', 'groq.com',
        'huggingface', 'huggingface.co',
        'meta.ai',
        'mistral', 'mistral.ai',
        'moge.ai',
        'openai', 'openai.com',
        'perplexity', 'perplexity.ai',
        'poe', 'poe.com',
        'replit.com',
        'together.ai',
        'you.com'
    ) and (
        {{ referrer_host_column }} is null
        or referrer.medium = 'ai'
    ) then 'ai'

    -- UTM Params - normalize known medium values to canonical categories
    when campaign_medium = 'email' then 'email'
    when campaign_medium in ('newsletter', 'data_elixir', 'rss') then 'newsletter'
    when campaign_medium in ('social', 'zalo') then 'social'
    when campaign_medium = 'sponsorship' then 'sponsorship'
    when campaign_medium in ('jobposting', 'dynamitejobs-job-post') then 'jobposting'
    when campaign_medium = 'blog' then 'blog'
    when campaign_medium = 'events' then 'events'
    when campaign_medium = 'referral' then 'referral'
    when campaign_medium = 'ai' then 'ai'
    when campaign_medium is not null then 'other-campaign'

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
    when {{ referrer_host_column }} like '%.zoom.us' then 'zoom'
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
    when {{ referrer_host_column }} in (
        'dagster.io', 'dagster.cloud', 'elementl.com', 'dagsterlabs.com',
        'legacy-docs.dagster.io', 'dagster-io.github.io'
    ) then 'dagster-referrer'
    when {{ referrer_host_column }} like '%.dagster.cloud' then 'dagster-referrer'
    when {{ referrer_host_column }} like '%.dagster.plus' then 'dagster-referrer'
    when {{ referrer_host_column }} in (
        'compass.dagster.io', 'dagstercompass.com', 'docs.compass.dagster.io'
    ) then 'compass-referrer'
    when {{ referrer_host_column }} is not null then 'other-referrer'
    else 'uncategorized'
end

{%- endmacro -%} 