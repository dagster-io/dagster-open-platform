{% macro sessionize_page_visits(source_ref, include_campaign_logic=false) %}

with pages as (
    select
        *,
        row_number() over (partition by anonymous_id order by timestamp) as page_view_number,
        lag(timestamp) over (partition by anonymous_id order by timestamp) as previous_tstamp
    from {{ source_ref }}
),

referrer as (
    select * from {{ ref('referrer_mapping') }}
),

page_sessions as (
    select
        pages.*,
        datediff('second', previous_tstamp, timestamp) as period_of_inactivity,
        iff(period_of_inactivity <= {{ var('session_threshold_seconds') }}, 0, 1) as new_session,
        referrer.medium as referrer_medium,

        {{ get_page_attribution_category() }} as page_attribution_category,
        sum(new_session)
            over (
                partition by anonymous_id
                order by page_view_number rows between unbounded preceding and current row
            ) as session_number
    from pages
    left join referrer on pages.referrer_host = referrer.host
),

add_session_id as (
    select
        *,
        md5(anonymous_id || session_number) as session_id
    from page_sessions
),

add_paid_flags as (
    select 
        *,
        case
            when page_attribution_category in ('cpc', 'ppc', 'sponsorship') then true
            else false
        end as is_paid_category
    from add_session_id
),

page_sessions_numbered as (
    select
        *,
        row_number() over (partition by session_id order by timestamp) as page_number_in_session,
        count(*) over (partition by session_id) as total_pages_in_session
    from add_paid_flags
),

session_attribution as (
    select
        session_id,
        page_attribution_category as session_attribution_category
    from page_sessions_numbered
    where page_number_in_session = 1
),

session_utm_data as (
    select
        session_id,
        campaign_source as session_campaign_source,
        campaign_medium as session_campaign_medium,
        campaign_name as session_campaign_name,
        campaign_content as session_campaign_content,
        utm_term as session_utm_term,
        referrer as session_referrer
    from page_sessions_numbered
    where page_number_in_session = 1
),

{% if include_campaign_logic %}
add_campaign_logic as (
    select 
        ps.* exclude (previous_tstamp, period_of_inactivity, new_session),
        sa.session_attribution_category,
        su.session_campaign_source,
        su.session_campaign_medium,
        su.session_campaign_name,
        su.session_campaign_content,
        su.session_utm_term,
        su.session_referrer,
        case 
            -- Check if utm_campaign is numeric-only and use it if so
            when utm_campaign is not null and try_to_number(utm_campaign) is not null then utm_campaign
            -- Check if utm_campaign_second is numeric-only and use it if so
            when utm_campaign_second is not null and try_to_number(utm_campaign_second) is not null then utm_campaign_second
            -- Existing specific campaign mappings
            when campaign_name = '9124225-25-03-ABM_Airflow' and campaign_content = 'airlift_guide' then '2202694925580706257'
            when campaign_name = '9124225-25-03-ABM_Airflow' then '2187501160110163210'
            when campaign_name = '5531654-2025 Q1 | AI Campaign' and reddit_cid is not null then '2175886033960630843'
            when campaign_name = '8626009-25-02-eBook_Data-Platform-Fundamentals' and reddit_cid is not null then '2251306549350750173'
            when campaign_name = '5531654-2025%20Q1%20%7C%20AI%20Campaign' and reddit_cid is not null then '2175886033960630843'
            when campaign_source = 'email' and campaign_medium = 'sponsorship' then concat('email_sponsorship_name_', campaign_name, '_content_', campaign_content)
            else coalesce(campaign_name, utm_campaign, utm_content)
        end as campaign_identifier,
        case 
            when is_paid_category = true and lower(campaign_source) like 'bing' then 'bing'
            when is_paid_category = true and lower(campaign_source) like 'adwords' then 'adwords'
            when is_paid_category = true and lower(campaign_source) like 'reddit' then 'reddit'
            when is_paid_category = true and lower(campaign_source) like 'google' then 'adwords'
            when is_paid_category = true and lower(campaign_source) like 'linkedin' then 'linkedin'
            when is_paid_category = true and lower(campaign_source) like 'dzone%' then 'dzone'
            when is_paid_category = true and lower(campaign_source) like 'email' then campaign_content
            when is_paid_category = true and gclid is not null then 'adwords'
            when is_paid_category = true and fbclid is not null then 'facebook'
            when is_paid_category = true and msclkid is not null then 'microsoft'
            when is_paid_category = true and twclid is not null then 'twitter'
            when is_paid_category = true and ttclid is not null then 'tiktok'
            when is_paid_category = true and wbraid is not null then 'google'
            when is_paid_category = true and gbraid is not null then 'google'
            when is_paid_category = true and dclid is not null then 'google'
            when is_paid_category = true and linkedin_campaign_id is not null then 'linkedin'
            when is_paid_category = true and reddit_cid is not null then 'reddit'
        end as paid_campaign_platform
    from page_sessions_numbered ps
    left join session_attribution sa on ps.session_id = sa.session_id
    left join session_utm_data su on ps.session_id = su.session_id
)

select * from add_campaign_logic

{% else %}
final as (
    select 
        ps.* exclude (previous_tstamp, period_of_inactivity, new_session),
        sa.session_attribution_category,
        su.session_campaign_source,
        su.session_campaign_medium,
        su.session_campaign_name,
        su.session_campaign_content,
        su.session_utm_term,
        su.session_referrer
    from page_sessions_numbered ps
    left join session_attribution sa on ps.session_id = sa.session_id
    left join session_utm_data su on ps.session_id = su.session_id
)

select * from final
{% endif %}

{% endmacro %}

