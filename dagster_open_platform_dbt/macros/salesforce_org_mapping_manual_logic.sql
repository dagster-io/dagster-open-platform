{#
  Shared logic for respecting manual Salesforce account–org mapping (Organization_ID__c).
  Used by sync_salesforce_account (to avoid overwriting manual updates) and
  sync_hubspot_organization (to write the same mapping to HubSpot so it is respected).
  Keep field name, date window, and devtools list in sync here.
#}

{% macro salesforce_org_mapping_history_filter() -%}
    field = 'Organization_ID__c'
    and created_date >= '2025-01-01'
{%- endmacro %}

{% macro salesforce_org_mapping_devtools_emails() -%}
    ('devtools@elementl.com', 'devtools+hightouch@dagsterlabs.com')
{%- endmacro %}

{% macro salesforce_org_mapping_was_allowable(created_by_email_column) -%}
    ({{ created_by_email_column }} is null or {{ created_by_email_column }} in {{ salesforce_org_mapping_devtools_emails() }})
{%- endmacro %}

{% macro salesforce_org_mapping_was_manual(created_by_email_column) -%}
    ({{ created_by_email_column }} is not null and {{ created_by_email_column }} not in {{ salesforce_org_mapping_devtools_emails() }})
{%- endmacro %}
