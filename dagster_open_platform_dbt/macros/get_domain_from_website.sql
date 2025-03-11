{% macro get_domain_from_website(website) -%}
    --    Retrieves the domain.com portion of a website
    --       │Optionally capture http(s)://  and discard
    --       │
    --       │             Optionally capture www. and discard
    --       │             │
    --       │             │      Capture the domain.tld portion
    --       │             │      │Matching as many letters before a dot
    --       │             │      │And 2 or more letters after the dot (the tld)
    --       ▼             ▼      ▼
    --    (http[s]:\/\/)?(w*\.)?(\w*\.\w{2,})
    regexp_substr({{website}}, $$(http[s]:\/\/)?(w*\.)?(\w*\.\w{2,})$$, 1, 1, 'e', 3)
{%- endmacro %}