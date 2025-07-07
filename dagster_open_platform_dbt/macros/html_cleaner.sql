{#
    Purpose: Cleans HTML content by removing HTML tags and entities
    
    Args:
        html_col: The column containing HTML content to clean
        
    Returns:
        Clean text with HTML tags removed and HTML entities decoded
        
    Cleaning Process:
        1. Remove HTML tags (e.g., <p>, <div>, <br>, etc.)
        2. Decode common HTML entities (&amp;, &lt;, &gt;, &quot;, &#x27;)
        3. Clean up extra whitespace and newlines
        4. Handle null/empty values
#}
{% macro html_cleaner(html_col) %}
    case
        when {{ html_col }} is null or {{ html_col }} = '' then null
        else trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace({{ html_col }}, '<[^>]*>', ' '),
                                    '&amp;', '&'
                                ),
                                '&lt;', '<'
                            ),
                            '&gt;', '>'
                        ),
                        '&quot;', '"'
                    ),
                    '&#x27;', ''''
                ),
                '\\s+', ' '
            )
        )
    end
{% endmacro %} 