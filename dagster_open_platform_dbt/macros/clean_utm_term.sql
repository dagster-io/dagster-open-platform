{#
  Macro: clean_utm_term
  
  Purpose: 
    Standardizes and cleans UTM term parameters extracted from URL query strings.
    Handles common issues like URL encoding, extra whitespace, and invalid values.
    
  Parameters:
    - utm_term_column: The column or expression containing the raw utm_term value
    
  Returns:
    A cleaned, standardized utm_term string suitable for marketing attribution analysis
    
  Cleaning Process:
    1. Null/Empty Value Handling - Filters out invalid or meaningless values
    2. URL Decoding - Converts percent-encoded characters back to readable format
    3. Whitespace Normalization - Removes excess spaces and special whitespace characters
    4. Case Standardization - Converts to lowercase for consistent analysis
    
  Examples of transformations:
    'data%20engineering' -> 'data engineering'
    'machine+learning' -> 'machine learning'  
    '  Analytics  ' -> 'analytics'
    'undefined' -> NULL
    '' -> NULL
#}

{% macro clean_utm_term(utm_term_column) %}
    
    CASE 
        {# Step 1: Handle NULL values #}
        WHEN {{ utm_term_column }} IS NULL THEN NULL
        
        {# Step 2: Handle empty strings (whitespace-only strings become empty after TRIM) #}
        WHEN TRIM({{ utm_term_column }}) = '' THEN NULL
        
        {# Step 3: Handle common invalid placeholder values that analytics tools sometimes insert #}
        WHEN LOWER(TRIM({{ utm_term_column }})) = 'undefined' THEN NULL
        WHEN LOWER(TRIM({{ utm_term_column }})) = 'null' THEN NULL
        
        ELSE 
            {# Step 4: Apply comprehensive cleaning pipeline #}
            TRIM(
                LOWER(
                    {# Step 4e: Final cleanup - Remove any remaining leading/trailing whitespace #}
                    {# Pattern '^\\s+|\\s+$' matches whitespace at start (^\\s+) OR end (\\s+$) #}
                    REGEXP_REPLACE(
                        {# Step 4d: Replace special whitespace characters with regular spaces #}
                        {# Pattern '[\\r\\n\\t]' matches carriage returns, newlines, and tabs #}
                        REGEXP_REPLACE(
                            {# Step 4c: Normalize multiple consecutive spaces to single space #}
                            {# Pattern '\\s{2,}' matches 2 or more whitespace characters in a row #}
                            REGEXP_REPLACE(
                                {# Step 4b: URL decode the utm_term parameter #}
                                COALESCE(
                                    {# Primary: Use existing URI decode function (handles all percent encoding) #}
                                    {{ target.database }}.utils.uri_percent_decode({{ utm_term_column }}::varchar),
                                    
                                    {# Fallback: Manual decoding for most common cases if function fails #}
                                    REGEXP_REPLACE(
                                        {# Replace %20 (percent-encoded space) with actual space #}
                                        REGEXP_REPLACE({{ utm_term_column }}, '%20', ' ', 1, 0),
                                        {# Replace + (plus-encoded space) with actual space #}
                                        {# Pattern '\\+' matches literal plus sign (escaped because + is regex special char) #}
                                        '\\+', ' ', 1, 0
                                    )
                                ),
                                {# Normalize whitespace: replace 2+ consecutive spaces with single space #}
                                '\\s{2,}', ' ', 1, 0
                            ),
                            {# Replace special whitespace chars (tabs, newlines, carriage returns) with spaces #}
                            '[\\r\\n\\t]', ' ', 1, 0
                        ),
                        {# Remove leading and trailing whitespace #}
                        '^\\s+|\\s+$', '', 1, 0
                    )
                )
            )
    END
    
    {#
    REGEXP_REPLACE Parameter Explanation (Snowflake syntax):
    - Parameter 1: subject (the string to search in)
    - Parameter 2: pattern (the regex pattern to find)  
    - Parameter 3: replacement (what to replace matches with)
    - Parameter 4: position (starting position, 1 = from beginning)
    - Parameter 5: occurrence (0 = replace all occurrences, equivalent to global 'g' flag)
    #}

{% endmacro %} 