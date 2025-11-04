CREATE TABLE IF NOT EXISTS raw.anthropic_usage (
  uncached_input_tokens NUMBER,
  cache_creation_ephemeral_1h_input_tokens NUMBER,
  cache_creation_ephemeral_5m_input_tokens NUMBER,
  cache_read_input_tokens NUMBER,
  output_tokens NUMBER,
  web_search_requests NUMBER,
  api_key_id VARCHAR(100),
  workspace_id VARCHAR(100),
  model VARCHAR(100),
  service_tier VARCHAR(50),
  context_window VARCHAR(50),
  starting_at TIMESTAMP_NTZ NOT NULL,
  ending_at TIMESTAMP_NTZ NOT NULL,
  extracted_at TIMESTAMP_NTZ
)