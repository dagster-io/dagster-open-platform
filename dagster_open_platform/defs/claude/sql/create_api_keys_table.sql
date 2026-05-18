CREATE TABLE IF NOT EXISTS raw.anthropic_api_keys (
  id VARCHAR(100) NOT NULL,
  name VARCHAR(500),
  workspace_id VARCHAR(100),
  created_at TIMESTAMP_NTZ,
  created_by_id VARCHAR(100),
  created_by_type VARCHAR(50),
  status VARCHAR(50),
  expires_at TIMESTAMP_NTZ,
  partial_key_hint VARCHAR(50),
  extracted_at TIMESTAMP_NTZ
)
