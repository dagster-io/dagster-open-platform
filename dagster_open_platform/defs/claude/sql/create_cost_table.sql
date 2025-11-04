CREATE TABLE IF NOT EXISTS raw.anthropic_costs (
  currency VARCHAR(10),
  amount FLOAT,
  workspace_id VARCHAR(100),
  description VARCHAR(500),
  cost_type VARCHAR(100),
  context_window VARCHAR(50),
  model VARCHAR(100),
  service_tier VARCHAR(50),
  token_type VARCHAR(100),
  starting_at TIMESTAMP_NTZ NOT NULL,
  ending_at TIMESTAMP_NTZ NOT NULL,
  extracted_at TIMESTAMP_NTZ
)