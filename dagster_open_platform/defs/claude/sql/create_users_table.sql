CREATE TABLE IF NOT EXISTS raw.anthropic_users (
  id VARCHAR(100) NOT NULL,
  email VARCHAR(500),
  name VARCHAR(500),
  role VARCHAR(50),
  added_at TIMESTAMP_NTZ,
  extracted_at TIMESTAMP_NTZ
)
