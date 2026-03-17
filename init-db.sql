CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    slack_webhook_url VARCHAR(255),
    alert_config JSONB DEFAULT '{"alertOnErrors": true, "alertOnTypes": []}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_value VARCHAR(255) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events_aggregation (
    time TIMESTAMPTZ NOT NULL,
    project_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count BIGINT NOT NULL,
    UNIQUE (time, project_id, event_type)
);

CREATE TABLE IF NOT EXISTS raw_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    time TIMESTAMPTZ NOT NULL,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL
);

-- Convert to hypertables
SELECT create_hypertable('events_aggregation', 'time', if_not_exists => TRUE);
SELECT create_hypertable('raw_events', 'time', if_not_exists => TRUE);

-- Retention policies
SELECT add_retention_policy('events_aggregation', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('raw_events', INTERVAL '7 days', if_not_exists => TRUE);
