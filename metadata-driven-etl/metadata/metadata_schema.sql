CREATE TABLE source_config(
    source_id SERIAL PRIMARY KEY,
    source_name VARCAHR(50),
    file_path VARCAHR(500),
    delimeter VARCHAR(10)
)

CREATE TABLE transformation_rules (
    rule_id SERIAL PRIMARY KEY, 
    source_id INT,
    column_name VARCHAR(50),
    rule_type VARCHAR(50),
    rule_value VARCAHR(50)
)

CREATE TABLE target_config (
    source_id SERIAL PRIMARY KEY, 
    target_table VARCHAR(50),
    load_type VARCAHR(50)
)

CREATE TABLE etl_run_log(
    run_id SERIAL PRIMARY KEY,
    source_id INT,
    run_time TIMESTAMP,
    status VARCAHR(20),
    rows_processed INT,
    error_message TEXT
)

