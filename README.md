🚀 Project 3: Metadata-Driven ETL Pipeline & Control Panel

(Industry-style, not student-style)

4
🧠 First: Understand the CORE IDEA (No Code Yet)
❌ Normal ETL (what most students do)

Write PySpark code

Hardcode column names

Hardcode transformations

Change code every time data changes

This does not scale.

✅ Metadata-Driven ETL (what companies prefer)

Store rules in tables

PySpark reads rules

ETL logic stays the same

Only metadata changes

Think of it like this:

Code = engine
Metadata = steering wheel

🧩 What “Metadata” Means (Simple Language)

Metadata = data about data

Example:

Which file to read?

Which columns to drop?

Which column to rename?

Which type to cast?

Instead of writing this in Python, you store it in SQL tables.

🏗️ Final Architecture (Burn This Into Memory)
Raw CSV Files
     ↓
Metadata Tables (SQL)
     ↓
PySpark ETL Engine (generic)
     ↓
Cleaned Tables / Parquet
     ↓
ETL Logs + Dashboard

If you explain this clearly in interviews → you win.

🧱 PHASE 1: Project Structure (Do This First)

Create a new repo:

metadata-driven-etl/
│
├── data/
│   └── raw/
│
├── etl/
│   ├── spark_job.py
│
├── metadata/
│   └── metadata_schema.sql
│
├── dashboard/
│   └── app.py   (later)
│
├── logs/
│
├── requirements.txt
└── README.md

This already looks industry-grade.

🧱 PHASE 2: Design Metadata Tables (MOST IMPORTANT PART)

Create metadata/metadata_schema.sql

1️⃣ Source Configuration Table
CREATE TABLE source_config (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50),
    file_path VARCHAR(200),
    delimiter VARCHAR(5)
);

📌 Controls what data to read

2️⃣ Transformation Rules Table
CREATE TABLE transformation_rules (
    rule_id SERIAL PRIMARY KEY,
    source_id INT,
    column_name VARCHAR(50),
    rule_type VARCHAR(30),
    rule_value VARCHAR(50)
);

📌 Controls how data is cleaned

Examples:

drop_nulls

cast_int

rename

uppercase

3️⃣ Target Configuration Table
CREATE TABLE target_config (
    source_id INT,
    target_table VARCHAR(50),
    load_type VARCHAR(20)
);

📌 Controls where data goes

4️⃣ ETL Run Log Table
CREATE TABLE etl_run_log (
    run_id SERIAL PRIMARY KEY,
    source_id INT,
    run_time TIMESTAMP,
    status VARCHAR(20),
    rows_processed INT,
    error_message TEXT
);

📌 THIS is what real DEs care about.

🧠 Pause & Reality Check

At this point you already learned:

Config-driven systems

Pipeline control

Logging & observability

Most freshers never touch this.

🧱 PHASE 3: Sample Metadata (Insert Data)

Example:

INSERT INTO source_config (source_name, file_path, delimiter)
VALUES ('sales', 'data/raw/sales.csv', ',');

INSERT INTO transformation_rules (source_id, column_name, rule_type, rule_value)
VALUES
(1, 'quantity', 'drop_nulls', ''),
(1, 'price', 'cast_double', ''),
(1, 'product_name', 'uppercase', '');

INSERT INTO target_config (source_id, target_table, load_type)
VALUES (1, 'clean_sales', 'full');

Now your ETL is controlled by SQL, not Python.

🧱 PHASE 4: PySpark ETL Engine (GENERIC LOGIC)

Create etl/spark_job.py

Conceptually, this file will:

Read source_config

Read CSV

Read transformation_rules

Apply rules dynamically

Write output

Log results

Pseudo-logic (important):

read metadata
read raw data

for each rule:
    if rule_type == "drop_nulls":
        apply dropna
    if rule_type == "cast_double":
        cast column
    if rule_type == "uppercase":
        transform column

write cleaned data
log success/failure

This is real ETL engine design.

We’ll code this step-by-step next — safely.

🧱 PHASE 5: Control Panel (LIGHT UI, NOT CRUD)

Simple dashboard (Streamlit or Django):

Dropdown: select source

Button: run ETL

Table: ETL run history

Status: success / failure

This is pipeline control, not an app.