-- ============================================================
-- STAR SCHEMA: HEALTHCARE ANALYTICS
-- Grain: One row per encounter
-- ============================================================


-- ============================================================
-- DIMENSION: DATE
-- ============================================================
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,               
    full_date DATE NOT NULL UNIQUE,
    day INT NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    year INT NOT NULL CHECK (year >= 1900),
    quarter INT CHECK (quarter BETWEEN 1 AND 4),
    week_of_year INT CHECK (week_of_year BETWEEN 1 AND 53),
    day_of_week INT CHECK (day_of_week BETWEEN 0 AND 6),
    is_weekend BOOLEAN,
    month_name VARCHAR(20),
    quarter_name VARCHAR(10)
);


-- ============================================================
-- DIMENSION: PATIENT
-- ============================================================
CREATE TABLE dim_patient (
    patient_key SERIAL PRIMARY KEY,
    patient_id INT NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender CHAR(1) CHECK (gender IN ('M','F','O')),
    age_group VARCHAR(20)
);


-- ============================================================
-- DIMENSION: SPECIALTY
-- ============================================================
CREATE TABLE dim_specialty (
    specialty_key SERIAL PRIMARY KEY,
    specialty_id INT NOT NULL UNIQUE,
    specialty_name VARCHAR(100) NOT NULL UNIQUE
);


-- ============================================================
-- DIMENSION: DEPARTMENT
-- ============================================================
CREATE TABLE dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id INT NOT NULL UNIQUE,
    department_name VARCHAR(100) NOT NULL
);


-- ============================================================
-- DIMENSION: PROVIDER (SCD TYPE 2 SUPPORT)
-- ============================================================
CREATE TABLE dim_provider (
    provider_key SERIAL PRIMARY KEY,
    provider_id INT NOT NULL,
    full_name VARCHAR(150) NOT NULL,
    specialty_key INT NOT NULL,
    department_key INT NOT NULL,
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);


-- ============================================================
-- DIMENSION: ENCOUNTER TYPE
-- ============================================================
CREATE TABLE dim_encounter_type (
    encounter_type_key SERIAL PRIMARY KEY,
    encounter_type_name VARCHAR(50) NOT NULL UNIQUE
);


-- ============================================================
-- DIMENSION: DIAGNOSIS
-- ============================================================
CREATE TABLE dim_diagnosis (
    diagnosis_key SERIAL PRIMARY KEY,
    diagnosis_id INT NOT NULL UNIQUE,
    icd10_code VARCHAR(10) NOT NULL,
    description VARCHAR(200)
);


-- ============================================================
-- DIMENSION: PROCEDURE
-- ============================================================
CREATE TABLE dim_procedure (
    procedure_key SERIAL PRIMARY KEY,
    procedure_id INT NOT NULL UNIQUE,
    cpt_code VARCHAR(10) NOT NULL,
    description VARCHAR(200)
);


-- ============================================================
-- FACT TABLE: ENCOUNTER
-- ============================================================
CREATE TABLE fact_encounter (
    encounter_key SERIAL PRIMARY KEY,
    encounter_id INT UNIQUE,

    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    encounter_type_key INT NOT NULL,
    date_key INT NOT NULL,

    -- Pre-aggregated measures
    total_allowed_amount NUMERIC(12,2) CHECK (total_allowed_amount >= 0),
    diagnosis_count INT CHECK (diagnosis_count >= 0),
    procedure_count INT CHECK (procedure_count >= 0),

    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);


-- ============================================================
-- BRIDGE TABLE: ENCOUNTER ↔ DIAGNOSIS
-- ============================================================
CREATE TABLE bridge_encounter_diagnosis (
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    PRIMARY KEY (encounter_key, diagnosis_key),

    FOREIGN KEY (encounter_key) REFERENCES fact_encounter(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key)
);


-- ============================================================
-- BRIDGE TABLE: ENCOUNTER ↔ PROCEDURE
-- ============================================================
CREATE TABLE bridge_encounter_procedure (
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    PRIMARY KEY (encounter_key, procedure_key),

    FOREIGN KEY (encounter_key) REFERENCES fact_encounter(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key)
);


-- ============================================================
-- INDEXING STRATEGY (QUERY-DRIVEN)
-- ============================================================

-- Fact table indexes
CREATE INDEX idx_fact_date 
ON fact_encounter(date_key);

CREATE INDEX idx_fact_provider 
ON fact_encounter(provider_key);

CREATE INDEX idx_fact_patient 
ON fact_encounter(patient_key);

-- Composite index for time + provider queries
CREATE INDEX idx_fact_date_provider 
ON fact_encounter(date_key, provider_key);

-- Alternative composite index for provider-first filtering
CREATE INDEX idx_fact_provider_date 
ON fact_encounter(provider_key, date_key);

-- Bridge table indexes
CREATE INDEX idx_bridge_diagnosis 
ON bridge_encounter_diagnosis(diagnosis_key);

CREATE INDEX idx_bridge_procedure 
ON bridge_encounter_procedure(procedure_key);


-- ============================================================
-- NOTES
-- ============================================================
-- Fact table stores pre-aggregated measures for faster analytics
-- Bridge tables prevent duplication from many-to-many relationships
-- Date dimension removes need for runtime date functions
-- Indexes are designed based on analytical query patterns